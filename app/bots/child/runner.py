import asyncio
import signal
import contextlib
import os
import time
import json
from typing import Dict, Optional, Tuple, Any

from aiogram import Bot
from aiogram.exceptions import TelegramUnauthorizedError

from app.db import SessionLocal, engine
from app.models import Tenant, TenantStatus
from app.bots.child.bot_instance import run_child_bot
from app.settings import settings


# ========================= Настройки и константы =========================

# Опрашиваем базу чаще (по умолчанию было 10) — делаем быстрее реакции.
CHECK_INTERVAL_SEC = 2

# Файл-бампер: любое обновление mtime → мягкий рестарт всех активных детей.
BUMPER_PATH = "/tmp/pb_runner.bump"
_last_bump_mtime: Optional[float] = None

# Папка со статусами детей (для GA-меню, чтобы показать «последний статус»)
STATUS_DIR = "/tmp/children_status"


# Родительский бот – используется для проверки членства владельца
_parent_bot: Optional[Bot] = None

# Печать диагностики БД один раз
_DB_DEBUG_DONE = False


# ========================= Утилиты статусов (для GA) =========================
def _write_child_status(tenant_id: int, phase: str, detail: Optional[str] = None):
    """
    Пишем статус ребёнка в файл: /tmp/children_status/{tenant_id}.json
    Пример:
      {"tenant_id": 12, "phase": "running", "detail": "", "ts": 1710000000.0}
    """
    try:
        os.makedirs(STATUS_DIR, exist_ok=True)
        path = os.path.join(STATUS_DIR, f"{tenant_id}.json")
        payload = {
            "tenant_id": tenant_id,
            "phase": phase,
            "detail": detail or "",
            "ts": time.time(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


# ========================= Хелперы =========================
def _need_owner_membership_check() -> bool:
    """Проверяем членство владельца только если явно задан PRIVATE_CHANNEL_ID в .env."""
    try:
        return bool(getattr(settings, "private_channel_id", None))
    except Exception:
        return False


async def _ensure_parent_bot() -> Bot:
    global _parent_bot
    if _parent_bot is None:
        _parent_bot = Bot(token=settings.parent_bot_token)
    return _parent_bot


async def _owner_is_member(owner_tg_id: int) -> bool:
    """
    Проверяем, что владелец состоит в приватном канале.
    Если канал не задан — считаем OK.
    """
    if not _need_owner_membership_check():
        return True
    try:
        bot = await _ensure_parent_bot()
        chat_id = settings.private_channel_id
        m = await bot.get_chat_member(chat_id, owner_tg_id)
        ok = m.status not in ("left", "kicked")
        if not ok:
            print(f"[runner][membership] NOT MEMBER: owner={owner_tg_id} in chat={chat_id}, status={m.status}")
        return ok
    except Exception as e:
        print(f"[runner][membership] ERROR chat={getattr(settings,'private_channel_id',None)} owner={owner_tg_id}: {e!r}")
        return False


async def _preflight_token(tenant: Tenant) -> bool:
    """
    Быстрая проверка токена перед запуском polling:
    - getMe (ловим Unauthorized)
    - delete_webhook(drop_pending_updates=True)
    """
    token = (tenant.child_bot_token or "").strip()
    if not token:
        print(f"[runner] preflight FAIL empty token: tenant_id={tenant.id}")
        _write_child_status(tenant.id, "preflight_fail", "empty_token")
        return False

    bot = Bot(token=token)
    try:
        me = await bot.get_me()
        print(f"[runner] preflight OK: tenant_id={tenant.id} bot_id={me.id} @{me.username}")
        _write_child_status(tenant.id, "preflight_ok", f"@{me.username or ''}")
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        return True
    except TelegramUnauthorizedError:
        print(f"[runner] preflight FAIL Unauthorized: tenant_id={tenant.id}")
        _write_child_status(tenant.id, "unauthorized", "getMe Unauthorized")
        return False
    except Exception as e:
        # Не валим на прочих ошибках — дадим шансы run_child_bot
        print(f"[runner] preflight FAIL other: tenant_id={tenant.id} err={e!r}")
        _write_child_status(tenant.id, "preflight_other", repr(e))
        return True
    finally:
        with contextlib.suppress(Exception):
            await bot.session.close()


def _pause_tenant(tenant_id: int, reason: str = "unauthorized"):
    """Помечаем тенанта paused в БД (без лишних зависимостей)."""
    db = SessionLocal()
    try:
        # SQLAlchemy 2.x: используем session.get
        t = db.get(Tenant, tenant_id)
        if t and t.status != TenantStatus.paused:
            t.status = TenantStatus.paused
            db.commit()
            print(f"[runner] tenant paused: tenant_id={tenant_id} reason={reason}")
    except Exception as e:
        print(f"[runner] tenant pause failed: tenant_id={tenant_id} err={e!r}")
    finally:
        db.close()


def _signature_for(tenant: Tenant) -> Tuple[Any, ...]:
    """
    Сигнатура, по которой понимаем, что ребёнка надо перезапустить.
    Используем только критичные поля:
      - child_bot_token
      - updated_at (если есть в модели; если нет — будет None)
    """
    token = (tenant.child_bot_token or "").strip()
    upd = None
    if hasattr(tenant, "updated_at") and getattr(tenant, "updated_at") is not None:
        try:
            upd = int(getattr(tenant, "updated_at").timestamp())
        except Exception:
            upd = str(getattr(tenant, "updated_at"))
    return (token, upd)


async def _child_entry(tenant_id: int):
    """
    Обёртка вокруг run_child_bot — получаем свежего Tenant внутри, перезапускаем при падениях,
    на Unauthorized — автопауза и выходим.
    """
    print(f"[runner] child starting: tenant_id={tenant_id}")

    # Берём свежего тенанта для префлайта
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    finally:
        db.close()
    if not t:
        print(f"[runner] child start skipped: tenant_id={tenant_id} not found")
        _write_child_status(tenant_id, "not_found")
        return

    ok = await _preflight_token(t)
    if not ok:
        _pause_tenant(tenant_id, reason="unauthorized_preflight")
        return

    while True:
        try:
            _write_child_status(tenant_id, "starting")
            # Внутри run_child_bot он сам ещё раз get_me + delete_webhook, потом polling
            await run_child_bot(t)
            _write_child_status(tenant_id, "exited", "run_child_bot returned")
            print(f"[runner] run_child_bot RETURNED: tenant_id={tenant_id}; restart in 2s")
        except asyncio.CancelledError:
            _write_child_status(tenant_id, "cancelled")
            print(f"[runner] child cancelled: tenant_id={tenant_id}")
            raise
        except TelegramUnauthorizedError:
            _write_child_status(tenant_id, "unauthorized", "during run")
            print(f"[runner] child Unauthorized during run: tenant_id={tenant_id}")
            _pause_tenant(tenant_id, reason="unauthorized_during_run")
            return
        except Exception as e:
            _write_child_status(tenant_id, "crashed", repr(e))
            print(f"[runner] child crashed: tenant_id={tenant_id} exc={e!r}; restart in 2s")
        await asyncio.sleep(2)


async def manager_loop():
    global _DB_DEBUG_DONE, _last_bump_mtime

    # tasks[tenant_id] = (asyncio.Task, signature)
    tasks: Dict[int, Tuple[asyncio.Task, Tuple[Any, ...]]] = {}

    async def stop_task(tid: int):
        rec = tasks.pop(tid, None)
        if not rec:
            return
        task = rec[0]
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        print(f"[runner] stopped child tenant_id={tid}")
        _write_child_status(tid, "stopped_by_manager")

    async def ensure_db_debug_once():
        global _DB_DEBUG_DONE
        if _DB_DEBUG_DONE:
            return
        try:
            print("[runner][DB] engine.url:", engine.url)
            with engine.connect() as conn:
                try:
                    dblist = conn.exec_driver_sql("PRAGMA database_list;").all()
                    print("[runner][DB] PRAGMA database_list:", dblist)
                except Exception:
                    pass
                try:
                    cols = conn.exec_driver_sql("PRAGMA table_info(tenants);").all()
                    print("[runner][DB] tenants columns:", [c[1] for c in cols])
                except Exception:
                    pass
        except Exception as e:
            print("[runner][DB] introspection error:", repr(e))
        finally:
            _DB_DEBUG_DONE = True

    # Инициализируем текущее mtime бампера
    try:
        _last_bump_mtime = os.path.getmtime(BUMPER_PATH)
    except Exception:
        _last_bump_mtime = 0.0

    while True:
        await ensure_db_debug_once()

        db = SessionLocal()
        try:
            # 1) Автопауза активных, если владелец не в канале
            if _need_owner_membership_check():
                active_for_check = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
                changed = False
                for t in active_for_check:
                    try:
                        ok = await _owner_is_member(t.owner_tg_id)
                    except Exception:
                        ok = False
                    if not ok:
                        t.status = TenantStatus.paused
                        changed = True
                        _write_child_status(t.id, "paused_owner_not_member")
                        print(f"[runner] auto-paused tenant_id={t.id} (owner not member)")
                if changed:
                    db.commit()

            # 2) Список «желательных» активных детей
            active = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            active_ids = {t.id for t in active}
            active_map = {t.id: t for t in active}

            # 3) Остановить тех, кого больше нет в активных
            for tid in list(tasks.keys()):
                if tid not in active_ids:
                    await stop_task(tid)

            # 3.5) Проверка bump-файла: если изменился — мягко перезапускаем всех активных детей
            try:
                mtime = os.path.getmtime(BUMPER_PATH) if os.path.exists(BUMPER_PATH) else 0.0
            except Exception:
                mtime = 0.0
            if mtime and mtime != _last_bump_mtime:
                _last_bump_mtime = mtime
                print("[runner] bump detected — restarting all active children")
                for tid in list(tasks.keys()):
                    await stop_task(tid)

            # 4) Поднять/перезапустить нужных
            for t in active:
                token = (t.child_bot_token or "").strip()
                if not token:
                    # активен без токена — пропускаем
                    continue

                signature = _signature_for(t)
                current = tasks.get(t.id)

                if current is None:
                    # Новый активный — стартуем мгновенно
                    task = asyncio.create_task(_child_entry(t.id), name=f"child-{t.id}")
                    tasks[t.id] = (task, signature)
                    print(f"[runner] started child tenant_id={t.id}")
                    continue

                task, old_signature = current
                # Если поменялся токен/updated_at — перезапустим
                if signature != old_signature:
                    await stop_task(t.id)
                    task = asyncio.create_task(_child_entry(t.id), name=f"child-{t.id}")
                    tasks[t.id] = (task, signature)
                    print(f"[runner] restarted child tenant_id={t.id} (signature changed)")
                else:
                    # Если таска упала — перезапустим
                    if task.done():
                        await stop_task(t.id)
                        task = asyncio.create_task(_child_entry(t.id), name=f"child-{t.id}")
                        tasks[t.id] = (task, signature)
                        print(f"[runner] resurrected child tenant_id={t.id} (task done)")

        finally:
            db.close()

        await asyncio.sleep(CHECK_INTERVAL_SEC)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop_event = asyncio.Event()

    def _handle_signal(sig_name: str):
        print(f"[runner] received {sig_name}, shutting down...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig.name)
        except NotImplementedError:
            pass

    async def _run():
        mgr = asyncio.create_task(manager_loop(), name="children-manager")
        await stop_event.wait()
        mgr.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await mgr

        # Закрыть parent-бота
        global _parent_bot
        if _parent_bot is not None:
            with contextlib.suppress(Exception):
                await _parent_bot.session.close()

    try:
        loop.run_until_complete(_run())
    finally:
        with contextlib.suppress(Exception):
            loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
