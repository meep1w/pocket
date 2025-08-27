import asyncio
import signal
from typing import Dict, Optional, Tuple

from aiogram import Bot
from aiogram.exceptions import TelegramUnauthorizedError
from sqlalchemy import text  # (может пригодиться для ручных SQL; не критично)

from app.db import SessionLocal, engine
from app.models import Tenant, TenantStatus
from app.bots.child.bot_instance import run_child_bot
from app.settings import settings

CHECK_INTERVAL_SEC = 5

# Родительский бот – используем только для проверки членства владельца в канале (если включено)
_parent_bot: Optional[Bot] = None

# Печатаем информацию о БД один раз после старта
_DB_DEBUG_DONE = False


def _need_owner_membership_check() -> bool:
    """Включаем проверку только если явно задан private_channel_id."""
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
    """Проверяем, что владелеВц состоит в приватном канале. Если канал не задан — пропускаем проверку."""
    if not _need_owner_membership_check():
        return True
    try:
        bot = await _ensure_parent_bot()
        m = await bot.get_chat_member(settings.private_channel_id, owner_tg_id)
        return m.status not in ("left", "kicked")
    except Exception:
        return False


async def _preflight_token(tenant: Tenant) -> bool:
    """
    Быстрая проверка токена перед запуском polling:
    - getMe (ловим Unauthorized)
    - delete_webhook(drop_pending_updates=True), чтобы убрать возможный вебхук/хвост апдейтов
    """
    token = (tenant.child_bot_token or "").strip()
    if not token:
        print(f"[runner] preflight FAIL empty token: tenant_id={tenant.id}")
        return False

    bot = Bot(token=token)
    try:
        me = await bot.get_me()
        print(f"[runner] preflight OK: tenant_id={tenant.id} bot_id={me.id} @{me.username}")
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            # не критично, просто идём дальше
            pass
        return True
    except TelegramUnauthorizedError:
        print(f"[runner] preflight FAIL Unauthorized: tenant_id={tenant.id}")
        return False
    except Exception as e:
        print(f"[runner] preflight FAIL other: tenant_id={tenant.id} err={e!r}")
        # не помечаем paused на любое исключение, только на Unauthorized
        return True  # дадим шанс run_child_bot самому упасть/перезапуститься
    finally:
        try:
            await bot.session.close()
        except Exception:
            pass


def _pause_tenant(tenant_id: int, reason: str = "unauthorized"):
    """Помечаем тенанта paused в БД (без лишних зависимостей)."""
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(tenant_id)
        if t and t.status != TenantStatus.paused:
            t.status = TenantStatus.paused
            db.commit()
            print(f"[runner] tenant paused: tenant_id={tenant_id} reason={reason}")
    except Exception as e:
        print(f"[runner] tenant pause failed: tenant_id={tenant_id} err={e!r}")
    finally:
        db.close()


async def _child_entry(tenant: Tenant):
    """
    Обёртка вокруг run_child_bot — перезапускает ребёнка при любых исключениях.
    Внутри run_child_bot уже делается delete_webhook(drop_pending_updates=True).
    """
    print(f"[runner] child starting: tenant_id={tenant.id} username={tenant.child_bot_username}")

    # Предполётная проверка токена. Если 401 — автопауза и выходим без бесконечного цикла.
    ok = await _preflight_token(tenant)
    if not ok:
        _pause_tenant(tenant.id, reason="unauthorized_preflight")
        return

    while True:
        try:
            await run_child_bot(tenant)  # держит polling до остановки
            print(
                f"[runner] run_child_bot RETURNED: tenant_id={tenant.id} username={tenant.child_bot_username}; restart in 5s"
            )
        except asyncio.CancelledError:
            print(f"[runner] child cancelled: tenant_id={tenant.id} username={tenant.child_bot_username}")
            raise
        except TelegramUnauthorizedError:
            # Если Unauthorized сорвался уже в процессе — автопауза и выходим
            print(f"[runner] child Unauthorized during run: tenant_id={tenant.id}")
            _pause_tenant(tenant.id, reason="unauthorized_during_run")
            return
        except Exception as e:
            print(
                f"[runner] child crashed: tenant_id={tenant.id} username={tenant.child_bot_username} exc={e!r}; restart in 5s"
            )
        await asyncio.sleep(5)


async def manager_loop():
    global _DB_DEBUG_DONE
    # tasks[tenant_id] = (asyncio.Task, signature)
    # signature = (token, username) — чтобы понимать, что конфиг изменился и ребёнка надо перезапустить
    tasks: Dict[int, Tuple[asyncio.Task, Tuple[str, str]]] = {}

    async def stop_task(tid: int):
        rec = tasks.pop(tid, None)
        if not rec:
            return
        task = rec[0]
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def ensure_db_debug_once():
        global _DB_DEBUG_DONE
        if _DB_DEBUG_DONE:
            return
        try:
            print("[runner][DB] engine.url:", engine.url)
            with engine.connect() as conn:
                dblist = conn.exec_driver_sql("PRAGMA database_list;").all()
                print("[runner][DB] PRAGMA database_list:", dblist)

                cols = conn.exec_driver_sql("PRAGMA table_info(tenants);").all()
                print("[runner][DB] tenants columns:", [c[1] for c in cols])

                try:
                    _ = conn.exec_driver_sql("SELECT channel_url FROM tenants LIMIT 1;").all()
                    print("[runner][DB] raw SELECT channel_url: OK")
                except Exception as e:
                    print("[runner][DB] raw SELECT channel_url: FAIL ->", repr(e))
        except Exception as e:
            print("[runner][DB] introspection error:", repr(e))
        finally:
            _DB_DEBUG_DONE = True

    while True:
        await ensure_db_debug_once()

        db = SessionLocal()
        try:
            # 1) Автопауза тенантов, если включена проверка членства владельца
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
                if changed:
                    db.commit()

            # 2) Считываем актуальные активные тенанты
            active = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            active_map: Dict[int, Tenant] = {t.id: t for t in active}
            active_ids = set(active_map.keys())

            # 3) Остановить тех, кого больше нет в активных
            for tid in list(tasks.keys()):
                if tid not in active_ids:
                    await stop_task(tid)

            # 4) Старт/рестарт активных:
            #    - нет задачи → стартуем
            #    - токен/юзернейм изменились → перезапускаем
            for t in active:
                token = (t.child_bot_token or "").strip()
                username = (t.child_bot_username or "").strip()
                signature = (token, username)

                current = tasks.get(t.id)
                if current is None:
                    # старт нового ребёнка
                    task = asyncio.create_task(_child_entry(t))
                    tasks[t.id] = (task, signature)
                    continue

                task, old_signature = current
                if signature != old_signature:
                    # конфиг изменился — перезапускаем ребёнка
                    await stop_task(t.id)
                    task = asyncio.create_task(_child_entry(t))
                    tasks[t.id] = (task, signature)

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

    # ловим SIGINT/SIGTERM для корректной остановки
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig.name)
        except NotImplementedError:
            # на Windows сигналы могут не поддерживаться
            pass

    async def _run():
        mgr = asyncio.create_task(manager_loop())
        await stop_event.wait()
        mgr.cancel()
        try:
            await mgr
        except asyncio.CancelledError:
            pass
        # закрыть родительского бота
        global _parent_bot
        if _parent_bot is not None:
            try:
                await _parent_bot.session.close()
            except Exception:
                pass

    try:
        loop.run_until_complete(_run())
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    main()
