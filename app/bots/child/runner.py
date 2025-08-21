import asyncio
from typing import Dict, Optional

from aiogram import Bot
from sqlalchemy import text

from app.db import SessionLocal, engine
from app.models import Tenant, TenantStatus
from app.bots.child.bot_instance import run_child_bot
from app.settings import settings

CHECK_INTERVAL_SEC = 5

# Бот-родитель: нужен только для проверки членства владельца в канале
parent_bot = Bot(token=settings.parent_bot_token)

# Глобальный флаг: печатаем отладку БД один раз
_DB_DEBUG_DONE = False


async def _owner_is_member(owner_tg_id: int) -> bool:
    """Проверка, что владелец (owner_tg_id) состоит в приватном канале.
    Если упали на любом исключении — считаем, что не состоит.
    """
    try:
        m = await parent_bot.get_chat_member(settings.private_channel_id, owner_tg_id)
        return m.status not in ("left", "kicked")
    except Exception:
        return False


async def _child_entry(t: Tenant):
    """Обёр тка вокруг run_child_bot, чтобы не падать тихо и перезапускаться при сбоях."""
    print(f"[runner] child starting: tenant_id={t.id} username={t.child_bot_username}")
    while True:
        try:
            await run_child_bot(t)  # должен висеть, пока идёт polling
            print(f"[runner] run_child_bot RETURNED: tenant_id={t.id} username={t.child_bot_username}; restart in 5s")
        except asyncio.CancelledError:
            print(f"[runner] child cancelled: tenant_id={t.id} username={t.child_bot_username}")
            raise
        except Exception as e:
            print(f"[runner] child crashed: tenant_id={t.id} username={t.child_bot_username} exc={e!r}; restart in 5s")
        await asyncio.sleep(5)


async def manager_loop():
    global _DB_DEBUG_DONE
    tasks: Dict[int, asyncio.Task] = {}

    async def stop_task(tid: int):
        task = tasks.pop(tid, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    while True:
        # --- одноразовая отладка БД ---
        if not _DB_DEBUG_DONE:
            try:
                print("[runner][DB] engine.url:", engine.url)
                with engine.connect() as conn:
                    dblist = conn.exec_driver_sql("PRAGMA database_list;").all()
                    print("[runner][DB] PRAGMA database_list:", dblist)

                    cols = conn.exec_driver_sql("PRAGMA table_info(tenants);").all()
                    print("[runner][DB] tenants columns:", [c[1] for c in cols])

                    # Проверим конкретно наличие столбца реальным запросом
                    try:
                        _ = conn.exec_driver_sql("SELECT channel_url FROM tenants LIMIT 1;").all()
                        print("[runner][DB] raw SELECT channel_url: OK")
                    except Exception as e:
                        print("[runner][DB] raw SELECT channel_url: FAIL ->", repr(e))
            except Exception as e:
                print("[runner][DB] introspection error:", repr(e))
            finally:
                _DB_DEBUG_DONE = True
        # --- конец отладки ---

        db = SessionLocal()
        try:
            # автопауза: если владелец не в канале — переводим в paused
            active = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            for t in active:
                try:
                    ok = await _owner_is_member(t.owner_tg_id)
                except Exception:
                    ok = False
                if not ok:
                    t.status = TenantStatus.paused
            db.commit()

            # перечитаем активных
            active = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            active_ids = {t.id for t in active}

            # погасить лишние
            for tid in list(tasks.keys()):
                if tid not in active_ids:
                    await stop_task(tid)

            # запустить недостающих
            for t in active:
                if t.id not in tasks:
                    task = asyncio.create_task(_child_entry(t))
                    tasks[t.id] = task
        finally:
            db.close()

        await asyncio.sleep(CHECK_INTERVAL_SEC)


def main():
    try:
        asyncio.run(manager_loop())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
