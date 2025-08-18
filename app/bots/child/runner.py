import asyncio
from typing import Dict

from aiogram import Bot
from app.db import SessionLocal
from app.models import Tenant, TenantStatus
from app.bots.child.bot_instance import run_child_bot
from app.settings import settings

CHECK_INTERVAL_SEC = 5

parent_bot = Bot(token=settings.parent_bot_token)  # для проверки членства


async def _owner_is_member(owner_tg_id: int) -> bool:
    try:
        m = await parent_bot.get_chat_member(settings.private_channel_id, owner_tg_id)
        return m.status not in ("left", "kicked")
    except Exception:
        return False


async def manager_loop():
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
        db = SessionLocal()
        try:
            # автопауза, если владелец не в канале
            active = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            for t in active:
                ok = await _owner_is_member(t.owner_tg_id)
                if not ok:
                    t.status = TenantStatus.paused
            db.commit()

            # пересобираем активных
            active = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            active_ids = {t.id for t in active}

            # погасить лишние
            for tid in list(tasks.keys()):
                if tid not in active_ids:
                    await stop_task(tid)

            # запустить недостающих
            for t in active:
                if t.id not in tasks:
                    tasks[t.id] = asyncio.create_task(run_child_bot(t))
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
