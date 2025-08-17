import asyncio
from typing import Dict
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from app.db import SessionLocal, init_db, Base
from app.models import Tenant, TenantStatus
from app.settings import settings
from .bot_instance import run_child_bot

class ChildrenManager:
    def __init__(self):
        self.tasks: Dict[int, asyncio.Task] = {}
        self.parent_bot = Bot(
            token=settings.parent_bot_token,
            default=DefaultBotProperties(parse_mode="HTML")
        )

    async def close(self):
        await self.parent_bot.session.close()

    async def refresh(self):
        db = SessionLocal()
        try:
            tenants = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted).all()
        finally:
            db.close()

        current_ids = set(self.tasks.keys())
        want_running = {t.id for t in tenants if t.status == TenantStatus.active}

        # start new
        for tid in want_running - current_ids:
            db = SessionLocal()
            try:
                t = db.query(Tenant).filter(Tenant.id == tid).first()
            finally:
                db.close()
            self.tasks[tid] = asyncio.create_task(run_child_bot(t))

        # stop paused
        for tid in list(current_ids - want_running):
            task = self.tasks.pop(tid, None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

    async def auto_pause_by_membership(self):
        db = SessionLocal()
        try:
            tenants = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted).all()
            for t in tenants:
                try:
                    m = await self.parent_bot.get_chat_member(settings.private_channel_id, t.owner_tg_id)
                    ok = m.status not in ("left", "kicked")
                except Exception:
                    ok = False
                if not ok and t.status == TenantStatus.active:
                    t.status = TenantStatus.paused
                    db.commit()
                if ok and t.status == TenantStatus.paused:
                    t.status = TenantStatus.active
                    db.commit()
        finally:
            db.close()

async def main():
    init_db(Base)
    mgr = ChildrenManager()
    try:
        while True:
            await mgr.refresh()
            await mgr.auto_pause_by_membership()
            await asyncio.sleep(10)
    finally:
        await mgr.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
