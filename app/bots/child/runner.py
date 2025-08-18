import asyncio
from typing import Dict

from app.db import SessionLocal
from app.models import Tenant, TenantStatus
from app.bots.child.bot_instance import run_child_bot


CHECK_INTERVAL_SEC = 5  # как часто сверять состояние в БД


async def manager_loop():
    """
    Держим пул задач для активных тенантов.
    Если тенант стал paused/deleted — гасим его задачу.
    Если появился новый active — запускаем.
    """
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
            # все актуальные активные тенанты
            active_tenants = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            active_ids = {t.id for t in active_tenants}

            # 1) остановить те, кто больше не активен
            for tid in list(tasks.keys()):
                if tid not in active_ids:
                    await stop_task(tid)

            # 2) запустить недостающих
            for t in active_tenants:
                if t.id not in tasks:
                    # запускаем детского бота для этого тенанта
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
