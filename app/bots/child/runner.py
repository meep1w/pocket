import asyncio
import contextlib
import signal
from typing import Dict, Optional

from app.db import SessionLocal
from app.models import Tenant, TenantStatus
from app.bots.child.bot_instance import run_child_bot


CHECK_INTERVAL_SEC = 30  # как часто проверять список тенантов/статусы


class Runner:
    """
    Следит за активными тенантами и поднимает/гасит детские боты.
    """
    def __init__(self) -> None:
        self._tasks: Dict[int, asyncio.Task] = {}   # tenant_id -> task
        self._stopping = asyncio.Event()

    async def _start_tenant(self, t: Tenant):
        if t.id in self._tasks:
            return
        async def _task():
            try:
                await run_child_bot(t)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[runner] bot task crashed tenant_id={t.id}: {e!r}")
        task = asyncio.create_task(_task(), name=f"bot-{t.id}")
        self._tasks[t.id] = task
        print(f"[runner] started child bot for tenant_id={t.id}")

    async def _stop_tenant(self, tenant_id: int):
        task = self._tasks.pop(tenant_id, None)
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        print(f"[runner] stopped child bot for tenant_id={tenant_id}")

    def _fetch_active_tenants(self) -> Dict[int, Tenant]:
        db = SessionLocal()
        try:
            rows = db.query(Tenant).filter(Tenant.status == TenantStatus.active).all()
            return {t.id: t for t in rows}
        finally:
            db.close()

    async def _reconcile(self):
        """
        Сопоставляет желаемое состояние (активные тенанты в БД)
        с текущим состоянием задач. Стартует новые, гасит лишние.
        """
        active_map = self._fetch_active_tenants()

        # Стартуем новые
        for tid, t in active_map.items():
            if tid not in self._tasks:
                await self._start_tenant(t)

        # Останавливаем те, кто ушёл из active/удалён
        for tid in list(self._tasks.keys()):
            if tid not in active_map:
                await self._stop_tenant(tid)

    async def serve_forever(self):
        # первичная синхронизация
        await self._reconcile()

        # основной цикл
        while not self._stopping.is_set():
            await asyncio.sleep(CHECK_INTERVAL_SEC)
            await self._reconcile()

        # Грейсфул-шатдаун
        for tid in list(self._tasks.keys()):
            await self._stop_tenant(tid)

    def stop(self):
        self._stopping.set()


runner: Optional[Runner] = None


def _install_signals(loop: asyncio.AbstractEventLoop, r: Runner):
    def _handler():
        print("[runner] shutdown signal received")
        r.stop()
    with contextlib.suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGINT, _handler)
        loop.add_signal_handler(signal.SIGTERM, _handler)


async def main():
    global runner
    runner = Runner()
    loop = asyncio.get_running_loop()
    _install_signals(loop, runner)
    print("[runner] starting…")
    await runner.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
