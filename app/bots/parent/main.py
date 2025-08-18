import asyncio
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from app.settings import settings
from app.db import init_db, Base
from .handlers import start as h_start, ga as h_ga, onboarding as h_on
from app.bots.parent.handlers import ga as h_ga

async def main():
    init_db(Base)
    bot = Bot(token=settings.parent_bot_token, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(h_start.router)
    dp.include_router(h_ga.router)
    dp.include_router(h_on.router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
