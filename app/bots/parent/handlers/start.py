from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from app.settings import settings

router = Router()

@router.message(Command("start", "access", "доступ"))
async def start_access(msg: Message):
    await msg.answer("👋 Отправьте команду /connect, чтобы проверить доступ и подключить вашего бота.")
