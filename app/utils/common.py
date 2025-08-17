from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from typing import Optional

async def safe_delete_message(bot: Bot, chat_id: int, message_id: Optional[int]):
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest:
        pass
    except Exception:
        pass
