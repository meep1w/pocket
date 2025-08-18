import re
from secrets import token_urlsafe
from aiogram import Router, Bot, F
from aiogram.filters import Command
from aiogram.types import Message
from app.settings import settings
from app.db import SessionLocal
from app.models import Tenant, TenantStatus

router = Router()

TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{20,}$")

async def is_member(bot: Bot, user_id: int) -> bool:
    """Проверка, что пользователь в приватном канале"""
    try:
        m = await bot.get_chat_member(settings.private_channel_id, user_id)
        return m.status not in ("left", "kicked")
    except Exception:
        return False


# === 1. /connect ===
@router.message(Command("connect"))
async def cmd_connect(msg: Message):
    db = SessionLocal()
    try:
        # проверка: у владельца уже есть бот?
        exists = db.query(Tenant).filter(
            Tenant.owner_tg_id == msg.from_user.id,
            Tenant.status.in_(["active", "paused"])
        ).first()
        if exists:
            await msg.answer(
                f"Извините, но нельзя подключать больше одного бота.\n"
                f"Сейчас привязан: <b>{exists.child_bot_username or 'без имени'}</b>.\n\n"
                f"Если хотите заменить — запросите удаление у ГА."
            )
            return
    finally:
        db.close()

    # проверка на членство
    bot = msg.bot
    if not await is_member(bot, msg.from_user.id):
        await msg.answer("⛔️ Вы не участник моего приватного канала. Доступ запрещён.")
        return

    await msg.answer(
        "✅ Доступ подтверждён. "
        "Пришлите <b>API-токен</b> вашего бота (формат <code>123456:ABC...</code>)."
    )


# === 2. Приём токена ===
@router.message(lambda m: bool(TOKEN_RE.match(m.text or "")))
async def got_token(msg: Message):
    # проверка на членство
    bot = msg.bot
    if not await is_member(bot, msg.from_user.id):
        await msg.answer("⛔️ Вы не участник приватного канала.")
        return

    token = (msg.text or "").strip()

    # проверка токена у Telegram
    try:
        test_bot = Bot(token=token)
        me = await test_bot.get_me()
        username = me.username
        if not username:
            raise ValueError("У бота нет username")
    except Exception:
        await msg.answer("❌ Токен невалиден. Проверьте и пришлите ещё раз.")
        return

    db = SessionLocal()
    try:
        t = Tenant(
            owner_tg_id=msg.from_user.id,
            child_bot_token=token,
            child_bot_username=f"@{username}",
            status=TenantStatus.active,
        )
        if settings.tenant_secret_mode == "enabled":
            t.postback_secret = token_urlsafe(24)

        db.add(t)
        db.commit()
    finally:
        db.close()

    await msg.answer(
        f"🤖 Бот <b>@{username}</b> подключён!\n"
        f"Перейдите в него и введите <code>/admin</code> для настройки."
    )
