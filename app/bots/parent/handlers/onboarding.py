import re
from secrets import token_urlsafe
from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy import and_

from app.settings import settings
from app.db import SessionLocal
from app.models import Tenant, TenantStatus

router = Router()

TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{20,}$")


def membership_required() -> bool:
    # включаем проверку только если приватный канал задан
    return bool(getattr(settings, "private_channel_id", None))


async def is_member(bot: Bot, user_id: int) -> bool:
    if not membership_required():
        return True
    try:
        m = await bot.get_chat_member(settings.private_channel_id, user_id)
        return m.status not in ("left", "kicked")
    except Exception:
        return False


def _has_any_attached(db, owner_id: int) -> Tenant | None:
    return db.query(Tenant).filter(
        and_(Tenant.owner_tg_id == owner_id, Tenant.status.in_([TenantStatus.active, TenantStatus.paused]))
    ).first()


@router.message(Command("connect"))
async def cmd_connect(msg: Message):
    db = SessionLocal()
    try:
        exists = _has_any_attached(db, msg.from_user.id)
        if exists:
            await msg.answer(
                "Извините, но нельзя подключать больше одного бота.\n"
                f"Сейчас привязан: <b>{exists.child_bot_username or 'без имени'}</b>."
            )
            return
    finally:
        db.close()

    if not await is_member(msg.bot, msg.from_user.id):
        await msg.answer("⛔️ Вы не участник приватного канала. Доступ запрещён.")
        return

    await msg.answer(
        "✅ Доступ подтверждён. Пришлите <b>API-токен</b> вашего бота (формат <code>123456:ABC...</code>)."
    )


@router.message(lambda m: bool(TOKEN_RE.match(m.text or "")))
async def got_token(msg: Message):
    if not await is_member(msg.bot, msg.from_user.id):
        await msg.answer("⛔️ Вы не участник приватного канала.")
        return

    token = (msg.text or "").strip()

    # проверяем токен у Telegram
    test_bot = Bot(token=token)
    try:
        me = await test_bot.get_me()
        username = me.username
        if not username:
            raise ValueError("У бота нет username")

        # снимаем вебхук и чистим подвисшие апдейты
        try:
            await test_bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
    except Exception:
        await msg.answer("❌ Токен невалиден. Проверьте и пришлите ещё раз.")
        return
    finally:
        try:
            await test_bot.session.close()
        except Exception:
            pass

    db = SessionLocal()
    try:
        # UPSERT: если у владельца уже есть бот — обновляем запись
        t = db.query(Tenant).filter(Tenant.owner_tg_id == msg.from_user.id).one_or_none()
        if t is None:
            # fallback: ищем по username (если запись осталась без owner_id)
            t = db.query(Tenant).filter(Tenant.child_bot_username == f"@{username}").one_or_none()

        if t:
            t.child_bot_token = token
            t.child_bot_username = f"@{username}"
            t.status = TenantStatus.active
        else:
            t = Tenant(
                owner_tg_id=msg.from_user.id,
                child_bot_token=token,
                child_bot_username=f"@{username}",
                status=TenantStatus.active,
            )
            if getattr(settings, "tenant_secret_mode", "") == "enabled":
                t.postback_secret = token_urlsafe(24)
            db.add(t)

        db.commit()
    finally:
        db.close()

    await msg.answer(
        f"🤖 Бот <b>@{username}</b> подключён!\n"
        f"Перейдите в него и введите <code>/admin</code> для настройки."
    )
