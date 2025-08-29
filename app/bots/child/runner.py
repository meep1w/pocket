# app/bots/child/bot_instance.py
import asyncio
import contextlib
from typing import Optional, List, Tuple

from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, FSInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command

from sqlalchemy import func

from app.models import Tenant, User, UserStep, TenantText, TenantConfig, Postback, TenantStatus
from app.db import SessionLocal
from app.settings import settings
from app.utils.common import safe_delete_message

from pathlib import Path

# ---------------------- ЭКРАНЫ / КЛЮЧИ ----------------------
KEYS: List[Tuple[str, dict]] = [
    ("lang",       {"ru": "Выбор языка",        "en": "Language"}),
    ("main",       {"ru": "Главное меню",       "en": "Main menu"}),
    ("guide",      {"ru": "Инструкция",         "en": "Instruction"}),
    ("subscribe",  {"ru": "Подписка на канал",  "en": "Subscribe"}),
    ("step1",      {"ru": "Шаг 1. Регистрация", "en": "Step 1. Registration"}),
    ("step2",      {"ru": "Шаг 2. Депозит",     "en": "Step 2. Deposit"}),
    ("unlocked",   {"ru": "Доступ открыт",      "en": "Access granted"}),

    # --- Кнопки (кастомизируемые из админки «Контент»)
    ("btn_main",          {"ru": "Кнопка: Главное меню",   "en": "Button: Main menu"}),
    ("btn_instruction",   {"ru": "Кнопка: Инструкция",     "en": "Button: Instruction"}),
    ("btn_get_signal",    {"ru": "Кнопка: Получить сигнал","en": "Button: Get signal"}),
    ("btn_support",       {"ru": "Кнопка: Поддержка",      "en": "Button: Support"}),
    ("btn_change_lang",   {"ru": "Кнопка: Сменить язык",   "en": "Button: Change language"}),
    ("btn_go_channel",    {"ru": "Кнопка: Перейти в канал","en": "Button: Go to channel"}),
    ("btn_ive_subscribed",{"ru": "Кнопка: Я подписался",   "en": "Button: I've subscribed"}),
]

DEFAULT_TEXTS = {
    # --- Экраны
    "lang": {"ru": "Выберите язык", "en": "Choose your language"},
    "main": {"ru": "Главное меню", "en": "Main menu"},
    "guide": {
        "ru": (
            "1. Зарегистрируйте аккаунт на брокере {{ref}}, обязательно через нашего бота, "
            "для этого введите /start > Получить сигнал > Зарегистрироваться\n"
            "2. Ожидайте автоматической проверки регистрации, бот вас оповестит.\n"
            "3. После успешной проверки внесите депозит, для этого введите /start > Получить сигнал > Внести депозит\n"
            "4. Ожидайте автоматической проверки депозита, бот вас оповестит.\n"
            "5. Нажмите «Получить сигнал».\n"
            "6. Выберите инструмент для торговли в первой строчке интерфейса бота.\n"
            "7. Дублируйте этот инструмент на брокере {{ref}}.\n"
            "8. Выберите модель торговли TESSA Plus для обычных пользователей, TESSA Quantum для платинум пользователей.\n"
            "9. Выберите любое время экспирации.\n"
            "10. Дублируйте тоже самое время экспирации на брокере {{ref}}.\n"
            "11. Нажмите кнопку «Сгенерировать сигнал» и торгуйте строго исходя из аналитики бота, старайтесь подбирать более высокую вероятность.\n"
            "12. Заработайте профит."
        ),
        "en": (
            "1. Register an account on the broker {{ref}}, be sure to use our bot, "
            "to do this, enter /start > Receive signal > Register\n"
            "2. Expect automatic registration verification, and the bot will notify you.\n"
            "3. After successful verification, make a deposit by entering /start > Receive signal > Make a deposit \n"
            "4. Wait for the automatic verification of the deposit, the bot will notify you.\n"
            "5. Click «Receive signal».\n"
            "6. Select a trading tool in the first line of the bot interface.\n"
            "7. Duplicate this tool on the broker {{ref}}."
            "8. Select the trading model TESSA Plus for regular users, TESSA Quantum for platinum users.\n"
            "9. Select any expiration time.\n"
            "10. Duplicate the same expiration time on the broker {{ref}}"
            "11. Click the «Generate signal» button and trade strictly based on the bot's analytics, try to select a higher probability.\n"
            "12. Earn a profit."
        ),
    },
    "subscribe": {
        "ru": "Для начала подпишитесь на канал.\n\nПосле подписки вернитесь в бот.",
        "en": "First, subscribe to the channel.\n\nAfter subscribing, return to the bot.",
    },
    "step1": {
        "ru": "⚡️Регистрация\n\nДля получения сигналов нужно зарегистрироваться по нашей ссылке.",
        "en": "⚡️Registration\n\nTo receive signals, you need to register via our link.",
    },
    "step2": {
        "ru": "⚡️Внесите депозит: ${{min_dep}}.",
        "en": "⚡️Make a deposit: ${{min_dep}}.",
    },
    "unlocked": {
        "ru": "🎉 Доступ открыт. Нажмите «Получить сигнал».",
        "en": "🎉 Access granted. Press “Get signal”."
    },

    # --- Кнопки (дефолтные подписи RU/EN)
    "btn_main":          {"ru": "🏠 Главное меню",   "en": "🏠 Main menu"},
    "btn_instruction":   {"ru": "📘 Инструкция",     "en": "📘 Instruction"},
    "btn_get_signal":    {"ru": "📈 Получить сигнал","en": "📈 Get signal"},
    "btn_support":       {"ru": "🆘 Поддержка",      "en": "🆘 Support"},
    "btn_change_lang":   {"ru": "🌐 Сменить язык",   "en": "🌐 Change language"},
    "btn_go_channel":    {"ru": "🚀 Перейти в канал","en": "🚀 Go to channel"},
    "btn_ive_subscribed":{"ru": "✅ Я подписался",   "en": "✅ I've subscribed"},
}

def apply_placeholders(text: str, tenant: Tenant) -> str:
    ref = (getattr(tenant, "ref_link", None) or "").strip()
    if "{{ref}}" in text:
        if ref:
            return text.replace("{{ref}}", f'<a href="{ref}">PocketOption</a>')
        else:
            return text.replace("{{ref}}", "PocketOption")
    return text

def key_title(key: str, locale: str) -> str:
    for k, names in KEYS:
        if k == key:
            return names.get(locale, k)
    return key

def default_text(key: str, locale: str) -> str:
    return DEFAULT_TEXTS.get(key, {}).get(locale, key)

def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]

def _find_stock_file(key: str, locale: str) -> Path | None:
    stock = _project_root() / "static" / "stock"
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = stock / f"{key}-{locale}.{ext}"
        if p.exists():
            return p
    return None

# ---------------------- ПОДПИСКА ----------------------
def _as_chat_ident(channel_url: str) -> Optional[str]:
    if not channel_url:
        return None
    u = channel_url.strip()
    if not u:
        return None
    if u.startswith("@") or u.startswith("-100"):
        return u
    if "t.me/" in u:
        tail = u.split("t.me/", 1)[1]
        name = tail.split("/", 1)[0].lstrip("@")
        if name and not (name.startswith("+") or name.lower().startswith("joinchat")):
            return f"@{name}"
    return None

async def is_user_subscribed(bot: Bot, channel_url: str, user_id: int) -> bool:
    ident = _as_chat_ident(channel_url or "")
    if not ident:
        return True
    try:
        member = await bot.get_chat_member(ident, user_id)
        status = getattr(member, "status", None)
        return status in ("member", "administrator", "creator", "restricted")
    except Exception as e:
        print(f"[subscribe-check] error: {e}")
        return True

# ---------------------- УТИЛЫ ДЛЯ СВЕЖИХ ДАННЫХ ----------------------
def tget(db, tenant_id: int, key: str, locale: str, fallback_text: str):
    tt = db.query(TenantText).filter(
        TenantText.tenant_id == tenant_id,
        TenantText.locale == locale,
        TenantText.key == key,
    ).first()
    return (tt.text if tt and tt.text else fallback_text), (tt.image_file_id if tt else None)

def tget_label(db, tenant_id: int, key: str, locale: str) -> str:
    # для кнопок — только текстовая часть
    txt, _ = tget(db, tenant_id, key, locale, default_text(key, locale))
    return txt

def get_cfg(db: SessionLocal, tenant_id: int) -> TenantConfig:
    cfg = db.query(TenantConfig).filter(TenantConfig.tenant_id == tenant_id).first()
    if not cfg:
        cfg = TenantConfig(
            tenant_id=tenant_id,
            require_deposit=True,
            min_deposit=50,
            require_subscription=False,
            vip_threshold=500,
        )
        db.add(cfg)
        db.commit()
        db.refresh(cfg)
    if getattr(cfg, "require_subscription", None) is None:
        cfg.require_subscription = False
        db.commit(); db.refresh(cfg)
    if getattr(cfg, "vip_threshold", None) is None:
        cfg.vip_threshold = 500
        db.commit(); db.refresh(cfg)
    return cfg

def get_fresh_tenant(db: SessionLocal, tenant_id: int) -> Tenant:
    # всегда берём свежие данные (support_url/miniapp_url/ref_link/channel_url)
    return db.query(Tenant).filter(Tenant.id == tenant_id).first()

def get_deposit_total(db, tenant_id: int, user: User) -> int:
    total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
        Postback.tenant_id == tenant_id,
        Postback.event == "deposit",
        Postback.click_id == str(user.tg_user_id),
        Postback.token_ok.is_(True),
    ).scalar() or 0
    return int(total)

# -------------------------- ОТПРАВКА ЭКРАНА --------------------------
async def send_screen(bot, user, key: str, locale: str, text: str, kb, image_file_id: str | None):
    await safe_delete_message(bot, user.tg_user_id, user.last_message_id)
    if image_file_id:
        try:
            m = await bot.send_photo(user.tg_user_id, image_file_id, caption=text, reply_markup=kb)
            user.last_message_id = m.message_id
            return
        except TelegramBadRequest:
            pass
        except Exception:
            pass
    p = _find_stock_file(key, locale)
    if p:
        try:
            m = await bot.send_photo(user.tg_user_id, FSInputFile(str(p)), caption=text, reply_markup=kb)
            user.last_message_id = m.message_id
            return
        except Exception:
            pass
    m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)
    user.last_message_id = m.message_id

# -------------------------- URL МИНИ-АППЫ --------------------------
def tenant_miniapp_url(tenant: Tenant, user: User) -> str:
    if getattr(user, "vip_miniapp_url", None):
        base = user.vip_miniapp_url.rstrip("/")
        return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

    is_vip = bool(getattr(user, "is_vip", False))
    vip_env = getattr(settings, "vip_miniapp_url", None)
    if is_vip and vip_env:
        base = vip_env.rstrip("/")
        return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

    base = (tenant.miniapp_url or settings.miniapp_url).rstrip("/")
    return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

# ------------------------------- КНОПКИ -------------------------------
def _normalize_support_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if u.startswith("@"):
        return f"https://t.me/{u[1:]}"
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("t.me/") or "t.me/" in u:
        return "https://" + u.lstrip("/")
    return None

def kb_main_with_labels(locale: str, support_url: Optional[str], tenant: Tenant, user: User, has_access: bool,
                        btn_instruction: str, btn_support: str, btn_change_lang: str, btn_get_signal: str):
    # Если доступ есть — сразу WebApp; иначе ведём по шагам
    if has_access:
        signal_btn = InlineKeyboardButton(
            text=btn_get_signal,
            web_app=WebAppInfo(url=tenant_miniapp_url(tenant, user)),
        )
    else:
        signal_btn = InlineKeyboardButton(
            text=btn_get_signal,
            callback_data="menu:get",
        )

    support_fallback = _normalize_support_url(support_url) or "https://t.me"
    rows = [
        [InlineKeyboardButton(text=btn_instruction, callback_data="menu:guide")],
        [InlineKeyboardButton(text=btn_support, url=support_fallback),
         InlineKeyboardButton(text=btn_change_lang, callback_data="menu:lang")],
        [signal_btn],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back_text(btn_main_text: str):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=btn_main_text, callback_data="menu:main")]])

def kb_lang(current: Optional[str], btn_main_text: str):
    ru = ("✅ " if current == "ru" else "") + "🇷🇺 Русский"
    en = ("✅ " if current == "en" else "") + "🇬🇧 English"
    rows = [
        [InlineKeyboardButton(text=ru, callback_data="lang:ru"),
         InlineKeyboardButton(text=en, callback_data="lang:en")],
        [InlineKeyboardButton(text=btn_main_text, callback_data="menu:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_subscribe(locale: str, channel_url: str, btn_go_channel: str, btn_ive_subscribed: str, btn_main_text: str) -> InlineKeyboardMarkup:
    url_raw = (channel_url or "").strip()
    if url_raw.startswith("@"):
        url = f"https://t.me/{url_raw[1:]}"
    elif url_raw.startswith("-100"):
        url = "https://t.me"
    elif url_raw.startswith("http://") or url_raw.startswith("https://"):
        url = url_raw
    elif url_raw.startswith("t.me/") or "t.me/" in url_raw:
        url = "https://" + url_raw.lstrip("/")
    else:
        url = "https://t.me"

    rows = [
        [InlineKeyboardButton(text=btn_go_channel, url=url)],
        [InlineKeyboardButton(text=btn_ive_subscribed, callback_data="menu:subcheck")],
        [InlineKeyboardButton(text=btn_main_text, callback_data="menu:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --------------------------- РЕНДЕР ЭКРАНОВ: UI ---------------------------
async def render_lang_screen(bot: Bot, tenant: Tenant, user: User, current_lang: Optional[str]):
    db = SessionLocal()
    try:
        # берём свежего тенанта только для контента (на всякий случай)
        tenant = get_fresh_tenant(db, tenant.id) or tenant

        locale = (current_lang or tenant.lang_default or "ru").lower()
        text, img = tget(db, tenant.id, "lang", locale, default_text("lang", locale))
        btn_main = tget_label(db, tenant.id, "btn_main", locale)

        await safe_delete_message(bot, user.tg_user_id, user.last_message_id)
        rm = kb_lang(current_lang, btn_main)

        try:
            if img:
                m = await bot.send_photo(user.tg_user_id, img, caption=text, reply_markup=rm)
            else:
                p = _find_stock_file("lang", locale)
                if p:
                    m = await bot.send_photo(user.tg_user_id, FSInputFile(str(p)), caption=text, reply_markup=rm)
                else:
                    m = await bot.send_message(user.tg_user_id, text, reply_markup=rm)
        except Exception:
            m = await bot.send_message(user.tg_user_id, text, reply_markup=rm)

        user.last_message_id = m.message_id
        db.commit()
    finally:
        db.close()

async def render_main(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        # СВЕЖИЕ ДАННЫЕ тенанта → обновится support_url/miniapp_url/etc
        tenant = get_fresh_tenant(db, tenant.id) or tenant

        locale = user.lang or tenant.lang_default or "ru"
        cfg = get_cfg(db, tenant.id)
        has_access = (user.step == UserStep.deposited) or (not cfg.require_deposit and user.step >= UserStep.registered)

        text, img = tget(db, tenant.id, "main", locale, default_text("main", locale))

        # Кастомные подписи кнопок
        btn_instruction = tget_label(db, tenant.id, "btn_instruction", locale)
        btn_support = tget_label(db, tenant.id, "btn_support", locale)
        btn_change_lang = tget_label(db, tenant.id, "btn_change_lang", locale)
        btn_get_signal = tget_label(db, tenant.id, "btn_get_signal", locale)

        kb = kb_main_with_labels(
            locale, tenant.support_url, tenant, user, has_access,
            btn_instruction, btn_support, btn_change_lang, btn_get_signal
        )
        await send_screen(bot, user, "main", locale, text, kb, img)
        db.commit()
    finally:
        db.close()

async def render_guide(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant
        locale = user.lang or tenant.lang_default or "ru"
        t, i = tget(db, tenant.id, "guide", locale, default_text("guide", locale))
        t = apply_placeholders(t, tenant)
        btn_main = tget_label(db, tenant.id, "btn_main", locale)
        await send_screen(bot, user, "guide", locale, t, kb_back_text(btn_main), i)
        db.commit()
    finally:
        db.close()

async def render_subscribe(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant
        locale = user.lang or tenant.lang_default or "ru"
        text, img = tget(db, tenant.id, "subscribe", locale, default_text("subscribe", locale))

        btn_go = tget_label(db, tenant.id, "btn_go_channel", locale)
        btn_chk = tget_label(db, tenant.id, "btn_ive_subscribed", locale)
        btn_main = tget_label(db, tenant.id, "btn_main", locale)

        kb = kb_subscribe(locale, tenant.channel_url or "", btn_go, btn_chk, btn_main)
        await send_screen(bot, user, "subscribe", locale, text, kb, img)
        db.commit()
    finally:
        db.close()

async def render_get(bot: Bot, tenant: Tenant, user: User, force_unlocked: bool = False):
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant
        locale = user.lang or tenant.lang_default or "ru"
        cfg = get_cfg(db, tenant.id)

        # 0) Подписка
        if getattr(cfg, "require_subscription", False):
            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                await render_subscribe(bot, tenant, user)
                db.commit()
                return

        # Доступ разрешён?
        access = (user.step == UserStep.deposited) or (not cfg.require_deposit and user.step >= UserStep.registered)
        if force_unlocked or access:
            if not getattr(user, "access_notified", False):
                text, img = tget(db, tenant.id, "unlocked", locale, default_text("unlocked", locale))
                btn_get = tget_label(db, tenant.id, "btn_get_signal", locale)
                btn_main = tget_label(db, tenant.id, "btn_main", locale)
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=btn_get,
                                web_app=WebAppInfo(url=tenant_miniapp_url(tenant, user)),
                            )
                        ],
                        [InlineKeyboardButton(text=btn_main, callback_data="menu:main")],
                    ]
                )
                await send_screen(bot, user, "unlocked", locale, text, kb, img)
                user.access_notified = True
                db.commit()
                return

            await render_main(bot, tenant, user)
            db.commit()
            return

        # Шаг 1 — Регистрация
        if user.step in (UserStep.new, UserStep.asked_reg):
            text, img = tget(db, tenant.id, "step1", locale, default_text("step1", locale))
            url = f"{settings.service_host}/r/reg?tenant_id={tenant.id}&uid={user.tg_user_id}"
            btn_main = tget_label(db, tenant.id, "btn_main", locale)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=("🟢  Зарегистрироваться" if locale == "ru" else "🟢  Register"), url=url)],
                    [InlineKeyboardButton(text=btn_main, callback_data="menu:main")],
                ]
            )
            user.step = UserStep.asked_reg
            await send_screen(bot, user, "step1", locale, text, kb, img)
            db.commit()
            return

        # Шаг 2 — Депозит (если обязателен)
        text, img = tget(db, tenant.id, "step2", locale, default_text("step2", locale))
        text = text.replace("{{min_dep}}", str(cfg.min_deposit))

        dep_total = get_deposit_total(db, tenant.id, user)
        left = max(0, cfg.min_deposit - dep_total)
        progress_line = (
            f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${left})"
            if locale == "ru"
            else f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${left})"
        )
        text = text + progress_line

        # VIP-инфо по порогу
        try:
            thr = int(getattr(cfg, "vip_threshold", 500) or 500)
            if dep_total >= thr and not getattr(user, "vip_notified", False):
                msg_txt = (
                    "🎉 Поздравляем! Вам доступен премиум-бот. Напишите в поддержку для подключения."
                    if locale == "ru" else
                    "🎉 Congrats! You’re eligible for the premium bot. Please contact support to get access."
                )
                await bot.send_message(user.tg_user_id, msg_txt)
                user.vip_notified = True
        except Exception as e:
            print(f"[vip-notify] {e}")

        url = f"{settings.service_host}/r/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
        btn_main = tget_label(db, tenant.id, "btn_main", locale)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=("💳 Внести депозит" if locale == "ru" else "💳 Deposit"), url=url)],
                [InlineKeyboardButton(text=btn_main, callback_data="menu:main")],
            ]
        )
        user.step = UserStep.asked_deposit
        await send_screen(bot, user, "step2", locale, text, kb, img)
        db.commit()
    finally:
        db.close()

# ------------------------------- MIDDLEWARE -------------------------------
class TenantGate(BaseMiddleware):
    def __init__(self, tenant_id: int):
        super().__init__()
        self.tenant_id = tenant_id

    async def __call__(self, handler, event, data):
        try:
            db = SessionLocal()
            try:
                t = db.query(Tenant).filter(Tenant.id == self.tenant_id).first()
                status = t.status if t else TenantStatus.deleted
            finally:
                db.close()

            if status != TenantStatus.active:
                if isinstance(event, Message):
                    await event.answer("⏸ Бот на паузе / удалён.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("⏸ Бот на паузе / удалён.", show_alert=False)
                return
        except Exception as e:
            print(f"[TenantGate] error: {e}")
        return await handler(event, data)

# --------------------------------- ADMIN FSM ---------------------------------
class AdminForm(StatesGroup):
    waiting_support = State()
    waiting_ref = State()
    waiting_dep = State()
    waiting_miniapp = State()
    waiting_channel = State()

    # VIP
    vip_wait_user_id = State()
    vip_wait_url = State()
    vip_wait_threshold = State()
    vip_wait_miniapp_url = State()

    content_wait_lang = State()
    content_wait_key = State()
    content_wait_text = State()
    content_wait_photo = State()

    bcast_wait_segment = State()
    bcast_wait_content = State()
    bcast_confirm = State()

    params_wait_min_dep = State()

# ----------------------------- АДМИН КНОПКИ/МЕНЮ -----------------------------
def kb_admin_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Ссылки", callback_data="adm:links")],
            [InlineKeyboardButton(text="🔁 Постбэки", callback_data="adm:pb")],
            [InlineKeyboardButton(text="🧩 Контент", callback_data="adm:content")],
            [InlineKeyboardButton(text="⚙️ Параметры", callback_data="adm:params")],
            [InlineKeyboardButton(text="👑 PLATINUM", callback_data="adm:vip")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        ]
    )

def kb_admin_links():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить Support URL", callback_data="adm:set:support")],
            [InlineKeyboardButton(text="✏️ Изменить Реф. ссылку", callback_data="adm:set:ref")],
            [InlineKeyboardButton(text="✏️ Изменить ссылку депозита", callback_data="adm:set:dep")],
            [InlineKeyboardButton(text="✏️ Изменить Web-app URL", callback_data="adm:set:miniapp")],
            [InlineKeyboardButton(text="✏️ Изменить ссылку канала", callback_data="adm:set:channel")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def kb_content_lang():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🇷🇺 RU", callback_data="adm:cl:ru"),
             InlineKeyboardButton(text="🇬🇧 EN", callback_data="adm:cl:en")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def kb_content_keys(locale: str):
    rows = [[InlineKeyboardButton(text=f"• {key_title(k, locale)}", callback_data=f"adm:ck:{k}:{locale}")]
            for k, _ in KEYS]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:content")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_content_edit(key: str, locale: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Изменить текст", callback_data=f"adm:ce:text:{key}:{locale}")],
            [InlineKeyboardButton(text="🖼 Изменить картинку", callback_data=f"adm:ce:photo:{key}:{locale}")],
            [InlineKeyboardButton(text="🗑 Удалить картинку", callback_data=f"adm:ce:delphoto:{key}:{locale}")],
            [InlineKeyboardButton(text="🔄 Сбросить", callback_data=f"adm:ce:reset:{key}:{locale}")],
            [InlineKeyboardButton(text="👀 Предпросмотр", callback_data=f"adm:ce:preview:{key}:{locale}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:content")],
        ]
    )

def kb_params(cfg: TenantConfig):
    req_sub = bool(getattr(cfg, "require_subscription", False))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=("✅ Проверять подписку" if req_sub else "❌ Не проверять подписку"),
                    callback_data="adm:param:toggle_sub",
                )
            ],
            [
                InlineKeyboardButton(
                    text=("✅ Проверять депозит" if cfg.require_deposit else "❌ Не проверять депозит"),
                    callback_data="adm:param:toggle_dep",
                )
            ],
            [InlineKeyboardButton(text=f"💵 Минимальный депозит: ${cfg.min_deposit}",
                                  callback_data="adm:param:set_min")],
            [InlineKeyboardButton(text="↩️ Вернуть стоковую Web-app", callback_data="adm:param:stock_miniapp")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def kb_broadcast_segments():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👥 Все", callback_data="adm:bs:all"),
                InlineKeyboardButton(text="📝 Зарегистрировались", callback_data="adm:bs:registered"),
                InlineKeyboardButton(text="💰 С депозитом", callback_data="adm:bs:deposited"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def editor_status_text(db, tenant_id: int, key: str, lang: str) -> str:
    tt = db.query(TenantText).filter(
        TenantText.tenant_id == tenant_id, TenantText.locale == lang, TenantText.key == key
    ).first()
    text_len = len(tt.text) if tt and tt.text else 0
    has_img = bool(tt and tt.image_file_id)
    return (
        f"Редактирование: <b>{key_title(key, lang)}</b> ({lang})\n"
        f"Текст: {text_len} символ(ов)\n"
        f"Картинка: {'есть' if has_img else 'нет'}"
    )

# ---------------------------- ЗАПУСК ДЕТСКОГО БОТА ----------------------------
async def run_child_bot(tenant: Tenant):
    bot = Bot(token=tenant.child_bot_token, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=MemoryStorage())

    # Проверим токен
    try:
        me = await bot.get_me()
        print(f"[child] bot online: @{me.username} (tenant_id={tenant.id})")
    except Exception as e:
        print(f"[child] INVALID TOKEN for tenant_id={tenant.id}: {e!r}")
        return

    r = Router()
    r.message.outer_middleware(TenantGate(tenant.id))
    r.callback_query.outer_middleware(TenantGate(tenant.id))

    # -------- PUBLIC --------
    @r.message(Command("start"))
    async def on_start(msg: Message):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == msg.from_user.id
            ).first()
            if not user:
                user = User(tenant_id=tenant.id, tg_user_id=msg.from_user.id)
                db.add(user)
                try:
                    db.commit()
                except Exception:
                    db.rollback()
                    user = db.query(User).filter(
                        User.tenant_id == tenant.id,
                        User.tg_user_id == msg.from_user.id
                    ).first()

            if user.lang:
                await recompute_and_route(bot, tenant, user)
                return

            await render_lang_screen(bot, tenant, user, current_lang=None)
        finally:
            db.close()

    # --- выбор языка (после клика на RU/EN)
    @r.callback_query(F.data.startswith("lang:"))
    async def on_lang_select(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == cb.from_user.id
            ).first()
            if not user:
                await cb.answer()
                return

            locale = (cb.data or "").split(":")[1]
            if locale not in ("ru", "en"):
                await cb.answer()
                return

            user.lang = locale
            db.commit()

            await render_main(bot, tenant, user)
            await cb.answer("Язык сохранён")
        finally:
            db.close()

    @r.callback_query(F.data == "menu:main")
    async def on_main(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == cb.from_user.id
            ).first()
            if not user:
                await cb.answer()
                return
            await render_main(bot, tenant, user)
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:guide")
    async def on_guide(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
            if not user:
                return
            await render_guide(bot, tenant, user)
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:lang")
    async def on_menu_lang(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
            if not user:
                return

            # возьмём свежий btn_main
            locale = (user.lang or "ru")
            btn_main = tget_label(db, tenant.id, "btn_main", locale)
            await render_lang_screen(bot, tenant, user, user.lang)
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:subcheck")
    async def on_subcheck(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == cb.from_user.id
            ).first()
            if not user:
                await cb.answer()
                return
            await recompute_and_route(bot, tenant, user)
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:get")
    async def on_get(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == cb.from_user.id
            ).first()
            if not user:
                await cb.answer()
                return
            await recompute_and_route(bot, tenant, user)
            await cb.answer()
        finally:
            db.close()

    # -------- ADMIN --------
    def owner_only(uid: int) -> bool:
        return uid == tenant.owner_tg_id  # только владелец этого тенанта

    @r.message(Command("admin"))
    async def admin_entry(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id):
            await msg.answer("⛔️ Нет доступа (вы не владелец этого бота)")
            return
        await state.clear()
        await msg.answer("<b>Админ-панель</b>", reply_markup=kb_admin_main())

    @r.callback_query(
        lambda c: (
                c.data in {"adm:menu", "adm:links", "adm:pb", "adm:content", "adm:broadcast", "adm:stats", "adm:params",
                           "adm:vip"}
                or (c.data or "").startswith("adm:set:")
                or ((c.data or "").startswith("adm:vip:") and not (c.data or "").startswith("adm:vip:do:"))
                or (c.data or "").startswith("adm:bs:")
                or (c.data or "").startswith("adm:ce:")
                or (c.data or "").startswith("adm:ck:")
                or (c.data or "").startswith("adm:cl:")
                or (c.data or "").startswith("adm:param:")
                or (c.data or "").startswith("adm:bc:")
        )
    )
    async def admin_router(cb: CallbackQuery, state: FSMContext):
        if not owner_only(cb.from_user.id):
            await cb.answer()
            return

        data = cb.data or ""
        # ----- меню верхнего уровня
        if data == "adm:menu":
            await state.clear()
            await cb.message.edit_text("<b>Админ-панель</b>", reply_markup=kb_admin_main())
            await cb.answer(); return

        if data == "adm:links":
            await state.clear()
            await cb.message.edit_text("🔗 Ссылки", reply_markup=kb_admin_links())
            await cb.answer(); return

        if data == "adm:content":
            await state.clear()
            await cb.message.edit_text("🧩 Контент: выберите язык", reply_markup=kb_content_lang())
            await cb.answer(); return

        if data == "adm:params":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
            await cb.answer(); return

        if data == "adm:stats":
            db = SessionLocal()
            try:
                total = db.query(User).filter(User.tenant_id == tenant.id).count()
                reg = db.query(User).filter(User.tenant_id == tenant.id, User.step >= UserStep.registered).count()
                dep = db.query(User).filter(User.tenant_id == tenant.id, User.step == UserStep.deposited).count()
            finally:
                db.close()
            await cb.message.edit_text(
                f"👥 Всего: {total}\n📝 Зарегистрировались: {reg}\n💰 С депозитом: {dep}",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")]]
                ),
            )
            await cb.answer(); return

        if data == "adm:pb":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            secret = tenant.postback_secret or settings.global_postback_secret
            base = settings.service_host
            reg = f"{base}/pb?tenant_id={tenant.id}&event=registration&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}"
            txt = (
                "<b>Постбэки Pocket Option</b>\n\n"
                "📝 <b>Регистрация</b>\n"
                f"<code>{reg}</code>\n"
                "Макросы в PP (1-в-1):\n"
                "• click_id → <code>click_id</code>\n"
                "• trader_id → <code>trader_id</code>\n\n"
            )
            if cfg.require_deposit:
                dep = f"{base}/pb?tenant_id={tenant.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"
                txt += (
                    "💳 <b>Депозит</b>\n"
                    f"<code>{dep}</code>\n"
                    "Макросы в PP (1-в-1):\n"
                    "• click_id → <code>click_id</code>\n"
                    "• trader_id → <code>trader_id</code>\n"
                    "• sumdep → <code>sum</code>\n\n"
                    f"⚠️ Минимальный депозит: ${cfg.min_deposit}."
                )
            else:
                txt += "ℹ️ Для этого бота проверка депозита отключена."

            await cb.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")]]
                ),
                disable_web_page_preview=True,
            )
            await cb.answer(); return

        # ----- LINKS input
        if data == "adm:set:support":
            await state.set_state(AdminForm.waiting_support)
            await cb.message.edit_text("Пришлите <b>новый Support URL</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if data == "adm:set:ref":
            await state.set_state(AdminForm.waiting_ref)
            await cb.message.edit_text(
                "Пришлите <b>новую реферальную ссылку</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if data == "adm:set:dep":
            await state.set_state(AdminForm.waiting_dep)
            await cb.message.edit_text("Пришлите <b>ссылку для депозита</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if data == "adm:set:miniapp":
            await state.set_state(AdminForm.waiting_miniapp)
            await cb.message.edit_text(
                "Пришлите <b>Web-app URL</b> одним сообщением.\n"
                "Совет: выложите мини-апп на GitHub Pages и пришлите HTTPS ссылку.\n\n⬅️ /admin — отмена."
            )
            await cb.answer(); return

        if data == "adm:set:channel":
            await state.set_state(AdminForm.waiting_channel)
            await cb.message.edit_text(
                "Пришлите ссылку на канал/группу (@username или -100..., или https://t.me/username).\n\n"
                "⚠️ Бот должен быть участником (в канале — админ)."
            )
            await cb.answer(); return

        # ----- PARAMS toggles
        if data == "adm:param:toggle_dep":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                cfg.require_deposit = not cfg.require_deposit
                db.commit()
                await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
                await cb.answer("Сохранено")
            finally:
                db.close()
            return

        if data == "adm:param:toggle_sub":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                cfg.require_subscription = not bool(getattr(cfg, "require_subscription", False))
                db.commit()
                await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
                await cb.answer("Сохранено")
            finally:
                db.close()
            return

        if data == "adm:param:set_min":
            await state.set_state(AdminForm.params_wait_min_dep)
            await cb.message.edit_text("Введи минимальную сумму депозита в $ (целое число).")
            await cb.answer(); return

        if data == "adm:param:stock_miniapp":
            db = SessionLocal()
            try:
                t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
                t.miniapp_url = None
                db.commit()
            finally:
                db.close()
            await cb.message.edit_text("✅ Вернул стоковую мини-апп (из ENV).", reply_markup=kb_admin_main())
            await cb.answer(); return

        # ----- CONTENT flow
        if data.startswith("adm:cl:"):
            lang = data.split(":")[2]
            await state.update_data(content_lang=lang)
            await state.set_state(AdminForm.content_wait_key)
            await cb.message.edit_text("🧩 Контент: выберите экран", reply_markup=kb_content_keys(lang))
            await cb.answer(); return

        if data.startswith("adm:ck:"):
            _, _, key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=key)
            db = SessionLocal()
            try:
                summary = editor_status_text(db, tenant.id, key, lang)
            finally:
                db.close()
            await cb.message.edit_text(summary, reply_markup=kb_content_edit(key, lang))
            await cb.answer(); return

        if data.startswith("adm:ce:text:"):
            _, _, _, key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=key)
            await state.set_state(AdminForm.content_wait_text)
            await cb.message.edit_text(
                f"Пришлите <b>новый текст</b> для «{key_title(key, lang)}» ({lang}) одним сообщением.")
            await cb.answer(); return

        if data.startswith("adm:ce:photo:"):
            _, _, _, key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=key)
            await state.set_state(AdminForm.content_wait_photo)
            await cb.message.edit_text(f"Пришлите <b>фото</b> для «{key_title(key, lang)}» ({lang}).")
            await cb.answer(); return

        if data.startswith("adm:ce:delphoto:"):
            _, _, _, key, lang = data.split(":")
            db = SessionLocal()
            try:
                tt = db.query(TenantText).filter(
                    TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key
                ).first()
                if tt and tt.image_file_id:
                    tt.image_file_id = None
                    db.commit()
                    msg = f"🗑 Картинка удалена для «{key_title(key, lang)}» ({lang})."
                else:
                    msg = f"Картинки не было для «{key_title(key, lang)}» ({lang})."
            finally:
                db.close()
            await cb.message.edit_text(msg, reply_markup=kb_content_edit(key, lang))
            await cb.answer(); return

        if data.startswith("adm:ce:reset:"):
            _, _, _, key, lang = data.split(":")
            db = SessionLocal()
            try:
                tt = db.query(TenantText).filter(
                    TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key
                ).first()
                if tt:
                    db.delete(tt)
                    db.commit()
            finally:
                db.close()
            await cb.message.edit_text(
                f"🔄 Сброшено к дефолту для «{key_title(key, lang)}» ({lang}).",
                reply_markup=kb_content_edit(key, lang)
            )
            await cb.answer(); return

        if data.startswith("adm:ce:preview:"):
            _, _, _, key, lang = data.split(":")
            db = SessionLocal()
            try:
                text, img = tget(db, tenant.id, key, lang, default_text(key, lang))
                cfg = get_cfg(db, tenant.id)
                if key == "step2":
                    text = text.replace("{{min_dep}}", str(cfg.min_deposit))
            finally:
                db.close()
            if img:
                await cb.message.answer_photo(img, caption=f"<b>Предпросмотр ({lang} / {key})</b>\n{text}")
            else:
                await cb.message.answer(f"<b>Предпросмотр ({lang} / {key})</b>\n{text}")
            await cb.answer(); return

        # ----- VIP menu
        if data == "adm:vip":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                thr = int(cfg.vip_threshold or 500)
            finally:
                db.close()
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=f"📋 Список кандидатов (≥ ${thr})", callback_data="adm:vip:list")],
                    [InlineKeyboardButton(text="🧾 Постбэк: Регистрация", callback_data="adm:vip:reg")],
                    [InlineKeyboardButton(text="💳 Постбэк: Депозит", callback_data="adm:vip:dep")],
                    [InlineKeyboardButton(text="✅ Выдать PLATINUM доступ", callback_data="adm:vip:grant")],
                    [InlineKeyboardButton(text="🛠 Изменить мини-апп (для имеющих доступ)",
                                          callback_data="adm:vip:miniapp")],
                    [InlineKeyboardButton(text="🎯 Задать порог PLATINUM", callback_data="adm:vip:thr")],
                    [InlineKeyboardButton(text="🆔 Управление по TG ID", callback_data="adm:vip:byid")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
                ]
            )
            await state.clear()
            await cb.message.edit_text("👑 PLATINUM — выберите действие", reply_markup=kb)
            await cb.answer(); return

        if data == "adm:vip:thr":
            await state.set_state(AdminForm.vip_wait_threshold)
            await cb.message.edit_text("Пришлите новое значение порога PLATINUM (целое число, $).")
            await cb.answer(); return

        if data == "adm:vip:list":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                thr = int(cfg.vip_threshold or 500)
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users:
                    total = get_deposit_total(db, tenant.id, u)
                    if total >= thr:
                        rows.append((u.tg_user_id, total, "✅" if u.is_vip else "❌"))
                rows.sort(key=lambda x: -x[1])
                txt = f"<b>Кандидаты PLATINUM (≥ ${thr}):</b>\n\n"
                if not rows:
                    txt += "Пока пусто."
                else:
                    for tg_id, total, flag in rows[:50]:
                        txt += f"{flag} <code>{tg_id}</code> — ${total}\n"
            finally:
                db.close()
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")]])
            await cb.message.edit_text(txt, reply_markup=kb, disable_web_page_preview=True)
            await cb.answer(); return

        if data == "adm:vip:reg":
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append(
                        [InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:do:reg:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для РЕГИСТРАЦИИ (ручной постбэк):", reply_markup=kb)
            await cb.answer(); return

        if data == "adm:vip:dep":
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append(
                        [InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:do:dep:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для ДЕПОЗИТА (ручной постбэк):", reply_markup=kb)
            await cb.answer(); return

        if data == "adm:vip:grant":
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append(
                        [InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:set:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для ВЫДАЧИ PLATINUM:", reply_markup=kb)
            await cb.answer(); return

        if data == "adm:broadcast":
            await state.set_state(AdminForm.bcast_wait_segment)
            await cb.message.edit_text("📣 Рассылка: выберите сегмент", reply_markup=kb_broadcast_segments())
            await cb.answer(); return

        # ----- VIP per-user miniapp settings
        if data.startswith("adm:vip:miniapp"):
            if data == "adm:vip:miniapp":
                db = SessionLocal()
                try:
                    cfg = get_cfg(db, tenant.id)
                    thr = int(cfg.vip_threshold or 500)
                    users = db.query(User).filter(User.tenant_id == tenant.id).all()
                    rows = []
                    for u in users:
                        total = get_deposit_total(db, tenant.id, u)
                        if u.is_vip or total >= thr:
                            label = f"{u.tg_user_id} ({'VIP' if u.is_vip else f'${total}'})"
                            rows.append(
                                [InlineKeyboardButton(text=label, callback_data=f"adm:vip:miniapp:set:{u.tg_user_id}")])
                    rows = rows[:50] if rows else [
                        [InlineKeyboardButton(text="Пока нет пользователей с доступом", callback_data="adm:vip")]]
                    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                    kb = InlineKeyboardMarkup(inline_keyboard=rows)
                finally:
                    db.close()
                await cb.message.edit_text("Выберите пользователя для изменения PLATINUM мини-аппы:", reply_markup=kb)
                await cb.answer(); return

            if data.startswith("adm:vip:miniapp:set:"):
                uid = int(data.rsplit(":", 1)[-1])
                db = SessionLocal()
                try:
                    u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                    if not u:
                        await cb.answer("Юзер не найден"); return
                    has_vip = bool(u.is_vip)
                    has_custom = bool(u.vip_miniapp_url)
                finally:
                    db.close()
                rows = [
                    [InlineKeyboardButton(text="🟣 Выдать PLATINUM-мини-апп (ENV)",
                                          callback_data=f"adm:vip:miniapp:env:{uid}")],
                    [InlineKeyboardButton(text="✏️ Задать кастомный PLATINUM URL",
                                          callback_data=f"adm:vip:miniapp:ask:{uid}")],
                    [InlineKeyboardButton(text="↩️ Вернуть стоковую мини-апп",
                                          callback_data=f"adm:vip:miniapp:stock:{uid}")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip:miniapp")],
                ]
                status = []
                if has_vip: status.append("VIP=✅")
                if has_custom: status.append("Custom URL=✅")
                if not status: status.append("обычная мини-апп")
                title = f"Пользователь <code>{uid}</code>\nТекущее: " + ", ".join(status)
                await cb.message.edit_text(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
                                           disable_web_page_preview=True)
                await cb.answer(); return

            if data.startswith("adm:vip:miniapp:env:"):
                uid = int(data.rsplit(":", 1)[-1])
                db = SessionLocal()
                try:
                    u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                    if not u:
                        await cb.answer("Юзер не найден"); return
                    u.is_vip = True
                    u.vip_miniapp_url = None
                    db.commit()
                    try:
                        await render_main(bot, tenant, u)
                    except Exception as e:
                        print(f"[vip env render_main] {e}")
                    try:
                        locale = u.lang or (get_fresh_tenant(db, tenant.id) or tenant).lang_default or "ru"
                        m = "🎉 Вам выдан доступ к премиум-боту!" if locale == "ru" else "🎉 You’ve been granted access to the premium bot!"
                        kb_support = None
                        fresh_tenant = get_fresh_tenant(db, tenant.id) or tenant
                        if fresh_tenant.support_url:
                            kb_support = InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text=("🆘 Поддержка" if locale == "ru" else "🆘 Support"),
                                                      url=fresh_tenant.support_url)]
                            ])
                        await bot.send_message(uid, m, reply_markup=kb_support)
                    except Exception as e:
                        print(f"[vip env notify] {e}")
                finally:
                    db.close()
                await cb.message.edit_text(
                    "✅ Назначена PLATINUM-мини-апп из ENV. Пользователь уже видит её в «Получить сигнал».",
                    reply_markup=kb_admin_main()
                )
                await cb.answer("Готово"); return

            if data.startswith("adm:vip:miniapp:ask:"):
                uid = int(data.rsplit(":", 1)[-1])
                await state.update_data(vip_user_id=uid)
                await state.set_state(AdminForm.vip_wait_miniapp_url)
                await cb.message.edit_text(
                    f"Пришлите PLATINUM Web-app URL для <code>{uid}</code> одним сообщением.\n"
                    f"Чтобы очистить, пришлите «-».")
                await cb.answer(); return

            if data.startswith("adm:vip:miniapp:stock:"):
                uid = int(data.rsplit(":", 1)[-1])
                db = SessionLocal()
                try:
                    u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                    if not u:
                        await cb.answer("Юзер не найден"); return
                    u.vip_miniapp_url = None
                    u.is_vip = False
                    db.commit()
                    try:
                        await render_main(bot, tenant, u)
                    except Exception as e:
                        print(f"[vip stock render_main] {e}")
                finally:
                    db.close()
                await cb.message.edit_text(
                    "↩️ Вернул обычную мини-апп. Теперь «Получить сигнал» открывает не-PLATINUM версию.",
                    reply_markup=kb_admin_main()
                )
                await cb.answer("Готово"); return

        # ----- VIP by id simple
        if data == "adm:vip:byid":
            await state.set_state(AdminForm.vip_wait_user_id)
            await cb.message.edit_text("Пришлите TG ID пользователя.")
            await cb.answer(); return

        if data.startswith("adm:vip:set:"):
            uid = int(data.rsplit(":", 1)[-1])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); return
                u.is_vip = True
                u.vip_notified = True
                db.commit()
                try:
                    await render_main(bot, tenant, u)
                except Exception as e:
                    print(f"[vip set render_main] {e}")
            finally:
                db.close()
            try:
                # пуш с текущим Support URL
                db = SessionLocal()
                fresh_tenant = get_fresh_tenant(db, tenant.id) or tenant
                db.close()
                locale = u.lang or fresh_tenant.lang_default or "ru"
                text = ("🎉 Вам выдан доступ к премиум-боту! Напишите в поддержку для подключения."
                        if locale == "ru" else
                        "🎉 You’ve been granted access to the premium bot! Contact support to get connected.")
                kb = None
                if fresh_tenant.support_url:
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=("🆘 Поддержка" if locale == "ru" else "🆘 Support"),
                                              url=fresh_tenant.support_url)]
                    ])
                await bot.send_message(uid, text, reply_markup=kb)
            except Exception:
                pass
            await cb.answer("PLATINUM включён"); return

        if data.startswith("adm:vip:unset:"):
            uid = int(data.rsplit(":", 1)[-1])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); return
                u.is_vip = False
                db.commit()
                try:
                    await render_main(bot, tenant, u)
                except Exception as e:
                    print(f"[vip unset render_main] {e}")
            finally:
                db.close()
            await cb.answer("PLATINUM выключен"); return

        if data.startswith("adm:vip:url:ask:"):
            uid = int(data.rsplit(":", 1)[-1])
            await state.update_data(vip_user_id=uid)
            await state.set_state(AdminForm.vip_wait_url)
            await cb.message.edit_text(f"Пришлите PLATINUM Web-app URL для <code>{uid}</code> одним сообщением.")
            await cb.answer(); return

        if data.startswith("adm:vip:url:clear:"):
            uid = int(data.rsplit(":", 1)[-1])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); return
                u.vip_miniapp_url = None
                db.commit()
                try:
                    await render_main(bot, tenant, u)
                except Exception as e:
                    print(f"[vip url clear render_main] {e}")
            finally:
                db.close()
            await cb.answer("URL очищен"); return

        # ----- Рассылка: выбор сегмента → ввод контента
        if data.startswith("adm:bs:"):
            seg = data.split(":")[2]
            if seg not in {"all", "registered", "deposited"}:
                seg = "all"
            await state.update_data(bcast_segment=seg)
            await state.set_state(AdminForm.bcast_wait_content)
            await cb.message.edit_text("📣 Рассылка: выберите сегмент и пришлите контент.\nЗатем нажмите «Запустить».")
            await cb.answer(); return

        # ----- Запуск рассылки
        if data == "adm:bc:run":
            data_state = await state.get_data()
            seg = data_state.get("bcast_segment", "all")
            text = data_state.get("bcast_text") or ""
            media_id = data_state.get("bcast_media")
            await cb.message.edit_text(
                "📣 Рассылка поставлена в очередь. Отправка будет дозировано (≤ rate/час).", reply_markup=kb_admin_main()
            )
            await state.clear()

            async def _run_broadcast(seg: str, text: str, media_id: Optional[str]):
                db = SessionLocal()
                try:
                    q = db.query(User).filter(User.tenant_id == tenant.id)
                    if seg == "registered":
                        q = q.filter(User.step >= UserStep.registered)
                    elif seg == "deposited":
                        q = q.filter(User.step == UserStep.deposited)
                    users = [u.tg_user_id for u in q.all() if u.tg_user_id]
                finally:
                    db.close()

                rate = max(1, int(getattr(settings, "broadcast_rate_per_hour", 40) or 40))
                interval = max(90, int(3600 / rate))

                sent = 0
                failed = 0
                for uid in users:
                    try:
                        if media_id:
                            await bot.send_photo(uid, media_id, caption=text or "")
                        else:
                            await bot.send_message(uid, text or "")
                        sent += 1
                    except Exception:
                        failed += 1
                    await asyncio.sleep(interval)

                try:
                    await bot.send_message(tenant.owner_tg_id,
                                           f"📣 Рассылка завершена. Отправлено: {sent}, ошибок: {failed}.")
                except Exception:
                    pass

            asyncio.create_task(_run_broadcast(seg, text, media_id))
            await cb.answer(); return

    # ---- end admin_router

    # ---- Admin: LINK inputs
    @r.message(AdminForm.waiting_support)
    async def on_support_input(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.support_url = url
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Support URL обновлён.", reply_markup=kb_admin_main())

    @r.message(AdminForm.waiting_miniapp)
    async def on_miniapp_input(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.miniapp_url = url
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Web-app URL обновлён. Кнопка «Получить сигнал» теперь открывает новую мини-аппу.",
                         reply_markup=kb_admin_main())

    @r.message(AdminForm.waiting_ref)
    async def on_ref_input(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        ref = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.ref_link = ref
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Реферальная ссылка обновлена.", reply_markup=kb_admin_main())

    @r.message(AdminForm.waiting_dep)
    async def on_dep_input(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        dep = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.deposit_link = dep
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Ссылка для депозита обновлена.", reply_markup=kb_admin_main())

    @r.message(AdminForm.waiting_channel)
    async def on_channel_input(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.channel_url = url
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Ссылка канала/чата сохранена. Добавьте бота в чат (в канале — админ).",
                         reply_markup=kb_admin_main())

    # ---- Admin: Content inputs
    @r.message(AdminForm.content_wait_text)
    async def on_content_text(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        data = await state.get_data()
        lang = data["content_lang"]
        key = data["content_key"]
        db = SessionLocal()
        try:
            tt = db.query(TenantText).filter(
                TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key
            ).first()
            if not tt:
                tt = TenantText(tenant_id=tenant.id, locale=lang, key=key, text=msg.text or "")
                db.add(tt)
            else:
                tt.text = msg.text or ""
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer(f"✅ Текст сохранён для «{key_title(key, lang)}» ({lang}).",
                         reply_markup=kb_content_edit(key, lang))

    @r.message(AdminForm.content_wait_photo)
    async def on_content_photo(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        if not msg.photo:
            await msg.answer("Нужно прислать именно фото.")
            return
        file_id = msg.photo[-1].file_id
        data = await state.get_data()
        lang = data["content_lang"]
        key = data["content_key"]
        db = SessionLocal()
        try:
            tt = db.query(TenantText).filter(
                TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key
            ).first()
            if not tt:
                tt = TenantText(tenant_id=tenant.id, locale=lang, key=key, image_file_id=file_id)
                db.add(tt)
            else:
                tt.image_file_id = file_id
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer(f"✅ Картинка сохранена для «{key_title(key, lang)}» ({lang}).",
                         reply_markup=kb_content_edit(key, lang))

    # ---- Admin: VIP inputs
    @r.message(AdminForm.vip_wait_threshold)
    async def vip_set_threshold(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        try:
            val = int((msg.text or "").strip())
            if val < 1: raise ValueError
        except Exception:
            await msg.answer("Нужно целое число ≥ 1. Попробуйте ещё раз.")
            return
        db = SessionLocal()
        try:
            cfg = get_cfg(db, tenant.id)
            cfg.vip_threshold = val
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer(f"✅ Порог PLATINUM обновлён: ${val}.", reply_markup=kb_admin_main())

    @r.message(AdminForm.vip_wait_user_id)
    async def vip_receive_user_id(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id): return
        try:
            uid = int((msg.text or "").strip())
        except Exception:
            await msg.answer("Нужно число (TG ID). Попробуйте ещё раз.")
            return
        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await state.clear()
                await msg.answer("Юзер не найден.", reply_markup=kb_admin_main())
                return
            total = get_deposit_total(db, tenant.id, u)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Включить PLATINUM", callback_data=f"adm:vip:set:{uid}"),
                 InlineKeyboardButton(text="❌ Выключить PLATINUM", callback_data=f"adm:vip:unset:{uid}")],
                [InlineKeyboardButton(text="✏️ Задать PLATINUM URL", callback_data=f"adm:vip:url:ask:{uid}")],
                [InlineKeyboardButton(text="🗑 Очистить URL", callback_data=f"adm:vip:url:clear:{uid}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")],
            ])
            txt = (
                f"<b>Пользователь</b> <code>{uid}</code>\n"
                f"PLATINUM: {'✅' if u.is_vip else '❌'}\n"
                f"PLATINUM URL: {u.vip_miniapp_url or '—'}\n"
                f"Сумма депозитов: ${total}"
            )
            await msg.answer(txt, reply_markup=kb, disable_web_page_preview=True)
        finally:
            db.close()

    @r.callback_query(F.data.startswith("adm:vip:do:reg:"))
    async def adm_vip_do_reg(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer(); return

        uid = int(cb.data.split(":")[-1])

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден"); return

            pb = Postback(
                tenant_id=tenant.id,
                event="registration",
                click_id=str(uid),
                trader_id="manual",
                sum=0,
                token_ok=True,
            )
            db.add(pb)

            u.step = UserStep.registered
            db.commit()

            try:
                await render_get(bot, tenant, u)
            except Exception:
                pass

            await cb.answer("Ок: регистрация проставлена")
            await cb.message.edit_text("✅ Ручной постбэк «Регистрация» установлен.", reply_markup=kb_admin_main())
        finally:
            db.close()

    @r.callback_query(F.data.startswith("adm:vip:do:dep:"))
    async def adm_vip_do_dep(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer()
            return

        uid = int(cb.data.split(":")[-1])

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден")
                return

            cfg = get_cfg(db, tenant.id)
            sum_value = int(cfg.min_deposit or 50)

            pb = Postback(
                tenant_id=tenant.id,
                event="deposit",
                click_id=str(uid),
                trader_id="manual",
                sum=sum_value,
                token_ok=True,
            )
            db.add(pb)

            u.step = UserStep.deposited
            db.commit()

            try:
                await render_get(bot, tenant, u, force_unlocked=True)
            except Exception:
                pass

            await cb.answer("Ок: депозит проставлен")
            await cb.message.edit_text("✅ Ручной постбэк «Депозит» установлен.", reply_markup=kb_admin_main())
        finally:
            db.close()

    @r.message(AdminForm.vip_wait_url)
    async def vip_set_url(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id):
            return
        data = await state.get_data()
        uid = data.get("vip_user_id")
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await state.clear()
                await msg.answer("Юзер не найден.", reply_markup=kb_admin_main())
                return
            u.vip_miniapp_url = url
            db.commit()
            with contextlib.suppress(Exception):
                await render_main(bot, tenant, u)
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ PLATINUM URL сохранён.", reply_markup=kb_admin_main())

    @r.message(AdminForm.vip_wait_miniapp_url)
    async def vip_set_miniapp_from_menu(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id):
            return
        data = await state.get_data()
        uid = data.get("vip_user_id")
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await state.clear()
                await msg.answer("Юзер не найден.", reply_markup=kb_admin_main())
                return
            if url == "-":
                u.vip_miniapp_url = None
            else:
                u.vip_miniapp_url = url
            db.commit()
            with contextlib.suppress(Exception):
                await render_main(bot, tenant, u)
        finally:
            db.close()
        await state.clear()
        await msg.answer(
            "✅ Мини-апп для пользователя обновлена. Напишите ему в ЛС, чтобы он нажал /start.",
            reply_markup=kb_admin_main()
        )

    @r.message(AdminForm.bcast_wait_content)
    async def bcast_collect(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id):
            return
        data = await state.get_data()
        seg = data["bcast_segment"]
        text = msg.caption if msg.photo else msg.text
        media_id = msg.photo[-1].file_id if msg.photo else None
        await state.update_data(bcast_text=text, bcast_media=media_id)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Запустить", callback_data="adm:bc:run")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:menu")],
            ]
        )
        if media_id:
            await msg.answer_photo(media_id, caption="<b>Предпросмотр рассылки</b>\n" + (text or ""), reply_markup=kb)
        else:
            await msg.answer("<b>Предпросмотр рассылки</b>\n" + (text or ""), reply_markup=kb)
        await state.set_state(AdminForm.bcast_confirm)

    @r.message(AdminForm.params_wait_min_dep)
    async def param_set_min_value(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id):
            return
        try:
            val = int((msg.text or "").strip())
            if val < 1:
                raise ValueError
        except Exception:
            await msg.answer("Нужно ввести целое число ≥ 1. Попробуй ещё раз.")
            return
        db = SessionLocal()
        try:
            cfg = get_cfg(db, tenant.id)
            cfg.min_deposit = val
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Минимальный депозит обновлён.", reply_markup=kb_admin_main())

    @r.callback_query(F.data == "prog:dep")
    async def refresh_progress(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == cb.from_user.id
            ).first()
            if not user:
                await cb.answer()
                return
            await recompute_and_route(bot, tenant, user)
            await cb.answer("Обновлено")
        finally:
            db.close()

    # подключаем роутер и запускаем поллинг
    dp.include_router(r)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[child] start_polling crashed for tenant_id={tenant.id}: {e!r}")
    finally:
        with contextlib.suppress(Exception):
            await bot.session.close()

# ---------------------- ПРОГРЕСС (централизованный роутер) ----------------------
async def recompute_and_route(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        # свежие данные для подписки/мини-аппы и др.
        tenant = get_fresh_tenant(db, tenant.id) or tenant
        cfg = get_cfg(db, tenant.id)
        locale = user.lang or tenant.lang_default or "ru"

        # 1) Подписка (если включена и канал задан)
        if getattr(cfg, "require_subscription", False):
            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                await render_subscribe(bot, tenant, user)
                db.commit()
                return

        # 2) Регистрация / Депозит
        if user.step in (UserStep.new, UserStep.asked_reg):
            await render_get(bot, tenant, user)
            db.commit()
            return

        if cfg.require_deposit and user.step != UserStep.deposited:
            await render_get(bot, tenant, user)
            db.commit()
            return

        # 3) Доступ открыт: показать «unlocked» ровно один раз
        if not getattr(user, "access_notified", False):
            await render_get(bot, tenant, user, force_unlocked=True)
            # render_get сам проставит access_notified = True и сделает commit
            return

        # 4) Обычное главное меню
        await render_main(bot, tenant, user)
        db.commit()
    finally:
        db.close()



