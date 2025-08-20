import asyncio
from typing import Optional, List

from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, FSInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

from sqlalchemy import func, or_

from app.models import Tenant, User, UserStep, TenantText, TenantConfig, Postback, TenantStatus
from app.db import SessionLocal
from app.settings import settings
from app.utils.common import safe_delete_message

from pathlib import Path

# ---------------------- ЭКРАНЫ / КЛЮЧИ ----------------------
KEYS = [
    ("lang",      {"ru": "Выбор языка",        "en": "Language"}),
    ("main",      {"ru": "Главное меню",       "en": "Main menu"}),
    ("guide",     {"ru": "Инструкция",         "en": "Instruction"}),
    ("subscribe", {"ru": "Подписка на канал",  "en": "Subscribe"}),
    ("step1",     {"ru": "Шаг 1. Регистрация", "en": "Step 1. Registration"}),
    ("step2",     {"ru": "Шаг 2. Депозит",     "en": "Step 2. Deposit"}),
    ("unlocked",  {"ru": "Доступ открыт",      "en": "Access granted"}),
]

DEFAULT_TEXTS = {
    "lang": {"ru": "Выберите язык", "en": "Choose your language"},
    "main": {"ru": "Главное меню", "en": "Main menu"},
    "guide": {
        "ru": ("Ниже пошаговая инструкция по использованию бота 🧾:\n\n"
               "1. Зарегистрируйте аккаунт на PocketOption через нашего бота.\n"
               "2. Запустите бота и выберите валютную пару и время экспирации.\n"
               "3. Нажмите «Получить сигнал» и строго следуйте ему.\n"
               "4. В случае неудачного сигнала рекомендуется удвоить сумму, чтобы компенсировать убыток следующим сигналом."),
        "en": ("Step-by-step instructions for using the bot 🧾:\n\n"
               "1) Register a PocketOption account via this bot.\n"
               "2) Start the bot and choose the currency pair and expiration time.\n"
               "3) Tap “Get signal” and follow it precisely.\n"
               "4) If a signal fails, it’s recommended to double the amount to compensate on the next signal.")
    },
    "subscribe": {
        "ru": "Для начала подпишитесь на канал.\n\nПосле подписки вернитесь в бот и снова нажмите «📈 Получить сигнал».",
        "en": "First, subscribe to the channel.\n\nAfter subscribing, come back and tap “📈 Get signal”."
    },
    "step1": {
        "ru": ("⚡️Регистрация\n\n"
               "Для получения сигналов необходимо зарегистрироваться у брокера PocketOption по нашей ссылке.\n"
               "Нажмите кнопку «🟢 Зарегистрироваться», чтобы создать аккаунт.\n\n"
               "❗️После регистрации вы автоматически перейдёте на следующий шаг!"),
        "en": ("⚡️Registration\n\n"
               "To receive signals, you must register with the PocketOption broker using our link.\n"
               "Click the «🟢 Register» button to create an account.\n\n"
               "❗️After registration you will automatically proceed to the next step!")
    },
    "step2": {
        "ru": ("⚡️Внесите депозит: ${{min_dep}}.\n\n"
               "Нажмите «💳 Внести депозит», чтобы пополнить баланс на сайте брокера — это нужно, чтобы сразу начать работу."),
        "en": ("⚡️Make a deposit: ${{min_dep}}.\n\n"
               "Press «💳 Deposit» to top up the balance on the broker's website — this is required to start right away.")
    },
    "unlocked": {
        "ru": "Доступ открыт. Нажмите «Получить сигнал».",
        "en": "Access granted. Press “Get signal”.",
    },
}

def key_title(key: str, locale: str) -> str:
    for k, names in KEYS:
        if k == key:
            return names.get(locale, k)
    return key

def default_text(key: str, locale: str) -> str:
    return DEFAULT_TEXTS.get(key, {}).get(locale, key)

def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]

def _find_stock_file(key: str, locale: str) -> Optional[Path]:
    stock = _project_root() / "static" / "stock"
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = stock / f"{key}-{locale}.{ext}"
        if p.exists():
            return p
    return None

# --------------------- CONFIG/TEXTS ---------------------
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
    # страховка на случай старой БД
    if getattr(cfg, "require_subscription", None) is None:
        cfg.require_subscription = False
        db.commit()
    if getattr(cfg, "vip_threshold", None) is None:
        cfg.vip_threshold = 500
        db.commit()
    return cfg

def tget(db, tenant_id: int, key: str, locale: str, fallback_text: str):
    tt = db.query(TenantText).filter(
        TenantText.tenant_id == tenant_id,
        TenantText.locale == locale,
        TenantText.key == key,
    ).first()
    return (tt.text if tt and tt.text else fallback_text), (tt.image_file_id if tt else None)

def get_deposit_total(db, tenant_id: int, user: User) -> int:
    total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
        Postback.tenant_id == tenant_id,
        Postback.event == "deposit",
        Postback.click_id == str(user.tg_user_id),
        Postback.token_ok.is_(True),
    ).scalar() or 0
    return int(total)

# --------------------- HELPERS ---------------------
async def send_screen(bot, user, key: str, locale: str, text: str, kb, image_file_id: Optional[str]):
    await safe_delete_message(bot, user.tg_user_id, user.last_message_id)

    if image_file_id:
        try:
            m = await bot.send_photo(user.tg_user_id, image_file_id, caption=text, reply_markup=kb)
            user.last_message_id = m.message_id
            return
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

def kb_main(locale: str, support_url: Optional[str], tenant: Tenant, user: User):
    if user.step == UserStep.deposited:
        signal_btn = InlineKeyboardButton(
            text="📈 Get signal" if locale == "en" else "📈 Получить сигнал",
            web_app=WebAppInfo(url=tenant_miniapp_url(tenant, user)),
        )
    else:
        signal_btn = InlineKeyboardButton(
            text="📈 Get signal" if locale == "en" else "📈 Получить сигнал",
            callback_data="menu:get",
        )

    if locale == "en":
        rows = [
            [InlineKeyboardButton(text="📘 Instruction", callback_data="menu:guide")],
            [
                InlineKeyboardButton(text="🆘 Support", url=support_url or "about:blank"),
                InlineKeyboardButton(text="🌐 Change language", callback_data="menu:lang"),
            ],
            [signal_btn],
        ]
    else:
        rows = [
            [InlineKeyboardButton(text="📘 Инструкция", callback_data="menu:guide")],
            [
                InlineKeyboardButton(text="🆘 Поддержка", url=support_url or "about:blank"),
                InlineKeyboardButton(text="🌐 Сменить язык", callback_data="menu:lang"),
            ],
            [signal_btn],
        ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back(locale: str):
    txt = "🏠 Main menu" if locale == "en" else "🏠 Главное меню"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=txt, callback_data="menu:main")]])

def kb_lang(current: Optional[str]):
    ru = ("✅ " if current == "ru" else "") + "🇷🇺 Русский"
    en = ("✅ " if current == "en" else "") + "🇬🇧 English"
    rows = [
        [InlineKeyboardButton(text=ru, callback_data="lang:ru"), InlineKeyboardButton(text=en, callback_data="lang:en")],
        [InlineKeyboardButton(text=("🏠 Главное меню" if (current or "ru") == "ru" else "🏠 Main menu"), callback_data="menu:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_subscribe(locale: str, channel_url: str) -> InlineKeyboardMarkup:
    go_txt = "🚀 Перейти в канал" if locale == "ru" else "🚀 Go to channel"
    back_txt = "🏠 Главное меню" if locale == "ru" else "🏠 Main menu"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=go_txt, url=channel_url or "about:blank")],
            [InlineKeyboardButton(text=back_txt, callback_data="menu:main")],
        ]
    )

# >>> VIP/miniapp priority
def tenant_miniapp_url(tenant: Tenant, user: User) -> str:
    base = (user.vip_miniapp_url or tenant.miniapp_url or settings.miniapp_url).rstrip("/")
    return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

# ------------------------------- РЕНДЕРЫ ------------------------------
async def render_lang_screen(bot: Bot, tenant: Tenant, user: User, current_lang: Optional[str]):
    db = SessionLocal()
    try:
        locale = (current_lang or tenant.lang_default or "ru").lower()
        text, img = tget(db, tenant.id, "lang", locale, default_text("lang", locale))
        rm = kb_lang(current_lang)
        await safe_delete_message(bot, user.tg_user_id, user.last_message_id)
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
        locale = user.lang or tenant.lang_default or "ru"
        text, img = tget(db, tenant.id, "main", locale, default_text("main", locale))
        kb = kb_main(locale, tenant.support_url, tenant, user)
        await send_screen(bot, user, "main", locale, text, kb, img)
        db.commit()
    finally:
        db.close()

async def render_guide(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        locale = user.lang or tenant.lang_default or "ru"
        text, img = tget(db, tenant.id, "guide", locale, default_text("guide", locale))
        await send_screen(bot, user, "guide", locale, text, kb_back(locale), img)
        db.commit()
    finally:
        db.close()

async def render_subscribe(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        locale = user.lang or tenant.lang_default or "ru"
        text, img = tget(db, tenant.id, "subscribe", locale, default_text("subscribe", locale))
        kb = kb_subscribe(locale, tenant.channel_url or "")
        await send_screen(bot, user, "subscribe", locale, text, kb, img)
        db.commit()
    finally:
        db.close()

async def is_user_subscribed(bot: Bot, channel: str, user_id: int) -> bool:
    """
    Поддерживает:
      • @username
      • https://t.me/username
      • -100xxxxxxxxxx (chat_id приватного канала)
    """
    if not channel:
        return True
    try:
        ch = channel.strip()
        if ch.startswith("http") and "t.me/" in ch:
            ch = ch.split("t.me/")[-1]
            ch = ch.split("?")[0].lstrip("/").lstrip("+@")
            if ch and not ch.startswith("-"):
                ch = "@" + ch
        member = await bot.get_chat_member(ch, user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        return False

async def render_get(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        locale = user.lang or tenant.lang_default or "ru"
        cfg = get_cfg(db, tenant.id)

        # подписка всегда первая
        if cfg.require_subscription:
            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                await render_subscribe(bot, tenant, user)
                db.commit()
                return

        if user.step == UserStep.deposited:
            text, img = tget(db, tenant.id, "unlocked", locale, default_text("unlocked", locale))
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📈 Получить сигнал" if locale == "ru" else "📈 Get signal",
                        web_app=WebAppInfo(url=tenant_miniapp_url(tenant, user)),
                    )],
                    [InlineKeyboardButton(text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main")],
                ]
            )
            await send_screen(bot, user, "unlocked", locale, text, kb, img)
        else:
            if user.step in (UserStep.new, UserStep.asked_reg):
                text, img = tget(db, tenant.id, "step1", locale, default_text("step1", locale))
                url = f"{settings.service_host}/r/reg?tenant_id={tenant.id}&uid={user.tg_user_id}"
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🟢  Зарегистрироваться" if locale == "ru" else "🟢  Register", url=url)],
                        [InlineKeyboardButton(text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main")],
                    ]
                )
                user.step = UserStep.asked_reg
                await send_screen(bot, user, "step1", locale, text, kb, img)
            else:
                text, img = tget(db, tenant.id, "step2", locale, default_text("step2", locale))
                text = text.replace("{{min_dep}}", str(cfg.min_deposit))
                dep_total = get_deposit_total(db, tenant.id, user)
                left = max(0, cfg.min_deposit - dep_total)
                text += ("\n\n💵 Внесено: ${} / ${} (осталось ${})".format(dep_total, cfg.min_deposit, left)
                         if locale == "ru" else
                         "\n\n💵 Paid: ${} / ${} (left ${})".format(dep_total, cfg.min_deposit, left))
                url = f"{settings.service_host}/r/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Внести депозит" if locale == "ru" else "💳 Deposit", url=url)],
                        [InlineKeyboardButton(text=("🔄 Прогресс: $" if locale == "ru" else "🔄 Progress: $") + f"{dep_total}/{cfg.min_deposit}", callback_data="prog:dep")],
                        [InlineKeyboardButton(text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main")],
                    ]
                )
                user.step = UserStep.asked_deposit
                await send_screen(bot, user, "step2", locale, text, kb, img)
        db.commit()
    finally:
        db.close()

# --------------------------------- ADMIN FSM ---------------------------------
class TenantGate(BaseMiddleware):
    def __init__(self, tenant_id: int):
        super().__init__()
        self.tenant_id = tenant_id

    async def __call__(self, handler, event, data):
        try:
            # статус тенанта
            db = SessionLocal()
            try:
                t = db.query(Tenant).filter(Tenant.id == self.tenant_id).first()
                status = t.status if t else "deleted"
            finally:
                db.close()
            if status != TenantStatus.active:
                if isinstance(event, Message):
                    await event.answer("⏸ Бот на паузе / удалён.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("⏸ Бот на паузе / удалён.", show_alert=False)
                return
        except Exception:
            return
        return await handler(event, data)

class AdminForm(StatesGroup):
    waiting_support = State()
    waiting_ref = State()
    waiting_dep = State()
    waiting_miniapp = State()
    waiting_channel = State()

    content_wait_lang = State()
    content_wait_key = State()
    content_wait_text = State()
    content_wait_photo = State()

    bcast_wait_segment = State()
    bcast_wait_content = State()
    bcast_confirm = State()

    params_wait_min_dep = State()

    # VIP:
    vip_wait_threshold = State()
    vip_wait_pick_user_for_app = State()
    vip_wait_app_url = State()

def kb_admin_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Ссылки", callback_data="adm:links")],
            [InlineKeyboardButton(text="🔁 Постбэки", callback_data="adm:pb")],
            [InlineKeyboardButton(text="🧩 Контент", callback_data="adm:content")],
            [InlineKeyboardButton(text="⚙️ Параметры", callback_data="adm:params")],
            [InlineKeyboardButton(text="👑 VIP", callback_data="adm:vip")],  # >>>
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        ]
    )

def kb_admin_links():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить Support URL",     callback_data="adm:set:support")],
            [InlineKeyboardButton(text="✏️ Изменить Реф. ссылку",     callback_data="adm:set:ref")],
            [InlineKeyboardButton(text="✏️ Изменить ссылку депозита", callback_data="adm:set:dep")],
            [InlineKeyboardButton(text="✏️ Изменить Web-app URL",     callback_data="adm:set:miniapp")],
            [InlineKeyboardButton(text="✏️ Изменить ссылку канала",   callback_data="adm:set:channel")],
            [InlineKeyboardButton(text="⬅️ Назад",                    callback_data="adm:menu")],
        ]
    )

def kb_content_lang():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🇷🇺 RU", callback_data="adm:cl:ru"), InlineKeyboardButton(text="🇬🇧 EN", callback_data="adm:cl:en")],
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
            [InlineKeyboardButton(text=("✅ Проверять подписку" if req_sub else "❌ Не проверять подписку"), callback_data="adm:param:toggle_sub")],
            [InlineKeyboardButton(text=("✅ Проверять депозит" if cfg.require_deposit else "❌ Не проверять депозит"), callback_data="adm:param:toggle_dep")],
            [InlineKeyboardButton(text=f"💵 Минимальный депозит: ${cfg.min_deposit}", callback_data="adm:param:set_min")],
            [InlineKeyboardButton(text="↩️ Вернуть стоковую Web-app", callback_data="adm:param:stock_miniapp")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def kb_admin_vip(cfg: TenantConfig):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"📏 Порог VIP: ${cfg.vip_threshold}", callback_data="vip:threshold")],
            [InlineKeyboardButton(text="🛠 Изменить мини-апп", callback_data="vip:setapp")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def kb_broadcast_segments():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Все", callback_data="adm:bs:all"),
             InlineKeyboardButton(text="📝 Зарегистрировались", callback_data="adm:bs:registered"),
             InlineKeyboardButton(text="💰 С депозитом", callback_data="adm:bs:deposited")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def editor_status_text(db, tenant_id: int, key: str, lang: str) -> str:
    tt = db.query(TenantText).filter(
        TenantText.tenant_id == tenant_id, TenantText.locale == lang, TenantText.key == key
    ).first()
    text_len = len(tt.text) if tt and tt.text else 0
    has_img = bool(tt and tt.image_file_id)
    return (f"Редактирование: <b>{key_title(key, lang)}</b> ({lang})\n"
            f"Текст: {text_len} символ(ов)\n"
            f"Картинка: {'есть' if has_img else 'нет'}")

# ---------------------------- ЗАПУСК ДЕТСКОГО БОТА ----------------------------
async def run_child_bot(tenant: Tenant):
    bot = Bot(token=tenant.child_bot_token, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=MemoryStorage())
    r = Router()
    r.message.outer_middleware(TenantGate(tenant.id))
    r.callback_query.outer_middleware(TenantGate(tenant.id))

    # -------- PUBLIC --------
    @r.message(F.text == "/start")
    async def on_start(msg: Message):
        db = SessionLocal()
        try:
            # устойчивое создание (на случай гонок)
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == msg.from_user.id).first()
            if not user:
                user = User(tenant_id=tenant.id, tg_user_id=msg.from_user.id)
                db.add(user)
                try:
                    db.commit()
                except Exception:
                    db.rollback()
                    user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == msg.from_user.id).first()

            if user.lang:
                await render_main(bot, tenant, user)
                db.commit()
                return

            await render_lang_screen(bot, tenant, user, None)
        finally:
            db.close()

    @r.message(F.text == "/resetme")
    async def reset_me(msg: Message):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == msg.from_user.id).first()
            if not user:
                await msg.answer("Пользователь ещё не зарегистрирован в системе.")
                return
            user.step = UserStep.new
            user.trader_id = None
            db.commit()
        finally:
            db.close()
        await msg.answer("♻️ Твой прогресс сброшен. Нажми «📈 Получить сигнал» и пройди шаги заново.")

    @r.callback_query(lambda c: c.data and c.data.startswith("lang:"))
    async def on_lang(cb: CallbackQuery):
        lang = cb.data.split(":")[1]
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == cb.from_user.id).first()
            if not user:
                user = User(tenant_id=tenant.id, tg_user_id=cb.from_user.id, lang=lang)
                db.add(user)
                db.commit()
            else:
                user.lang = lang
                db.commit()
            await render_main(bot, tenant, user)
            db.commit()
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:main")
    async def on_main(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == cb.from_user.id).first()
            if not user:
                await cb.answer()
                return
            await render_main(bot, tenant, user)
            db.commit()
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:guide")
    async def on_guide(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == cb.from_user.id).first()
            if not user:
                return
            await render_guide(bot, tenant, user)
            db.commit()
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:lang")
    async def on_menu_lang(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == cb.from_user.id).first()
            if not user:
                return
            await render_lang_screen(bot, tenant, user, user.lang)
            db.commit()
            await cb.answer()
        finally:
            db.close()

    @r.callback_query(F.data == "menu:get")
    async def on_get(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == cb.from_user.id).first()
            if not user:
                return
            if user.step == UserStep.deposited:
                await render_main(bot, tenant, user)
                await cb.answer()
                return
            await render_get(bot, tenant, user)
            db.commit()
            await cb.answer()
        finally:
            db.close()

    # -------- SUBSCRIBE PROGRESS REFRESH --------
    @r.callback_query(F.data == "prog:dep")
    async def refresh_progress(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == cb.from_user.id).first()
            if not user:
                await cb.answer()
                return
            locale = user.lang or tenant.lang_default or "ru"
            cfg = get_cfg(db, tenant.id)

            if cfg.require_subscription:
                ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
                if not ok:
                    await render_subscribe(bot, tenant, user)
                    await cb.answer("Сначала подпишитесь" if locale == "ru" else "Please subscribe first")
                    return

            if user.step == UserStep.deposited:
                await render_main(bot, tenant, user)
                await cb.answer("Доступ уже открыт ✅" if locale == "ru" else "Access already unlocked ✅")
                return

            dep_total = get_deposit_total(db, tenant.id, user)
            left = max(0, cfg.min_deposit - dep_total)

            text, img = tget(db, tenant.id, "step2", locale, default_text("step2", locale))
            text = text.replace("{{min_dep}}", str(cfg.min_deposit))
            text += ("\n\n💵 Внесено: ${} / ${} (осталось ${})".format(dep_total, cfg.min_deposit, left)
                     if locale == "ru" else
                     "\n\n💵 Paid: ${} / ${} (left ${})".format(dep_total, cfg.min_deposit, left))

            url = f"{settings.service_host}/r/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Внести депозит" if locale == "ru" else "💳 Deposit", url=url)],
                    [InlineKeyboardButton(text=("🔄 Прогресс: $" if locale == "ru" else "🔄 Progress: $") + f"{dep_total}/{cfg.min_deposit}", callback_data="prog:dep")],
                    [InlineKeyboardButton(text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main")],
                ]
            )

            is_media = bool(cb.message.photo or cb.message.document or cb.message.video or cb.message.animation)
            try:
                if is_media:
                    await cb.message.edit_caption(text, reply_markup=kb)
                else:
                    await cb.message.edit_text(text, reply_markup=kb)
            except TelegramBadRequest as e:
                if "message is not modified" in (str(e) or "").lower():
                    await cb.answer("Без изменений" if locale == "ru" else "No changes")
                    return
                await cb.message.answer(text, reply_markup=kb)
                await cb.answer("Обновлено" if locale == "ru" else "Updated")
                return

            await cb.answer("Обновлено" if locale == "ru" else "Updated")
        finally:
            db.close()

    # -------- ADMIN --------
    def owner_only(uid: int) -> bool:
        return uid == tenant.owner_tg_id

    @r.message(F.text == "/admin")
    async def admin_entry(msg: Message, state: FSMContext):
        if not owner_only(msg.from_user.id):
            return
        await state.clear()
        await msg.answer("<b>Админ-панель</b>", reply_markup=kb_admin_main())

    @r.callback_query(
        lambda c: (
            c.data in {"adm:menu", "adm:links", "adm:pb", "adm:content", "adm:broadcast", "adm:stats", "adm:params", "adm:vip"}
            or (c.data or "").startswith("adm:set:")
        )
    )
    async def admin_router(cb: CallbackQuery, state: FSMContext):
        if not owner_only(cb.from_user.id):
            await cb.answer()
            return

        if (cb.data or "").startswith("adm:set:"):
            action = "set:" + cb.data.split(":", 2)[2]
        else:
            action = cb.data.split(":", 1)[1]

        if action == "menu":
            await state.clear()
            await cb.message.edit_text("<b>Админ-панель</b>", reply_markup=kb_admin_main())
            await cb.answer(); return

        if action == "links":
            await state.clear()
            await cb.message.edit_text("🔗 Ссылки", reply_markup=kb_admin_links())
            await cb.answer(); return

        if action == "content":
            await state.clear()
            await cb.message.edit_text("🧩 Контент: выберите язык", reply_markup=kb_content_lang())
            await cb.answer(); return

        if action == "params":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
            await cb.answer(); return

        if action == "vip":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            await cb.message.edit_text("👑 VIP", reply_markup=kb_admin_vip(cfg))
            await cb.answer(); return

        if action == "set:support":
            await state.set_state(AdminForm.waiting_support)
            await cb.message.edit_text("Пришлите <b>новый Support URL</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if action == "set:ref":
            await state.set_state(AdminForm.waiting_ref)
            await cb.message.edit_text("Пришлите <b>новую реферальную ссылку</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if action == "set:dep":
            await state.set_state(AdminForm.waiting_dep)
            await cb.message.edit_text("Пришлите <b>ссылку для депозита</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if action == "set:miniapp":
            await state.set_state(AdminForm.waiting_miniapp)
            await cb.message.edit_text("Пришлите <b>Web-app URL</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if action == "set:channel":
            await state.set_state(AdminForm.waiting_channel)
            await cb.message.edit_text(
                "Пришлите ссылку на канал (@username | https://t.me/username | -100xxxxxxxxxx).\n"
                "Если канал приватный — используйте chat_id (-100…). Бот должен быть админом."
            )
            await cb.answer(); return

        if action == "pb":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            secret = tenant.postback_secret or settings.global_postback_secret
            base = settings.service_host
            reg = f"{base}/pb?tenant_id={tenant.id}&event=registration&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}"
            txt = ("<b>Постбэки Pocket Option</b>\n\n"
                   "📝 <b>Регистрация</b>\n"
                   f"<code>{reg}</code>\n"
                   "Макросы: click_id→<code>click_id</code>, trader_id→<code>trader_id</code>\n\n")
            if cfg.require_deposit:
                dep = f"{base}/pb?tenant_id={tenant.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"
                txt += ("💳 <b>Депозит</b>\n"
                        f"<code>{dep}</code>\n"
                        "Макросы: click_id→<code>click_id</code>, trader_id→<code>trader_id</code>, sumdep→<code>sum</code>\n\n"
                        f"⚠️ Минимальный депозит: ${cfg.min_deposit}.")
            else:
                txt += "ℹ️ Проверка депозита отключена."
            await cb.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")]]),
                disable_web_page_preview=True,
            )
            await cb.answer(); return

        if action == "broadcast":
            await state.set_state(AdminForm.bcast_wait_segment)
            await cb.message.edit_text("📣 Рассылка: выберите сегмент", reply_markup=kb_broadcast_segments())
            await cb.answer(); return

        if action == "stats":
            db = SessionLocal()
            try:
                total = db.query(User).filter(User.tenant_id == tenant.id).count()
                reg = db.query(User).filter(User.tenant_id == tenant.id, User.step >= UserStep.registered).count()
                dep = db.query(User).filter(User.tenant_id == tenant.id, User.step == UserStep.deposited).count()
            finally:
                db.close()
            await cb.message.edit_text(
                f"👥 Всего: {total}\n📝 Зарегистрировались: {reg}\n✅ С доступом: {dep}\n💰 С депозитом: {dep}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")]]),
            )
            await cb.answer(); return

    # ---- Admin: ввод ссылок
    @r.message(AdminForm.waiting_support)
    async def on_support_input(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
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
        if msg.from_user.id != tenant.owner_tg_id:
            return
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.miniapp_url = url
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Web-app URL обновлён.", reply_markup=kb_admin_main())

    @r.message(AdminForm.waiting_ref)
    async def on_ref_input(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
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
        if msg.from_user.id != tenant.owner_tg_id:
            return
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
        if msg.from_user.id != tenant.owner_tg_id:
            return
        url = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.channel_url = url
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Ссылка канала обновлена.", reply_markup=kb_admin_main())

    # ---- Admin: Контент (как было) ----
    @r.callback_query(lambda c: c.data and c.data.startswith("adm:cl:"))
    async def content_choose_lang(cb: CallbackQuery, state: FSMContext):
        lang = cb.data.split(":")[2]
        await state.update_data(content_lang=lang)
        await state.set_state(AdminForm.content_wait_key)
        await cb.message.edit_text("🧩 Контент: выберите экран", reply_markup=kb_content_keys(lang))
        await cb.answer()

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ck:"))
    async def content_choose_key(cb: CallbackQuery, state: FSMContext):
        _, _, key, lang = cb.data.split(":")
        await state.update_data(content_lang=lang, content_key=key)
        db = SessionLocal()
        try:
            summary = editor_status_text(db, tenant.id, key, lang)
        finally:
            db.close()
        await cb.message.edit_text(summary, reply_markup=kb_content_edit(key, lang))
        await cb.answer()

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:text:"))
    async def content_edit_text(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
        await state.update_data(content_lang=lang, content_key=key)
        await state.set_state(AdminForm.content_wait_text)
        await cb.message.edit_text(f"Пришлите <b>новый текст</b> для «{key_title(key, lang)}» ({lang}) одним сообщением.")
        await cb.answer()

    @r.message(AdminForm.content_wait_text)
    async def on_content_text(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        data = await state.get_data()
        lang = data["content_lang"]; key = data["content_key"]
        db = SessionLocal()
        try:
            tt = db.query(TenantText).filter(TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key).first()
            if not tt:
                tt = TenantText(tenant_id=tenant.id, locale=lang, key=key, text=msg.text or "")
                db.add(tt)
            else:
                tt.text = msg.text or ""
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer(f"✅ Текст сохранён для «{key_title(key, lang)}» ({lang}).", reply_markup=kb_content_edit(key, lang))

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:photo:"))
    async def content_edit_photo(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
        await state.update_data(content_lang=lang, content_key=key)
        await state.set_state(AdminForm.content_wait_photo)
        await cb.message.edit_text(f"Пришлите <b>фото</b> для «{key_title(key, lang)}» ({lang}).")
        await cb.answer()

    @r.message(AdminForm.content_wait_photo)
    async def on_content_photo(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        if not msg.photo:
            await msg.answer("Нужно прислать именно фото.")
            return
        file_id = msg.photo[-1].file_id
        data = await state.get_data()
        lang = data["content_lang"]; key = data["content_key"]
        db = SessionLocal()
        try:
            tt = db.query(TenantText).filter(TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key).first()
            if not tt:
                tt = TenantText(tenant_id=tenant.id, locale=lang, key=key, image_file_id=file_id)
                db.add(tt)
            else:
                tt.image_file_id = file_id
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer(f"✅ Картинка сохранена для «{key_title(key, lang)}» ({lang}).", reply_markup=kb_content_edit(key, lang))

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:delphoto:"))
    async def content_delete_photo(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
        db = SessionLocal()
        try:
            tt = db.query(TenantText).filter(TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key).first()
            if tt and tt.image_file_id:
                tt.image_file_id = None
                db.commit()
                msg = f"🗑 Картинка удалена для «{key_title(key, lang)}» ({lang})."
            else:
                msg = f"Картинки не было для «{key_title(key, lang)}» ({lang})."
        finally:
            db.close()
        await cb.message.edit_text(msg, reply_markup=kb_content_edit(key, lang))
        await cb.answer()

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:reset:"))
    async def content_reset(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
        db = SessionLocal()
        try:
            tt = db.query(TenantText).filter(TenantText.tenant_id == tenant.id, TenantText.locale == lang, TenantText.key == key).first()
            if tt:
                db.delete(tt)
                db.commit()
        finally:
            db.close()
        await cb.message.edit_text(f"🔄 Сброшено к дефолту для «{key_title(key, lang)}» ({lang}).", reply_markup=kb_content_edit(key, lang))
        await cb.answer()

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:preview:"))
    async def content_preview(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
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
        await cb.answer()

    # ---- Admin: Параметры
    @r.callback_query(F.data == "adm:param:toggle_dep")
    async def param_toggle_dep(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer(); return
        db = SessionLocal()
        try:
            cfg = get_cfg(db, tenant.id)
            cfg.require_deposit = not cfg.require_deposit
            db.commit()
            await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
            await cb.answer("Сохранено")
        finally:
            db.close()

    @r.callback_query(F.data == "adm:param:toggle_sub")
    async def param_toggle_sub(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer(); return
        db = SessionLocal()
        try:
            cfg = get_cfg(db, tenant.id)
            cfg.require_subscription = not bool(getattr(cfg, "require_subscription", False))
            db.commit()
            await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
            await cb.answer("Сохранено")
        finally:
            db.close()

    @r.callback_query(F.data == "adm:param:set_min")
    async def param_set_min(cb: CallbackQuery, state: FSMContext):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer(); return
        await state.set_state(AdminForm.params_wait_min_dep)
        await cb.message.edit_text("Введи минимальную сумму депозита в $ (целое число).")
        await cb.answer()

    @r.message(AdminForm.params_wait_min_dep)
    async def param_set_min_value(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
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

    @r.callback_query(F.data == "adm:param:stock_miniapp")
    async def param_stock_miniapp(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer(); return
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.miniapp_url = None
            db.commit()
        finally:
            db.close()
        await cb.message.edit_text("✅ Вернул стоковую мини-апп (из ENV).", reply_markup=kb_admin_main())
        await cb.answer()

    # ---- VIP: порог
    @r.callback_query(F.data == "vip:threshold")
    async def vip_threshold(cb: CallbackQuery, state: FSMContext):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer(); return
        await state.set_state(AdminForm.vip_wait_threshold)
        await cb.message.edit_text("Введи новый порог VIP в $ (целое число).")
        await cb.answer()

    @r.message(AdminForm.vip_wait_threshold)
    async def vip_threshold_set(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        try:
            val = int((msg.text or "").strip())
            if val < 1:
                raise ValueError
        except Exception:
            await msg.answer("Нужно целое число ≥ 1. Попробуй ещё раз.")
            return
        db = SessionLocal()
        try:
            cfg = get_cfg(db, tenant.id)
            cfg.vip_threshold = val
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Порог VIP обновлён.", reply_markup=kb_admin_main())

    # ---- VIP: изменить мини-апп
    @r.callback_query(F.data == "vip:setapp")
    async def vip_setapp(cb: CallbackQuery, state: FSMContext):
        if cb.from_user.id != tenant.owner_tg_id:
            return
        db = SessionLocal()
        try:
            users = (db.query(User)
                     .filter(User.tenant_id == tenant.id,
                             or_(User.is_vip.is_(True), User.vip_notified.is_(True)),
                             User.tg_user_id.isnot(None))
                     .order_by(User.id.desc())
                     .limit(50).all())
        finally:
            db.close()
        if not users:
            await cb.message.edit_text(
                "Пока нет пользователей с VIP-статусом или VIP-уведомлением.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")]]),
            )
            await cb.answer(); return

        rows: List[List[InlineKeyboardButton]] = []
        row: List[InlineKeyboardButton] = []
        for u in users:
            row.append(InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"vip:pickapp:{u.tg_user_id}"))
            if len(row) == 2:
                rows.append(row); row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])

        await cb.message.edit_text(
            "Выбери пользователя (TG ID), которому изменить мини-апп:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
        await state.set_state(AdminForm.vip_wait_pick_user_for_app)
        await cb.answer()

    @r.callback_query(lambda c: (c.data or "").startswith("vip:pickapp:"))
    async def vip_pick_user_for_app(cb: CallbackQuery, state: FSMContext):
        if cb.from_user.id != tenant.owner_tg_id:
            return
        _, _, uid_str = cb.data.split(":")
        await state.update_data(vip_target_uid=uid_str)
        await state.set_state(AdminForm.vip_wait_app_url)
        await cb.message.edit_text(
            f"Пришли ссылку на мини-апп для пользователя <code>{uid_str}</code>.\n"
            "Отправь <b>clear</b>, чтобы удалить персональную мини-апп.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="vip:setapp")]]),
            disable_web_page_preview=True,
        )
        await cb.answer()

    @r.message(AdminForm.vip_wait_app_url)
    async def vip_set_app_url(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        data = await state.get_data()
        uid_str = data.get("vip_target_uid")
        if not uid_str:
            await msg.answer("Не понял, для кого менять. Зайди заново: /admin → VIP → Изменить мини-апп")
            await state.clear()
            return

        url = (msg.text or "").strip()
        clear = (url.lower() == "clear")

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == int(uid_str)).first()
            if not u:
                await msg.answer("Не нашёл пользователя."); await state.clear(); return
            if clear:
                u.vip_miniapp_url = None
                db.commit()
                await msg.answer(f"🧹 Для <code>{uid_str}</code> персональная мини-апп удалена.", reply_markup=kb_admin_main())
            else:
                u.vip_miniapp_url = url
                db.commit()
                await msg.answer(
                    f"✅ Мини-апп для <code>{uid_str}</code> обновлена.\n"
                    "Напиши пользователю, чтобы нажал /start в боте.",
                    reply_markup=kb_admin_main(),
                    disable_web_page_preview=True,
                )
        finally:
            db.close()
        await state.clear()

    dp.include_router(r)

    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        pass
    finally:
        await bot.session.close()
