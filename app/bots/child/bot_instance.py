import asyncio
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
        "ru": (
            "Ниже пошаговая инструкция по использованию бота 🧾:\n\n"
            "1. Зарегистрируйте аккаунт на PocketOption через нашего бота.\n"
            "2. Запустите бота и выберите валютную пару и время экспирации.\n"
            "3. Нажмите «Получить сигнал» и строго следуйте ему.\n"
            "4. В случае неудачного сигнала рекомендуется удвоить сумму, чтобы компенсировать убыток следующим сигналом."
        ),
        "en": (
            "Step-by-step instructions for using the bot 🧾:\n\n"
            "1) Register a PocketOption account via this bot.\n"
            "2) Start the bot and choose the currency pair and expiration time.\n"
            "3) Tap “Get signal” and follow it precisely.\n"
            "4) If a signal fails, it’s recommended to double the amount to compensate on the next signal."
        ),
    },
    "subscribe": {
        "ru": "Для начала подпишитесь на канал.\n\nПосле подписки вернитесь в бот.",
        "en": "First, subscribe to the channel.\n\nAfter subscribing, return to the bot.",
    },
    "step1": {
        "ru": (
            "⚡️Регистрация\n\n"
            "Для получения сигналов необходимо зарегистрироваться у брокера PocketOption по нашей ссылке.\n"
            "Нажмите кнопку «🟢 Зарегистрироваться», чтобы создать аккаунт.\n\n"
            "❗️После регистрации вы автоматически перейдёте на следующий шаг!"
        ),
        "en": (
            "⚡️Registration\n\n"
            "To receive signals, you must register with the PocketOption broker using our link.\n"
            "Click the «🟢 Register» button to create an account.\n\n"
            "❗️After registration you will automatically proceed to the next step!"
        ),
    },
    "step2": {
        "ru": (
            "⚡️Внесите депозит: ${{min_dep}}.\n\n"
            "Нажмите «💳 Внести депозит», чтобы пополнить баланс на сайте брокера — это нужно, чтобы сразу начать работу."
        ),
        "en": (
            "⚡️Make a deposit: ${{min_dep}}.\n\n"
            "Press «💳 Deposit» to top up the balance on the broker's website — this is required to start right away."
        ),
    },
    "unlocked": {"ru": "Доступ открыт. Нажмите «Получить сигнал».", "en": "Access granted. Press “Get signal”."},
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

def _find_stock_file(key: str, locale: str) -> Path | None:
    stock = _project_root() / "static" / "stock"
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = stock / f"{key}-{locale}.{ext}"
        if p.exists():
            return p
    return None

def tget(db, tenant_id: int, key: str, locale: str, fallback_text: str):
    tt = db.query(TenantText).filter(
        TenantText.tenant_id == tenant_id,
        TenantText.locale == locale,
        TenantText.key == key,
    ).first()
    return (tt.text if tt and tt.text else fallback_text), (tt.image_file_id if tt else None)

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
    # миграционные «подстраховки»
    if getattr(cfg, "require_subscription", None) is None:
        cfg.require_subscription = False
        db.commit(); db.refresh(cfg)
    if getattr(cfg, "vip_threshold", None) is None:
        cfg.vip_threshold = 500
        db.commit(); db.refresh(cfg)
    return cfg

def get_deposit_total(db, tenant_id: int, user: User) -> int:
    total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
        Postback.tenant_id == tenant_id,
        Postback.event == "deposit",
        Postback.click_id == str(user.tg_user_id),
        Postback.token_ok.is_(True),
    ).scalar() or 0
    return int(total)

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

# --------- подписка ----------
def _parse_channel_identifier(url: str):
    if not url:
        print("[sub] no channel_url provided")
        return None
    u = str(url).strip()
    # numeric chat_id (supergroup/channel)
    if u.startswith("-100") and u[4:].isdigit():
        try:
            ident = int(u)
        except Exception:
            print(f"[sub] bad numeric id: {u}")
            return None
        print(f"[sub] parsed chat_id={ident}")
        return ident
    if u.startswith("@"):
        print(f"[sub] parsed username={u}")
        return u
    if "t.me/" in u:
        tail = u.split("t.me/", 1)[1]
        tail = tail.split("?", 1)[0].strip("/")
        if not tail or tail.startswith("+") or tail.lower() == "joinchat":
            # по инвайт-ссылке проверка не сработает
            print(f"[sub] invite or joinchat link, cannot check: {u}")
            return None
        ident = "@" + tail if not tail.startswith("@") else tail
        print(f"[sub] parsed from url -> {ident}")
        return ident
    print(f"[sub] unknown format: {u}")
    return None


async def is_user_subscribed(bot: Bot, channel_url: str, user_id: int) -> bool:
    ident = _parse_channel_identifier(channel_url)
    if not ident:
        print(f"[sub] ident is None for channel_url='{channel_url}'")
        return False
    try:
        print(f"[sub] get_chat_member ident={ident} user_id={user_id}")
        member = await bot.get_chat_member(ident, user_id)
        status = getattr(member, "status", None)
        print(f"[sub] result status={status}")
        # В супергруппах "restricted" = участник (с ограничениями), тоже считаем подписанным
        return status in ("member", "administrator", "creator", "restricted")
    except Exception as e:
        # На каналах без админства может быть CHAT_ADMIN_REQUIRED, а также 400 если чат недоступен
        print(f"[subscribe-check] error: {e} (channel_url={channel_url!r}, ident={ident}, user_id={user_id})")
        return False


def tenant_miniapp_url(tenant: Tenant, user: User) -> str:
    # 1) Персональная VIP-мини-аппа (если админ задал руками)
    if getattr(user, "vip_miniapp_url", None):
        base = user.vip_miniapp_url.rstrip("/")
        return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

    # 2) Стоковая VIP-мини-аппа из ENV — только если пользователь VIP
    is_vip = bool(getattr(user, "is_vip", False))
    vip_env = getattr(settings, "vip_miniapp_url", None)
    if is_vip and vip_env:
        base = vip_env.rstrip("/")
        return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

    # 3) Обычная мини-аппа (пер-ботовая или из ENV)
    base = (tenant.miniapp_url or settings.miniapp_url).rstrip("/")
    return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"


# ------------------------------- КНОПКИ -------------------------------
from aiogram.types import WebAppInfo

def kb_main(locale: str, support_url: Optional[str], tenant: Tenant, user: User):
    # если доступ уже открыт — сразу открываем мини-апп
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
        [
            InlineKeyboardButton(
                text=("🏠 Главное меню" if (current or "ru") == "ru" else "🏠 Main menu"), callback_data="menu:main"
            )
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_subscribe(locale: str, channel_url: str) -> InlineKeyboardMarkup:
    go_txt = "🚀 Перейти в канал" if locale == "ru" else "🚀 Go to channel"
    back_txt = "🏠 Главное меню" if locale == "ru" else "🏠 Main menu"
    check_txt = "✅ Я подписался" if locale == "ru" else "✅ I've subscribed"

    url = (channel_url or "").strip()
    if url.startswith("@"):
        url = f"https://t.me/{url[1:]}"
    elif not (url.startswith("http://") or url.startswith("https://")):
        url = "https://t.me"

    rows = [
        [InlineKeyboardButton(text=go_txt, url=url)],
        [InlineKeyboardButton(text=check_txt, callback_data="menu:subcheck")],
        [InlineKeyboardButton(text=back_txt, callback_data="menu:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ------------------------------- РЕНДЕР ЭКРАНОВ ------------------------------
async def render_lang_screen(bot: Bot, tenant: Tenant, user: User, current_lang: Optional[str]):
    db = SessionLocal()
    try:
        locale = (current_lang or tenant.lang_default or "ru").lower()
        text, img = tget(db, tenant.id, "lang", locale, default_text("lang", locale))

        await safe_delete_message(bot, user.tg_user_id, user.last_message_id)
        rm = kb_lang(current_lang)

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

async def render_get(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        locale = user.lang or tenant.lang_default or "ru"
        cfg = get_cfg(db, tenant.id)

        # 0) Подписка
        if cfg.require_subscription:
            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                await render_subscribe(bot, tenant, user)
                db.commit()
                return

        # Доступ
        if user.step == UserStep.deposited or (not cfg.require_deposit and user.step >= UserStep.registered):
            if user.step != UserStep.deposited and not cfg.require_deposit:
                user.step = UserStep.deposited
            text, img = tget(db, tenant.id, "unlocked", locale, default_text("unlocked", locale))
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="📈 Получить сигнал" if locale == "ru" else "📈 Get signal",
                            web_app=WebAppInfo(url=tenant_miniapp_url(tenant, user)),
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main"
                        )
                    ],
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
                progress_line = (
                    f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${left})"
                    if locale == "ru"
                    else f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${left})"
                )
                text = text + progress_line

                # VIP уведомление: динамический порог
                try:
                    if dep_total >= int(cfg.vip_threshold or 500) and not getattr(user, "vip_notified", False):
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
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Внести депозит" if locale == "ru" else "💳 Deposit", url=url)],
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
            # ВАЖНО: не возвращаемся! логируем и пропускаем дальше
            print(f"[TenantGate] error: {e}")

        return await handler(event, data)


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
    vip_wait_miniapp_uid = State()

    content_wait_lang = State()
    content_wait_key = State()
    content_wait_text = State()
    content_wait_photo = State()

    bcast_wait_segment = State()
    bcast_wait_content = State()
    bcast_confirm = State()

    params_wait_min_dep = State()

def kb_admin_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Ссылки", callback_data="adm:links")],
            [InlineKeyboardButton(text="🔁 Постбэки", callback_data="adm:pb")],
            [InlineKeyboardButton(text="🧩 Контент", callback_data="adm:content")],
            [InlineKeyboardButton(text="⚙️ Параметры", callback_data="adm:params")],
            [InlineKeyboardButton(text="👑 VIP", callback_data="adm:vip")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        ]
    )

def kb_admin_links():
    return InlineKeyboardMarkup(inline_keyboard=[
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
    req_sub = getattr(cfg, "require_subscription", False)
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
            [InlineKeyboardButton(text=f"💵 Минимальный депозит: ${cfg.min_deposit}", callback_data="adm:param:set_min")],
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
                await render_main(bot, tenant, user)
                return

            await render_lang_screen(bot, tenant, user, current_lang=None)
        finally:
            db.close()

    @r.message(F.text == "/resetme")
    async def reset_me(msg: Message):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == msg.from_user.id).first()
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
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
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
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
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
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
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
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
            if not user:
                return
            await render_lang_screen(bot, tenant, user, user.lang)
            db.commit()
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

            locale = user.lang or tenant.lang_default or "ru"
            cfg = get_cfg(db, tenant.id)

            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                # всё ещё нет
                await cb.answer("Ещё не вижу подписку 🤷‍♂️" if locale == "ru" else "Still not subscribed 🤷‍♂️",
                                show_alert=False)
                return

            # Ок — сразу продолжаем обычный сценарий
            await render_get(bot, tenant, user)
            db.commit()
            await cb.answer("Готово ✅" if locale == "ru" else "All set ✅")
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
                await cb.answer();
                return

            # ВСЕГДА идём через render_get (он сам покажет unlocked с web_app)
            await render_get(bot, tenant, user)
            db.commit()
            await cb.answer()
        finally:
            db.close()

    # -------- ADMIN --------
    def owner_only(uid: int) -> bool:
        return uid == tenant.owner_tg_id  # только владелец ТЕНАНТА

    @r.message(Command("admin"))
    async def admin_entry(msg: Message, state: FSMContext):
        print(f"[child-admin] /admin from={msg.from_user.id} tenant_id={tenant.id} owner={tenant.owner_tg_id}")
        if not owner_only(msg.from_user.id):
            await msg.answer("⛔️ Нет доступа (вы не владелец этого бота)")
            return
        await state.clear()
        await msg.answer("<b>Админ-панель</b>", reply_markup=kb_admin_main())

    @r.callback_query(
        lambda c: (
                c.data in {"adm:menu", "adm:links", "adm:pb", "adm:content", "adm:broadcast", "adm:stats", "adm:params","adm:vip"}
                or (c.data or "").startswith("adm:set:")
                or (
                    (c.data or "").startswith("adm:vip:")
                    and not any((c.data or "").startswith(p) for p in (
                        "adm:vip:do:",        # отдельные хендлеры: ручные постбэки
                        "adm:vip:url:ask:",   # отдельный хендлер: запрос VIP URL (по TG ID)
                    ))
                )
             )
        )
    async def admin_router(cb: CallbackQuery, state: FSMContext):
        if not owner_only(cb.from_user.id):
            await cb.answer()
            return

        data = cb.data or ""
        action = data.split(":", 1)[1] if not data.startswith("adm:set:") else "set:" + data.split(":", 2)[2]

        if action == "menu":
            await state.clear()
            await cb.message.edit_text("<b>Админ-панель</b>", reply_markup=kb_admin_main())
            await cb.answer()
            return

        if action == "links":
            await state.clear()
            await cb.message.edit_text("🔗 Ссылки", reply_markup=kb_admin_links())
            await cb.answer()
            return

        if action == "content":
            await state.clear()
            await cb.message.edit_text("🧩 Контент: выберите язык", reply_markup=kb_content_lang())
            await cb.answer()
            return

        if action == "params":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            await cb.message.edit_text("⚙️ Параметры", reply_markup=kb_params(cfg))
            await cb.answer()
            return

        if action == "set:support":
            await state.set_state(AdminForm.waiting_support)
            await cb.message.edit_text("Пришлите <b>новый Support URL</b> одним сообщением.\n\n⬅️ Или нажмите /admin чтобы отменить.")
            await cb.answer(); return

        if action == "set:ref":
            await state.set_state(AdminForm.waiting_ref)
            await cb.message.edit_text("Пришлите <b>новую реферальную ссылку</b> одним сообщением.\n\n⬅️ Или нажмите /admin чтобы отменить.")
            await cb.answer(); return

        if action == "set:dep":
            await state.set_state(AdminForm.waiting_dep)
            await cb.message.edit_text("Пришлите <b>ссылку для депозита</b> одним сообщением.\n\n⬅️ Или нажмите /admin чтобы отменить.")
            await cb.answer(); return

        if action == "set:miniapp":
            await state.set_state(AdminForm.waiting_miniapp)
            await cb.message.edit_text(
                "Пришлите <b>Web-app URL</b> одним сообщением.\n\n"
                "Самый простой способ — выложить мини-апп на GitHub Pages и отправить публичную HTTPS-ссылку."
                "\n\n⬅️ Или нажмите /admin чтобы отменить."
            ); await cb.answer(); return

        if action == "set:channel":
            await state.set_state(AdminForm.waiting_channel)
            await cb.message.edit_text(
                "Пришлите ссылку на канал (@username или https://t.me/username).\n\n"
                "⚠️ Для приватных инвайт-ссылок (+...) проверка не сработает. Лучше сделать публичный @username и добавить бота админом."
            ); await cb.answer(); return

        # ----- VIP MENU
        if action == "vip":
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
                    [InlineKeyboardButton(text="✅ Выдать VIP доступ", callback_data="adm:vip:grant")],
                    [InlineKeyboardButton(text="🛠 Изменить мини-апп (для имеющих доступ)", callback_data="adm:vip:miniapp")],
                    [InlineKeyboardButton(text="🎯 Задать порог VIP", callback_data="adm:vip:thr")],
                    [InlineKeyboardButton(text="🆔 Управление по TG ID", callback_data="adm:vip:byid")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
                ]
            )
            await state.clear()
            await cb.message.edit_text("👑 VIP — выберите действие", reply_markup=kb)
            await cb.answer(); return

        if action == "vip:thr":
            await state.set_state(AdminForm.vip_wait_threshold)
            await cb.message.edit_text("Пришлите новое значение порога VIP (целое число, $).")
            await cb.answer(); return

        if action == "vip:list":
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
                txt = f"<b>Кандидаты VIP (≥ ${thr}):</b>\n\n"
                if not rows:
                    txt += "Пока пусто."
                else:
                    for tg_id, total, flag in rows[:50]:
                        txt += f"{flag} <code>{tg_id}</code> — ${total}\n"
            finally:
                db.close()
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")]])
            await cb.message.edit_text(txt, reply_markup=kb, disable_web_page_preview=True)
            await cb.answer(); return

        if action == "vip:reg":
            # список юзеров для ручной регистрации
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append([InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:do:reg:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для РЕГИСТРАЦИИ (ручной постбэк):", reply_markup=kb)
            await cb.answer(); return

        if action == "vip:dep":
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append([InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:do:dep:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для ДЕПОЗИТА (ручной постбэк):", reply_markup=kb)
            await cb.answer(); return

        if action == "vip:grant":
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append([InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:set:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для ВЫДАЧИ VIP:", reply_markup=kb)
            await cb.answer(); return

        if action == "vip:miniapp":
            # список только тех, у кого есть доступ (is_vip True или достигнут порог)
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
                        rows.append([InlineKeyboardButton(text=label, callback_data=f"adm:vip:miniapp:set:{u.tg_user_id}")])
                rows = rows[:50] if rows else [[InlineKeyboardButton(text="Пока нет пользователей с доступом", callback_data="adm:vip")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await cb.message.edit_text("Выберите пользователя для изменения VIP мини-аппы:", reply_markup=kb)
            await cb.answer(); return

        if action.startswith("vip:miniapp:set:"):
            uid = int(action.split(":")[-1])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); db.close(); return
                # Текущее состояние
                has_vip = bool(u.is_vip)
                has_custom = bool(u.vip_miniapp_url)
            finally:
                db.close()

            # Кнопки: выдать VIP из ENV, задать кастомный, вернуть стоковую обычную
            rows = [
                [InlineKeyboardButton(text="🟣 Выдать VIP-мини-апп (ENV)", callback_data=f"adm:vip:miniapp:env:{uid}")],
                [InlineKeyboardButton(text="✏️ Задать кастомный VIP URL",  callback_data=f"adm:vip:miniapp:ask:{uid}")],
                [InlineKeyboardButton(text="↩️ Вернуть стоковую мини-апп", callback_data=f"adm:vip:miniapp:stock:{uid}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip:miniapp")],
            ]
            status = []
            if has_vip: status.append("VIP=✅")
            if has_custom: status.append("Custom URL=✅")
            if not status: status.append("обычная мини-апп")
            title = f"Пользователь <code>{uid}</code>\nТекущее: " + ", ".join(status)

            await cb.message.edit_text(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), disable_web_page_preview=True)
            await cb.answer(); return

        if action.startswith("vip:miniapp:env:"):
            uid = int(action.split(":")[-1])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); db.close(); return
                u.is_vip = True          # включаем VIP
                u.vip_miniapp_url = None # пусть берётся из ENV (settings.vip_miniapp_url)
                db.commit()
            finally:
                db.close()
            await cb.message.edit_text("✅ Назначена VIP-мини-апп из ENV. Пользователь увидит VIP при «Получить сигнал».",
                                       reply_markup=kb_admin_main())
            await cb.answer("Готово"); return

        if action.startswith("vip:miniapp:ask:"):
            uid = int(action.split(":")[-1])
            await state.update_data(vip_user_id=uid)
            await state.set_state(AdminForm.vip_wait_miniapp_url)
            await cb.message.edit_text(
                f"Пришлите VIP Web-app URL для <code>{uid}</code> одним сообщением.\n"
                f"Чтобы очистить, пришлите «-».")
            await cb.answer(); return

        if action.startswith("vip:miniapp:stock:"):
            uid = int(action.split(":")[-1])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); db.close(); return
                u.vip_miniapp_url = None  # убираем кастом
                u.is_vip = False          # выключаем VIP → откроется обычная мини-апп (tenant/ENV)
                db.commit()
            finally:
                db.close()
            await cb.message.edit_text("↩️ Вернул обычную мини-апп. Теперь «Получить сигнал» открывает не-VIP версию.",
                                       reply_markup=kb_admin_main())
            await cb.answer("Готово"); return

        if action == "vip:byid":
            await state.set_state(AdminForm.vip_wait_user_id)
            await cb.message.edit_text("Пришлите TG ID пользователя.")
            await cb.answer(); return

        if action.startswith("vip:set:"):
            uid = int(action.split(":")[2])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); db.close(); return

                u.is_vip = True
                u.vip_notified = True  # чтобы не дублировать уведомления далее
                db.commit()
            finally:
                db.close()

            # Отправим уведомление пользователю
            try:
                locale = u.lang or tenant.lang_default or "ru"
                if locale == "ru":
                    text = "🎉 Вам выдан доступ к премиум-боту! Напишите в поддержку для подключения."
                else:
                    text = "🎉 You’ve been granted access to the premium bot! Contact support to get connected."

                kb = None
                if tenant.support_url:
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🆘 Поддержка" if locale == "ru" else "🆘 Support",
                                              url=tenant.support_url)]
                    ])

                await bot.send_message(uid, text, reply_markup=kb)
            except Exception:
                pass

            await cb.answer("VIP включён"); return

        if action.startswith("vip:unset:"):
            uid = int(action.split(":")[2])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); db.close(); return
                u.is_vip = False
                db.commit()
            finally:
                db.close()
            await cb.answer("VIP выключен"); return

        if action.startswith("vip:url:clear:"):
            uid = int(action.split(":")[3])
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                if not u:
                    await cb.answer("Юзер не найден"); db.close(); return
                u.vip_miniapp_url = None
                db.commit()
            finally:
                db.close()
            await cb.answer("URL очищен"); return

        if action == "pb":
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
            await cb.answer()
            return

        if action == "broadcast":
            await state.set_state(AdminForm.bcast_wait_segment)
            await cb.message.edit_text("📣 Рассылка: выберите сегмент", reply_markup=kb_broadcast_segments())
            await cb.answer()
            return

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
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")]]
                ),
            )
            await cb.answer()
            return

    # ---------- ADMIN: ручные постбэки (VIP) ----------

    # ---- РЕГИСТРАЦИЯ (ручной постбэк)
    @r.callback_query(lambda c: c.data and c.data.startswith("adm:vip:do:reg:"))
    async def vip_do_registration(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer()
            return
        try:
            uid = int(cb.data.split(":")[-1])
        except Exception:
            await cb.answer("Некорректный UID")
            return

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден", show_alert=True)
                return

            pb = Postback(
                tenant_id=tenant.id,
                event="registration",
                click_id=str(uid),
                trader_id=u.trader_id,
                sum=0,
                token_ok=True,
                idempotency_key=f"adm:reg:{tenant.id}:{uid}",
                raw_query="manual",
            )
            db.add(pb)

            if u.step in (UserStep.new, UserStep.asked_reg):
                u.step = UserStep.registered

            db.commit()
        finally:
            db.close()

        try:
            await cb.message.edit_text("✅ Регистрация засчитана.\n\nВыберите следующее действие.",
                                       reply_markup=kb_admin_main())
        except Exception:
            pass
        await cb.answer("OK")

    # ---- ДЕПОЗИТ (ручной постбэк)
    @r.callback_query(lambda c: c.data and c.data.startswith("adm:vip:do:dep:"))
    async def vip_do_deposit(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer()
            return
        try:
            uid = int(cb.data.split(":")[-1])
        except Exception:
            await cb.answer("Некорректный UID")
            return

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден", show_alert=True)
                return

            cfg = get_cfg(db, tenant.id)
            amount = int(cfg.min_deposit or 50)

            pb = Postback(
                tenant_id=tenant.id,
                event="deposit",
                click_id=str(uid),
                trader_id=u.trader_id,
                sum=amount,
                token_ok=True,
                idempotency_key=f"adm:dep:{tenant.id}:{uid}:{amount}",
                raw_query="manual",
            )
            db.add(pb)
            db.commit()

            total = get_deposit_total(db, tenant.id, u)
            if total >= cfg.min_deposit and u.step != UserStep.deposited:
                u.step = UserStep.deposited

            db.commit()
            # Сообщим пользователю и сразу дадим кнопку WebApp
            try:
                locale = u.lang or tenant.lang_default or "ru"
                text, img = tget(db, tenant.id, "unlocked", locale, default_text("unlocked", locale))
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(
                            text="📈 Получить сигнал" if locale == "ru" else "📈 Get signal",
                            web_app=WebAppInfo(url=tenant_miniapp_url(tenant, u))
                        )],
                        [InlineKeyboardButton(
                            text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu",
                            callback_data="menu:main"
                        )],
                    ]
                )
                await send_screen(bot, u, "unlocked", locale, text, kb, img)
                db.commit()  # сохранить обновлённый last_message_id
            except Exception as e:
                print(f"[manual-dep unlocked notify] {e}")

            thr = int(getattr(cfg, "vip_threshold", 500) or 500)
            if total >= thr and not getattr(u, "vip_notified", False):
                try:
                    locale = u.lang or tenant.lang_default or "ru"
                    msg_txt = (
                        "🎉 Вам выдан доступ к премиум-боту! Напишите /start, чтобы активировать."
                        if locale == "ru" else
                        "🎉 You’re eligible for the premium bot! Send /start to activate."
                    )
                    await bot.send_message(uid, msg_txt)
                except Exception:
                    pass
                u.vip_notified = True

            db.commit()
        finally:
            db.close()

        try:
            await cb.message.edit_text("✅ Депозит засчитан.\n\nВыберите следующее действие.",
                                       reply_markup=kb_admin_main())
        except Exception:
            pass
        await cb.answer("OK")

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
        await msg.answer("✅ Web-app URL обновлён. Кнопка «Получить сигнал» теперь открывает новую мини-аппу.",
                         reply_markup=kb_admin_main())

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

    # ---- VIP: задать порог
    @r.message(AdminForm.vip_wait_threshold)
    async def vip_set_threshold(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        try:
            val = int((msg.text or "").strip())
            if val < 1:
                raise ValueError
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
        await msg.answer(f"✅ Порог VIP обновлён: ${val}.", reply_markup=kb_admin_main())

    # ---- VIP By ID
    @r.message(AdminForm.vip_wait_user_id)
    async def vip_receive_user_id(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
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
                [InlineKeyboardButton(text="✅ Включить VIP", callback_data=f"adm:vip:set:{uid}"),
                 InlineKeyboardButton(text="❌ Выключить VIP", callback_data=f"adm:vip:unset:{uid}")],
                [InlineKeyboardButton(text="✏️ Задать VIP URL", callback_data=f"adm:vip:url:ask:{uid}")],
                [InlineKeyboardButton(text="🗑 Очистить URL", callback_data=f"adm:vip:url:clear:{uid}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip")],
            ])
            txt = (
                f"<b>Пользователь</b> <code>{uid}</code>\n"
                f"VIP: {'✅' if u.is_vip else '❌'}\n"
                f"VIP URL: {u.vip_miniapp_url or '—'}\n"
                f"Сумма депозитов: ${total}"
            )
            await msg.answer(txt, reply_markup=kb, disable_web_page_preview=True)
        finally:
            db.close()

    @r.callback_query(F.data.startswith("adm:vip:url:ask:"))
    async def vip_ask_url(cb: CallbackQuery, state: FSMContext):
        uid = int(cb.data.split(":")[-1])
        await state.update_data(vip_user_id=uid)
        await state.set_state(AdminForm.vip_wait_url)
        await cb.message.edit_text(f"Пришлите VIP Web-app URL для <code>{uid}</code> одним сообщением.")
        await cb.answer()

    @r.message(AdminForm.vip_wait_url)
    async def vip_set_url(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
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
            await state.clear()
            await msg.answer("✅ VIP URL сохранён.", reply_markup=kb_admin_main())
        finally:
            db.close()

    # Изменение мини-аппы из меню «для имеющих доступ»
    @r.message(AdminForm.vip_wait_miniapp_url)
    async def vip_set_miniapp_from_menu(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
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
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Мини-апп для пользователя обновлена. Напишите ему в ЛС, чтобы он нажал /start.", reply_markup=kb_admin_main())

    # ---- Admin: Контент
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
        await msg.answer(f"✅ Картинка сохранена для «{key_title(key, lang)}» ({lang}).", reply_markup=kb_content_edit(key, lang))

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:delphoto:"))
    async def content_delete_photo(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
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
        await cb.answer()

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:ce:reset:"))
    async def content_reset(cb: CallbackQuery, state: FSMContext):
        _, _, _, key, lang = cb.data.split(":")
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
            f"🔄 Сброшено к дефолту для «{key_title(key, lang)}» ({lang}).", reply_markup=kb_content_edit(key, lang)
        )
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
            await cb.answer()
            return
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
            await cb.answer()
            return
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
            await cb.answer()
            return
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
            await cb.answer()
            return
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.miniapp_url = None
            db.commit()
        finally:
            db.close()
        await cb.message.edit_text("✅ Вернул стоковую мини-апп (из ENV).", reply_markup=kb_admin_main())
        await cb.answer()

    # ---- Admin: Рассылка
    @r.callback_query(lambda c: c.data and c.data.startswith("adm:bs:"))
    async def bcast_choose_segment(cb: CallbackQuery, state: FSMContext):
        seg = cb.data.split(":")[2]  # all/registered/deposited
        await state.update_data(bcast_segment=seg)
        await state.set_state(AdminForm.bcast_wait_content)
        await cb.message.edit_text(f"Сегмент: <b>{seg}</b>\nПришлите текст рассылки (можно с фото).")
        await cb.answer()

    @r.message(AdminForm.bcast_wait_content)
    async def bcast_collect(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
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

        rate = max(1, settings.broadcast_rate_per_hour)
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
            await bot.send_message(tenant.owner_tg_id, f"📣 Рассылка завершена. Отправлено: {sent}, ошибок: {failed}.")
        except Exception:
            pass

    @r.callback_query(F.data == "adm:bc:run")
    async def bcast_run(cb: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        seg = data.get("bcast_segment", "all")
        text = data.get("bcast_text") or ""
        media_id = data.get("bcast_media")
        await cb.message.edit_text(
            "📣 Рассылка поставлена в очередь. Отправка будет дозировано (≤ 40/час).", reply_markup=kb_admin_main()
        )
        await state.clear()
        asyncio.create_task(_run_broadcast(seg, text, media_id))
        await cb.answer()

    # ---- Прогресс депозита (обновление)
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

            locale = user.lang or tenant.lang_default or "ru"
            cfg = get_cfg(db, tenant.id)

            # проверка подписки
            if cfg.require_subscription:
                ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
                if not ok:
                    await render_subscribe(bot, tenant, user)
                    await cb.answer("Сначала подпишитесь" if locale == "ru" else "Please subscribe first")
                    return

            # доступ уже открыт
            if user.step == UserStep.deposited:
                await render_main(bot, tenant, user)
                await cb.answer("Доступ уже открыт ✅" if locale == "ru" else "Access already unlocked ✅")
                return

            dep_total = get_deposit_total(db, tenant.id, user)
            left = max(0, cfg.min_deposit - dep_total)

            # VIP уведомление по динамическому порогу
            try:
                if dep_total >= int(cfg.vip_threshold or 500) and not getattr(user, "vip_notified", False):
                    msg_txt = (
                        "🎉 Поздравляем! Вам доступен премиум-бот. Напишите в поддержку для подключения."
                        if locale == "ru" else
                        "🎉 Congrats! You’re eligible for the premium bot. Please contact support to get access."
                    )
                    await bot.send_message(user.tg_user_id, msg_txt)
                    user.vip_notified = True
            except Exception as e:
                print(f"[vip-notify] {e}")

            text, img = tget(db, tenant.id, "step2", locale, default_text("step2", locale))
            text = text.replace("{{min_dep}}", str(cfg.min_deposit))
            text += (
                f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${left})"
                if locale == "ru"
                else f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${left})"
            )

            url = f"{settings.service_host}/r/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Внести депозит" if locale == "ru" else "💳 Deposit", url=url)],
                    [InlineKeyboardButton(text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu",
                                          callback_data="menu:main")],
                ]
            )

            is_media = bool(cb.message.photo or cb.message.document or cb.message.video or cb.message.animation)
            try:
                if is_media:
                    await cb.message.edit_caption(text, reply_markup=kb)
                else:
                    await cb.message.edit_text(text, reply_markup=kb)
            except TelegramBadRequest as e:
                msg = (str(e) or "").lower()
                if "message is not modified" in msg:
                    await cb.answer("Без изменений" if locale == "ru" else "No changes")
                    return
                await cb.message.answer(text, reply_markup=kb)
                await cb.answer("Обновлено" if locale == "ru" else "Updated")
                return

            await cb.answer("Обновлено" if locale == "ru" else "Updated")
            db.commit()
        finally:
            db.close()

    # === ВАЖНО: подключаем роутер и запускаем поллинг ОДИН РАЗ, в самом конце run_child_bot ===
    dp.include_router(r)

    try:
        # На всякий случай снимем вебхук перед поллингом, чтобы не было конфликта
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        pass
    finally:
        await bot.session.close()
