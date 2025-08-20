import asyncio
from typing import Optional, List, Tuple

from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, FSInputFile
)

from sqlalchemy import func

from pathlib import Path

from app.db import SessionLocal
from app.models import (
    Tenant, User, UserStep, TenantText, TenantConfig, Postback, TenantStatus
)
from app.settings import settings
from app.utils.common import safe_delete_message


# ======================= КОНСТАНТЫ / КЛЮЧИ =======================

# Порог депозита для кандидатов VIP (можно менять числом)
VIP_THRESHOLD = 500

KEYS = [
    ("lang",      {"ru": "Выбор языка",           "en": "Language"}),
    ("main",      {"ru": "Главное меню",          "en": "Main menu"}),
    ("guide",     {"ru": "Инструкция",            "en": "Instruction"}),
    ("subscribe", {"ru": "Подписка на канал",     "en": "Subscribe"}),
    ("step1",     {"ru": "Шаг 1. Регистрация",    "en": "Step 1. Registration"}),
    ("step2",     {"ru": "Шаг 2. Депозит",        "en": "Step 2. Deposit"}),
    ("unlocked",  {"ru": "Доступ открыт",         "en": "Access granted"}),
]

DEFAULT_TEXTS = {
    "lang": {
        "ru": "Выберите язык",
        "en": "Choose your language",
    },
    "main": {
        "ru": "Главное меню",
        "en": "Main menu",
    },
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
        "ru": (
            "Для начала подпишитесь на канал.\n\n"
            "После подписки вернитесь сюда."
        ),
        "en": (
            "First, subscribe to the channel.\n\n"
            "Then come back here."
        ),
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
    "unlocked": {
        "ru": "Доступ открыт. Нажмите «Получить сигнал».",
        "en": "Access granted. Press “Get signal”.",
    },
}


# ======================= ХЕЛПЕРЫ ТЕКСТОВ/КАРТИНОК =======================

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


# ======================= DB/CFG/POSTBACK ХЕЛПЕРЫ =======================

def get_cfg(db: SessionLocal, tenant_id: int) -> TenantConfig:
    cfg = db.query(TenantConfig).filter(TenantConfig.tenant_id == tenant_id).first()
    if not cfg:
        cfg = TenantConfig(tenant_id=tenant_id, require_deposit=True, min_deposit=50)
        db.add(cfg)
        db.commit()
        db.refresh(cfg)
    # флаг подписки по умолчанию False (если колонки нет/не заполнена)
    if getattr(cfg, "require_subscription", None) is None:
        cfg.require_subscription = False
        db.commit()
        db.refresh(cfg)
    return cfg

def tget(db, tenant_id: int, key: str, locale: str, fallback_text: str) -> Tuple[str, Optional[str]]:
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

def list_vip_candidates(db, tenant_id: int, threshold: int = VIP_THRESHOLD, limit: int = 20) -> List[Tuple[int,int]]:
    """
    Возвращает список пар (tg_user_id, total_sum), где total_sum >= threshold.
    """
    # выберем всех юзеров арендатора
    users = db.query(User).filter(User.tenant_id == tenant_id).all()
    res = []
    for u in users:
        if not u.tg_user_id:
            continue
        s = get_deposit_total(db, tenant_id, u)
        if s >= threshold:
            res.append((u.tg_user_id, s))
    # отсортируем по сумме по убыванию
    res.sort(key=lambda x: x[1], reverse=True)
    return res[:limit]


# ======================= ОТПРАВКА ЭКРАНОВ =======================

async def send_screen(bot: Bot, user: User, key: str, locale: str, text: str, kb, image_file_id: Optional[str]):
    await safe_delete_message(bot, user.tg_user_id, user.last_message_id)

    # 1) сохранить file_id — шлём его
    if image_file_id:
        try:
            m = await bot.send_photo(user.tg_user_id, image_file_id, caption=text, reply_markup=kb)
            user.last_message_id = m.message_id
            return
        except TelegramBadRequest:
            pass
        except Exception:
            pass

    # 2) стоковая картинка из /static/stock
    p = _find_stock_file(key, locale)
    if p:
        try:
            m = await bot.send_photo(user.tg_user_id, FSInputFile(str(p)), caption=text, reply_markup=kb)
            user.last_message_id = m.message_id
            return
        except Exception:
            pass

    # 3) просто текст
    m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)
    user.last_message_id = m.message_id


# ======================= КЛАВИАТУРЫ =======================

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
    rows = [
        [InlineKeyboardButton(text=go_txt, url=channel_url or "about:blank")],
        [InlineKeyboardButton(text=back_txt, callback_data="menu:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

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

def kb_vip_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏅 Кандидаты", callback_data="adm:vip:candidates")],
            [InlineKeyboardButton(text="🛠 Изменить мини-апп", callback_data="adm:vip:pick")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )

def kb_vip_pick_user(items: List[Tuple[int,int]], back_to="adm:vip"):
    rows = []
    for tg_id, total in items:
        rows.append([InlineKeyboardButton(text=f"{tg_id} · ${total}", callback_data=f"adm:vip:set:{tg_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ======================= ВСПОМОГАТЕЛЬНОЕ =======================

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

def tenant_miniapp_url(tenant: Tenant, user: User) -> str:
    # индивидуальная miniapp у юзера имеет приоритет
    if getattr(user, "vip_miniapp_url", None):
        base = user.vip_miniapp_url.rstrip("/")
    else:
        base = (tenant.miniapp_url or settings.miniapp_url).rstrip("/")
    return f"{base}?tenant_id={tenant.id}&uid={user.tg_user_id}"

def _parse_channel_identifier(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if u.startswith("@"):
        return u
    if "t.me/" in u:
        tail = u.split("t.me/", 1)[1]
        tail = tail.split("?", 1)[0].strip("/")
        if tail and not tail.startswith("+") and tail.lower() != "joinchat":
            return "@" + tail
    if u.startswith("-100"):
        return u
    return None

async def is_user_subscribed(bot: Bot, channel: str, user_id: int) -> bool:
    """
    Проверяем, состоит ли пользователь в канале.
    Работает и для приватных каналов (с заявками), если бот — админ.
    channel: @username | https://t.me/username | -100xxxxxxxxxx
    """
    if not channel:
        return True
    try:
        ch = _parse_channel_identifier(channel) or channel
        if ch.startswith("http"):
            if "t.me/" in ch:
                ch = ch.split("t.me/")[-1]
                ch = ch.lstrip("+@")
        member = await bot.get_chat_member(ch, user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        print(f"[subscribe-check] error: {e}")
        return False


# ======================= РЕНДЕР ЭКРАНОВ =======================

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

        # 0) Сначала — подписка
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
                        [
                            InlineKeyboardButton(
                                text="🟢  Зарегистрироваться" if locale == "ru" else "🟢  Register", url=url
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main"
                            )
                        ],
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

                url = f"{settings.service_host}/r/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Внести депозит" if locale == "ru" else "💳 Deposit", url=url)],
                        [
                            InlineKeyboardButton(
                                text=("🔄 Прогресс: $" if locale == "ru" else "🔄 Progress: $")
                                + f"{dep_total}/{cfg.min_deposit}",
                                callback_data="prog:dep",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main"
                            )
                        ],
                    ]
                )
                user.step = UserStep.asked_deposit
                await send_screen(bot, user, "step2", locale, text, kb, img)

        db.commit()
    finally:
        db.close()


# ======================= ADMIN FSM / STATES =======================

class TenantGate(BaseMiddleware):
    def __init__(self, tenant_id: int):
        super().__init__()
        self.tenant_id = tenant_id

    async def __call__(self, handler, event, data):
        try:
            # проверка статуса арендатора
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

    vip_wait_user = State()
    vip_wait_user_url = State()


# ======================= ЗАПУСК ДЕТСКОГО БОТА =======================

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
            # берём или создаём запись пользователя
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
                db.commit()
                return

            await render_lang_screen(bot, tenant, user, None)
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

    @r.callback_query(F.data == "menu:get")
    async def on_get(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
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
            or (c.data or "").startswith(("adm:set:", "adm:cl:", "adm:ck:", "adm:ce:", "adm:bs:", "adm:vip:"))
        )
    )
    async def admin_router(cb: CallbackQuery, state: FSMContext):
        if not owner_only(cb.from_user.id):
            await cb.answer()
            return

        raw = cb.data or ""
        action = raw.split(":", 1)[1] if not raw.startswith(("adm:set:",)) else "set:" + raw.split(":", 2)[2]

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

        if action == "vip":
            await state.clear()
            await cb.message.edit_text("👑 VIP", reply_markup=kb_vip_main())
            await cb.answer()
            return

        # --- LINKS SETTERS ---

        if action == "set:support":
            await state.set_state(AdminForm.waiting_support)
            await cb.message.edit_text("Пришлите <b>новый Support URL</b> одним сообщением.\n\n⬅️ Или нажмите /admin чтобы отменить.")
            await cb.answer()
            return

        if action == "set:ref":
            await state.set_state(AdminForm.waiting_ref)
            await cb.message.edit_text("Пришлите <b>новую реферальную ссылку</b> одним сообщением.\n\n⬅️ Или нажмите /admin чтобы отменить.")
            await cb.answer()
            return

        if action == "set:dep":
            await state.set_state(AdminForm.waiting_dep)
            await cb.message.edit_text("Пришлите <b>ссылку для депозита</b> одним сообщением.\n\n⬅️ Или нажмите /admin чтобы отменить.")
            await cb.answer()
            return

        if action == "set:miniapp":
            await state.set_state(AdminForm.waiting_miniapp)
            await cb.message.edit_text(
                "Пришлите <b>Web-app URL</b> одним сообщением.\n\n"
                "Самый простой способ — выложить мини-апп на GitHub Pages и отправить публичную HTTPS-ссылку."
                "\n\n⬅️ Или нажмите /admin чтобы отменить."
            )
            await cb.answer()
            return

        if action == "set:channel":
            await state.set_state(AdminForm.waiting_channel)
            await cb.message.edit_text(
                "Пришлите ссылку на канал (@username | https://t.me/username | -100xxxxxxxxxx).\n\n"
                "⚠️ Для приватных инвайт-ссылок (+...) проверка не сработает. Лучше использовать @username или chat_id.")
            await cb.answer()
            return

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
            t.miniapp_url = None  # вернём значение из ENV (settings.miniapp_url)
            db.commit()
        finally:
            db.close()
        await cb.message.edit_text("✅ Вернул стоковую мини-апп (из ENV).", reply_markup=kb_admin_main())
        await cb.answer()

    # ---- Admin: VIP

    @r.callback_query(F.data == "adm:vip:candidates")
    async def vip_candidates(cb: CallbackQuery):
        db = SessionLocal()
        try:
            items = list_vip_candidates(db, tenant.id, VIP_THRESHOLD, limit=20)
        finally:
            db.close()
        if not items:
            await cb.message.edit_text("👑 Кандидаты не найдены.", reply_markup=kb_vip_main())
        else:
            await cb.message.edit_text(
                f"🏅 Кандидаты (порог ${VIP_THRESHOLD}): выбери пользователя",
                reply_markup=kb_vip_pick_user(items, back_to="adm:vip")
            )
        await cb.answer()

    @r.callback_query(F.data == "adm:vip:pick")
    async def vip_pick(cb: CallbackQuery, state: FSMContext):
        # Покажем первых 20 пользователей (не фильтруем по сумме, просто для выбора)
        db = SessionLocal()
        try:
            users = db.query(User).filter(User.tenant_id == tenant.id).all()
            items = []
            for u in users:
                if not u.tg_user_id:
                    continue
                s = get_deposit_total(db, tenant.id, u)
                items.append((u.tg_user_id, s))
            items.sort(key=lambda x: x[1], reverse=True)
            items = items[:20]
        finally:
            db.close()

        if not items:
            await cb.message.edit_text("Пока нет пользователей.", reply_markup=kb_vip_main())
        else:
            await cb.message.edit_text("Выбери пользователя для назначения персональной mini-app:",
                                       reply_markup=kb_vip_pick_user(items, back_to="adm:vip"))
        await cb.answer()

    @r.callback_query(lambda c: c.data and c.data.startswith("adm:vip:set:"))
    async def vip_set_user(cb: CallbackQuery, state: FSMContext):
        tg_id = int(cb.data.split(":")[3])
        await state.set_state(AdminForm.vip_wait_user_url)
        await state.update_data(vip_target=tg_id)
        await cb.message.edit_text(
            f"Введи полный URL mini-app для пользователя <code>{tg_id}</code>.\n"
            f"Чтобы очистить персональную mini-app и вернуть общую — отправь «-».")
        await cb.answer()

    @r.message(AdminForm.vip_wait_user_url)
    async def vip_save_user_url(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        data = await state.get_data()
        tg_id = int(data.get("vip_target"))
        url = (msg.text or "").strip()

        db = SessionLocal()
        try:
            user = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == tg_id).first()
            if not user:
                await msg.answer("Пользователь не найден.")
                await state.clear()
                return
            if url == "-":
                user.vip_miniapp_url = None
            else:
                user.vip_miniapp_url = url
            db.commit()
        finally:
            db.close()

        await state.clear()
        await msg.answer("✅ Персональная mini-app сохранена. Сообщи пользователю — пусть нажмёт /start.", reply_markup=kb_admin_main())

    # ---- Admin: Рассылка (без изменений)
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
            user = db.query(User).filter(User.tenant_id == tenant.id,
                                         User.tg_user_id == cb.from_user.id).first()
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
            text, img = tget(db, tenant.id, "step2", locale, default_text("step2", locale))
            text = text.replace("{{min_dep}}", str(cfg.min_deposit))
            text += (
                f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${max(0, cfg.min_deposit - dep_total)})"
                if locale == "ru"
                else f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${max(0, cfg.min_deposit - dep_total)})"
            )

            url = f"{settings.service_host}/r/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Внести депозит" if locale == "ru" else "💳 Deposit", url=url)],
                    [
                        InlineKeyboardButton(
                            text=("🔄 Прогресс: $" if locale == "ru" else "🔄 Progress: $") + f"{dep_total}/{cfg.min_deposit}",
                            callback_data="prog:dep",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="🏠 Главное меню" if locale == "ru" else "🏠 Main menu", callback_data="menu:main"
                        )
                    ],
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
        finally:
            db.close()

    dp.include_router(r)

    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        pass
    finally:
        await bot.session.close()
