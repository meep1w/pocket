# app/bots/child/bot_instance.py
import asyncio
import contextlib
from math import ceil
from typing import Optional, List, Tuple, Union
from sqlalchemy import func, or_

from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, FSInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command


from app.models import Tenant, User, UserStep, TenantText, TenantConfig, Postback, TenantStatus
from app.db import SessionLocal
from app.settings import settings
from app.utils.common import safe_delete_message
from datetime import datetime

from pathlib import Path

# ---------------------- ЭКРАНЫ / КЛЮЧИ ----------------------
KEYS: List[Tuple[str, dict]] = [
    ("lang",       {"ru": "Выбор языка",        "en": "Language", "hi": "भाषा चुनें",     "es": "Idioma"}),
    ("main",       {"ru": "Главное меню",       "en": "Main menu", "hi": "मुख्य मेनू",     "es": "Menú principal"}),
    ("guide",      {"ru": "Инструкция",         "en": "Instruction", "hi": "निर्देश",     "es": "Instrucciones"}),
    ("subscribe",  {"ru": "Подписка на канал",  "en": "Subscribe",   "hi": "सदस्यता लें", "es": "Suscribirse"}),
    ("step1",      {"ru": "Шаг 1. Регистрация", "en": "Step 1. Registration", "hi": "चरण 1. पंजीकरण", "es": "Paso 1. Registro"}),
    ("step2",      {"ru": "Шаг 2. Депозит",     "en": "Step 2. Deposit", "hi": "चरण 2. जमा", "es": "Paso 2. Depósito"}),
    ("unlocked",   {"ru": "Доступ открыт",      "en": "Access granted", "hi": "प्रवेश मिला", "es": "Acceso concedido"}),

    # --- Языковые ярлыки (для меню выбора языка)
    ("lang_label_ru", {"ru": "Ярлык языка: RU", "en": "Language label: RU", "hi": "भाषा लेबल: RU", "es": "Etiqueta de idioma: RU"}),
    ("lang_label_en", {"ru": "Ярлык языка: EN", "en": "Language label: EN", "hi": "भाषा लेबल: EN", "es": "Etiqueta de idioma: EN"}),
    ("lang_label_hi", {"ru": "Ярлык языка: HI", "en": "Language label: HI", "hi": "भाषा लेबल: HI", "es": "Etiqueta de idioma: HI"}),
    ("lang_label_es", {"ru": "Ярлык языка: ES", "en": "Language label: ES", "hi": "भाषा लेबल: ES", "es": "Etiqueta de idioma: ES"}),

    # --- Кнопки (кастомизируемые)
    ("btn_main",          {"ru": "Кнопка: Главное меню",   "en": "Button: Main menu", "hi": "बटन: मुख्य मेनू", "es": "Botón: Menú principal"}),
    ("btn_instruction",   {"ru": "Кнопка: Инструкция",     "en": "Button: Instruction", "hi": "बटन: निर्देश", "es": "Botón: Instrucciones"}),
    ("btn_get_signal",    {"ru": "Кнопка: Получить сигнал","en": "Button: Get signal", "hi": "बटन: सिग्नल प्राप्त करें", "es": "Botón: Obtener señal"}),
    ("btn_support",       {"ru": "Кнопка: Поддержка",      "en": "Button: Support", "hi": "บटन: समर्थन", "es": "Botón: Soporte"}),
    ("btn_change_lang",   {"ru": "Кнопка: Сменить язык",   "en": "Button: Change language", "hi": "बटन: भाषा बदलें", "es": "Botón: Cambiar idioma"}),
    ("btn_go_channel",    {"ru": "Кнопка: Перейти в канал","en": "Button: Go to channel", "hi": "บटन: चैनल पर जाएं", "es": "Botón: Ir al canal"}),
    ("btn_ive_subscribed",{"ru": "Кнопка: Я подписался",   "en": "Button: I've subscribed", "hi": "बटन: मैंने सदस्यता ली", "es": "Botón: Ya me suscribí"}),
]

# ---------------------- ДЕФОЛТНЫЕ ТЕКСТЫ ----------------------
DEFAULT_TEXTS = {
    "lang": {
        "ru": "Выберите язык",
        "en": "Choose your language",
        "hi": "भाषा चुनें",
        "es": "Elige tu idioma",
    },
    "main": {
        "ru": "Главное меню",
        "en": "Main menu",
        "hi": "मुख्य मेनू",
        "es": "Menú principal",
    },
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
            "11. Нажмите кнопку «Сгенерировать сигнал» и торгуйте строго исходя из аналитики бота, "
            "старайтесь подбирать более высокую вероятность.\n"
            "12. Заработайте профит."
        ),
        "en": (
            "1. Register an account on the broker {{ref}}, be sure to use our bot, "
            "to do this go to /start > Get signal > Register\n"
            "2. Wait for the automatic registration verification; the bot will notify you.\n"
            "3. After successful verification, make a deposit via /start > Get signal > Make a deposit\n"
            "4. Wait for the automatic deposit verification; the bot will notify you.\n"
            "5. Click “Get signal”.\n"
            "6. Select a trading instrument in the first line of the bot interface.\n"
            "7. Mirror this instrument on {{ref}}.\n"
            "8. Choose the trading model: TESSA Plus for regular users, TESSA Quantum for platinum users.\n"
            "9. Select any expiration time.\n"
            "10. Mirror the same expiration time on {{ref}}.\n"
            "11. Press “Generate signal” and trade strictly based on the bot’s analytics, "
            "try to pick higher probability entries.\n"
            "12. Earn a profit."
        ),
        "hi": (
            "1. ब्रोकर {{ref}} पर खाता हमारे बॉट के माध्यम से पंजीकृत करें, "
            "इसके लिए /start > सिग्नल प्राप्त करें > पंजीकरण पर जाएँ।\n"
            "2. पंजीकरण का स्वतः सत्यापन होने दें — बॉट आपको सूचित करेगा।\n"
            "3. सफल सत्यापन के बाद /start > सिग्नल प्राप्त करें > जमा करें से डिपॉज़िट करें।\n"
            "4. डिपॉज़िट का स्वतः सत्यापन होने दें — बॉट आपको सूचित करेगा।\n"
            "5. “सिग्नल प्राप्त करें” दबाएँ।\n"
            "6. бॉट इंटरफ़ेस की पहली पंक्ति में ट्रेडिंग इंस्ट्रूमेंट चुनें।\n"
            "7. उसी इंस्ट्रूमेंट को {{ref}} पर भी चुनें (डुप्लिकेट करें)।\n"
            "8. ट्रेडिंग मॉडल चुनें: सामान्य यूज़र्स के लिए TESSA Plus, प्लैटिनम यूज़र्स के लिए TESSA Quantum।\n"
            "9. कोई भी एक्सपायरी समय चुनें।\n"
            "10. वही एक्सपायरी समय {{ref}} पर भी सेट करें।\n"
            "11. “सиг्नल जेनरेट करें” दबाएँ और बॉट की एनालिटिक्स के अनुसार ही ट्रेड करें, "
            "उच्च संभावना वाले एंट्रीज़ चुनने की कोशिश करें।\n"
            "12. लाभ कमाएँ।"
        ),
        "es": (
            "1. Registra una cuenta en el bróker {{ref}} utilizando nuestro bot, "
            "ve a /start > Obtener señal > Registrarse\n"
            "2. Espera la verificación automática del registro; el bot te avisará.\n"
            "3. Tras la verificación, realiza un depósito desde /start > Obtener señal > Hacer depósito\n"
            "4. Espera la verificación automática del depósito; el bot te avisará.\n"
            "5. Pulsa “Obtener señal”.\n"
            "6. Elige un instrumento de trading en la primera línea de la interfaz del bot.\n"
            "7. Duplica ese instrumento en {{ref}}.\n"
            "8. Elige el modelo de trading: TESSA Plus para usuarios normales, TESSA Quantum para platinum usuarios.\n"
            "9. Selecciona cualquier tiempo de expiración.\n"
            "10. Duplica el mismo tiempo de expiración en {{ref}}.\n"
            "11. Pulsa “Generar señal” y opera estrictamente según la analítica del bot, "
            "intenta seleccionar entradas de mayor probabilidad.\n"
            "12. Obtén beneficios."
        ),
    },

    "subscribe": {
        "ru": "Для начала подпишитесь на канал.\n\nПосле подписки вернитесь в бот.",
        "en": "First, subscribe to the channel.\n\nAfter subscribing, return to the bot.",
        "hi": "पहले चैनल को सब्सक्राइब करें।\n\nसदस्यता लेने के बाद बॉट में वापस आएँ।",
        "es": "Primero, suscríbete al canal.\n\nDespués de suscribirte, regresa al bot.",
    },
    "step1": {
        "ru": "⚡️Регистрация\n\nДля получения сигналов нужно зарегистрироваться по нашей ссылке.",
        "en": "⚡️Registration\n\nTo receive signals, you need to register via our link.",
        "hi": "⚡️पंजीकरण\n\nसिग्नल पाने के लिए आपको हमारी लिंक से पंजीकरण करना होगा।",
        "es": "⚡️Registro\n\nPara recibir señales, debes registrarte con nuestro enlace.",
    },
    "step2": {
        "ru": "⚡️Внесите депозит: ${{min_dep}}.",
        "en": "⚡️Make a deposit: ${{min_dep}}.",
        "hi": "⚡️जमा करें: ${{min_dep}}.",
        "es": "⚡️Haz un depósito: ${{min_dep}}.",
    },
    "unlocked": {
        "ru": "🎉 Доступ открыт. Нажмите «Получить сигнал».",
        "en": "🎉 Access granted. Press “Get signal”.",
        "hi": "🎉 एक्सेस मिल गया। “सिग्नल प्राप्त करें” दबाएँ।",
        "es": "🎉 Acceso concedido. Pulsa “Obtener señal”.",
    },

    # --- Кнопки (дефолтные подписи)
    "btn_main": {
        "ru": "🏠 Главное меню", "en": "🏠 Main menu", "hi": "🏠 मुख्य मेनू", "es": "🏠 Menú principal"
    },
    "btn_instruction": {
        "ru": "📘 Инструкция", "en": "📘 Instruction", "hi": "📘 निर्देश", "es": "📘 Instrucciones"
    },
    "btn_get_signal": {
        "ru": "📈 Получить сигнал", "en": "📈 Get signal", "hi": "📈 सिग्नल प्राप्त करें", "es": "📈 Obtener señal"
    },
    "btn_support": {
        "ru": "🆘 Поддержка", "en": "🆘 Support", "hi": "🆘 समर्थन", "es": "🆘 Soporte"
    },
    "btn_change_lang": {
        "ru": "🌐 Сменить язык", "en": "🌐 Change language", "hi": "🌐 भाषा बदलें", "es": "🌐 Cambiar idioma"
    },
    "btn_go_channel": {
        "ru": "🚀 Перейти в канал", "en": "🚀 Go to channel", "hi": "🚀 चैनल पर जाएँ", "es": "🚀 Ir al canal"
    },
    "btn_ive_subscribed": {
        "ru": "✅ Я подписался", "en": "✅ I've subscribed", "hi": "✅ मैंने सदस्यता ली", "es": "✅ Ya me suscribí"
    },

    # Ярлыки языков (то, как они показываются в меню выбора языка)
    "lang_label_ru": {
        "ru": "🇷🇺 Русский", "en": "🇷🇺 Russian", "hi": "🇷🇺 रूसी", "es": "🇷🇺 Ruso"
    },
    "lang_label_en": {
        "ru": "🇬🇧 English", "en": "🇬🇧 English", "hi": "🇬🇧 अंग्रेज़ी", "es": "🇬🇧 Inglés"
    },
    "lang_label_hi": {
        "ru": "🇮🇳 Hindi", "en": "🇮🇳 Hindi", "hi": "🇮🇳 हिन्दी", "es": "🇮🇳 Hindi"
    },
    "lang_label_es": {
        "ru": "🇪🇸 Spanish", "en": "🇪🇸 Spanish", "hi": "🇪🇸 स्पेनिश", "es": "🇪🇸 Español"
    },
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
    return DEFAULT_TEXTS.get(key, {}).get(locale) or DEFAULT_TEXTS.get(key, {}).get("en", key)

def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]

def _find_stock_file(key: str, locale: str) -> Optional[Path]:
    """
    Для hi/es используем картинки от en, если своих нет.
    """
    stock = _project_root() / "static" / "stock"
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = stock / f"{key}-{locale}.{ext}"
        if p.exists():
            return p
    if locale in ("hi", "es"):
        for ext in ("jpg", "jpeg", "png", "webp"):
            p = stock / f"{key}-en.{ext}"
            if p.exists():
                return p
    return None

USERS_PER_PAGE = 10

async def _safe_edit_msg(cb: CallbackQuery, text: str, kb=None):
    # сначала пробуем как текст
    try:
        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        return
    except Exception:
        pass
    # если было медиа — правим подпись
    try:
        await cb.message.edit_caption(caption=text, reply_markup=kb)
        return
    except Exception:
        pass
    # крайний случай — присылаем новое сообщение
    try:
        await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        pass

async def _try_username(bot: Bot, uid: int) -> str:
    # Ленивая попытка получить username (на странице 10 штук — норм)
    try:
        chat = await bot.get_chat(uid)
        if getattr(chat, "username", None):
            return f"@{chat.username}"
    except Exception:
        pass
    return "—"

# ---------------------- ПОДПИСКА (fixed) ----------------------
def parse_channel_field(raw: str) -> tuple[Optional[Union[int, str]], Optional[str]]:
    """
    Поддерживает:
      - "@publicname"
      - "https://t.me/publicname" / "t.me/publicname"
      - "-1001234567890 | https://t.me/+invite"
    Возвращает:
      ident: int (для -100...) или str (для @name)
      url:   https://... (кликабельная ссылка)
    """
    if not raw:
        return None, None

    # чистим невидимые/мусорные символы
    raw = "".join(ch for ch in raw.strip() if ch.isprintable())

    if "|" in raw:
        left, right = [x.strip() for x in raw.split("|", 1)]
    else:
        left, right = raw, None

    ident: Optional[Union[int, str]] = None
    if left.startswith("-100"):
        num = "".join(ch for ch in left if ch.isdigit() or ch == "-")
        try:
            ident = int(num)
        except Exception:
            ident = None
    elif left.startswith("@"):
        ident = left
    elif "t.me/" in left:
        tail = left.split("t.me/", 1)[1]
        name = tail.split("/", 1)[0].lstrip("@")
        if name and not (name.startswith("+") or name.lower().startswith("joinchat")):
            ident = f"@{name}"

    url: Optional[str] = None
    val = (right or left).strip()
    if val.startswith("@"):
        url = f"https://t.me/{val[1:]}"
    elif val.startswith("http://") or val.startswith("https://"):
        url = val
    elif "t.me/" in val:
        url = "https://" + val.lstrip("/")

    return ident, url

async def is_user_subscribed(bot: Bot, channel_url: str, user_id: int) -> bool:
    """
    Строгая, но устойчивaя проверка:
    - приватный канал: ID '-100...' приводим к int;
    - публичный: пробуем '@name' и 'name' (без @);
    - Forbidden/BadRequest -> False, но логируем причину.
    """
    ident, _ = parse_channel_field(channel_url or "")
    if not ident:
        return True  # не задан канал — не блокируем

    chat_id = ident
    # аккуратно приводим к int только если это str вида "-100..."
    if isinstance(ident, str):
        s = ident.strip()
        if s.startswith("-100") and s[1:].isdigit():
            with contextlib.suppress(Exception):
                chat_id = int(s)

    try:
        m = await bot.get_chat_member(chat_id, user_id)
        status = getattr(m, "status", None)
        return status in ("member", "administrator", "creator", "restricted")

    except TelegramBadRequest as e:
        # иногда '@name' срабатывает без '@'
        if isinstance(chat_id, str) and chat_id.startswith("@"):
            try:
                m = await bot.get_chat_member(chat_id[1:], user_id)
                status = getattr(m, "status", None)
                return status in ("member", "administrator", "creator", "restricted")
            except Exception:
                pass
        print(f"[subscribe-check][badrequest] ident={ident} uid={user_id} err={e}")
        return False

    except TelegramForbiddenError as e:
        print(f"[subscribe-check][forbidden] ident={ident} uid={user_id} err={e}")
        return False

    except Exception as e:
        print(f"[subscribe-check][unexpected] ident={ident} uid={user_id} err={e!r}")
        return False


# ---------------------- УТИЛЫ ДЛЯ СВЕЖИХ ДАННЫХ ----------------------
def tget(db, tenant_id: int, key: str, locale: str, fallback_text: str):
    tt = db.query(TenantText).filter(
        TenantText.tenant_id == tenant_id,
        TenantText.locale == locale,
        TenantText.key == key,
    ).first()
    return (tt.text if tt and tt.text else fallback_text), (tt.image_file_id if tt else None)

def tget_label(db, tenant_id: int, key: str, locale: str) -> str:
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
    return db.query(Tenant).filter(Tenant.id == tenant_id).first()

def get_deposit_total(db, tenant_id: int, user: User) -> int:
    """
    Суммируем все подтверждённые депозиты пользователя по любым связкам:
      click_id ∈ {str(tg_user_id), user.click_id}
      ИЛИ trader_id ∈ {user.trader_id}
    """
    click_keys = set()
    if getattr(user, "tg_user_id", None):
        click_keys.add(str(user.tg_user_id))
    if getattr(user, "click_id", None):
        click_keys.add(str(user.click_id))

    trader_keys = set()
    if getattr(user, "trader_id", None):
        trader_keys.add(str(user.trader_id))

    if not click_keys and not trader_keys:
        return 0

    total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
        Postback.tenant_id == tenant_id,
        Postback.event == "deposit",
        Postback.token_ok.is_(True),
        or_(
            (Postback.click_id.in_(list(click_keys)) if click_keys else False),
            (Postback.trader_id.in_(list(trader_keys)) if trader_keys else False),
        )
    ).scalar() or 0
    return int(total)



# -------------------------- ОТПРАВКА ЭКРАНА (авто-удаление) --------------------------
# -------------------------- ОТПРАВКА ЭКРАНА (сначала send, потом delete старого) --------------------------
async def send_screen(bot: Bot, user: User, key: str, locale: str, text: str,
                      kb: Optional[InlineKeyboardMarkup], image_file_id: Optional[str]):
    """
    Отправляем новый экран (фото/сток/текст), и только после успешной отправки
    удаляем предыдущее сообщение (если было). Так не будет «тишины» при ошибках отправки.
    """
    old_msg_id = getattr(user, "last_message_id", None)
    m = None

    # 1) Пытаемся отправить кастомную картинку
    if image_file_id:
        try:
            m = await bot.send_photo(user.tg_user_id, image_file_id, caption=text, reply_markup=kb)
        except TelegramBadRequest:
            m = None
        except Exception:
            m = None

    # 2) Если не удалось — пробуем сток
    if not m:
        p = _find_stock_file(key, locale)
        if p:
            try:
                m = await bot.send_photo(user.tg_user_id, FSInputFile(str(p)), caption=text, reply_markup=kb)
            except Exception:
                m = None

    # 3) Последний фоллбек — просто текст
    if not m:
        try:
            m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)
        except Exception:
            # совсем крайний случай: без клавы
            with contextlib.suppress(Exception):
                m = await bot.send_message(user.tg_user_id, text)

    # 4) Если что-то таки отправили — обновляем last_message_id и удаляем старое
    if m:
        user.last_message_id = m.message_id
        with contextlib.suppress(Exception):
            if old_msg_id and old_msg_id != user.last_message_id:
                await safe_delete_message(bot, user.tg_user_id, old_msg_id)
    # если не отправили вообще ничего — оставляем старое сообщение нетронутым


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
    if u.startswith("http://t.me/"):
        return "https://" + u[len("http://"):]
    if u.startswith("https://t.me/") or u.startswith("t.me/") or "t.me/" in u:
        return "https://" + u.lstrip("/").lstrip("https://").lstrip("http://")
    if u.startswith("http://") or u.startswith("https://"):
        return u
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
    # Собирается в render_lang_screen (оставляем заглушку)
    raise RuntimeError("kb_lang is built inside render_lang_screen now")

def kb_subscribe(locale: str, channel_url: str,
                 btn_go_channel: str, btn_ive_subscribed: str, btn_main_text: str) -> InlineKeyboardMarkup:
    _, open_url = parse_channel_field(channel_url or "")
    if not open_url:
        open_url = "https://t.me"  # безопасный дефолт

    rows = [
        [InlineKeyboardButton(text=btn_go_channel, url=open_url)],
        [InlineKeyboardButton(text=btn_ive_subscribed, callback_data="menu:subcheck")],
        [InlineKeyboardButton(text=btn_main_text, callback_data="menu:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --------------------------- РЕНДЕР ЭКРАНОВ: UI ---------------------------
async def render_lang_screen(bot: Bot, tenant: Tenant, user: User, current_lang: Optional[str]):
    """
    Первый запуск: показываем выбор языка.
    Все последующие /start при уже заданном user.lang → сразу главное меню (не язык).
    """
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant

        # локаль экрана (в каком языке показывать текст и подписи)
        locale = (current_lang or tenant.lang_default or "ru").lower()

        # Текст/картинка экрана
        text, img = tget(db, tenant.id, "lang", locale, default_text("lang", locale))

        # Текст кнопки "Главное меню"
        btn_main = tget_label(db, tenant.id, "btn_main", locale)

        # Подписи языков (кастомизируемые через Контент)
        label_ru = tget_label(db, tenant.id, "lang_label_ru", locale)
        label_en = tget_label(db, tenant.id, "lang_label_en", locale)
        label_hi = tget_label(db, tenant.id, "lang_label_hi", locale)
        label_es = tget_label(db, tenant.id, "lang_label_es", locale)

        # Отмечаем текущий выбранный язык галочкой (если есть)
        ru = ("✅ " if current_lang == "ru" else "") + label_ru
        en = ("✅ " if current_lang == "en" else "") + label_en
        hi = ("✅ " if current_lang == "hi" else "") + label_hi
        es = ("✅ " if current_lang == "es" else "") + label_es

        # Собираем клавиатуру прямо тут
        rm = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=ru, callback_data="lang:ru"),
             InlineKeyboardButton(text=en, callback_data="lang:en")],
            [InlineKeyboardButton(text=hi, callback_data="lang:hi"),
             InlineKeyboardButton(text=es, callback_data="lang:es")],
            [InlineKeyboardButton(text=btn_main, callback_data="menu:main")],
        ])

        # Показываем экран (send_screen сам удалит предыдущее сообщение)
        await send_screen(bot, user, "lang", locale, text, rm, img)
        db.commit()
    finally:
        db.close()


async def render_main(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant

        locale = (user.lang or tenant.lang_default or "ru").lower()
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
        locale = (user.lang or tenant.lang_default or "ru").lower()
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
        locale = (user.lang or tenant.lang_default or "ru").lower()
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
    """
    Экран «Получить сигнал»:
    - последовательность: подписка (если включена) → регистрация (если step=new/asked_reg) → депозит (если включен)
    - доступ открыт: один раз показываем «unlocked», потом — просто главное меню (а кнопка ведёт в мини-апп).
    - VIP уведомление при достижении порога: удаляем предыдущее сообщение и отправляем уведомление (единожды), затем выходим.
    """
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant
        locale = (user.lang or tenant.lang_default or "ru").lower()
        cfg = get_cfg(db, tenant.id)

        # 0) Подписка
        if getattr(cfg, "require_subscription", False):
            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                await render_subscribe(bot, tenant, user)
                db.commit()
                return

        # VIP-инфо по порогу (уведомляем один раз, чисто)
        try:
            dep_total = get_deposit_total(db, tenant.id, user)
            thr = int(getattr(cfg, "vip_threshold", 500) or 500)
            if dep_total >= thr and not getattr(user, "vip_notified", False):
                msg_txt = (
                    "🎉 Поздравляем! Вам доступен премиум-бот. Напишите в поддержку для подключения."
                    if locale == "ru" else
                    "🎉 Congrats! You’re eligible for the premium bot. Please contact support to get access."
                )

                # удаляем прошлое сообщение и отправляем уведомление (сохраняем last_message_id)
                await safe_delete_message(bot, user.tg_user_id, getattr(user, "last_message_id", None))
                kb_support = None
                fresh_tenant = tenant  # уже свежий
                if fresh_tenant.support_url:
                    kb_support = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=("🆘 Поддержка" if locale == "ru" else "🆘 Support"),
                                              url=_normalize_support_url(fresh_tenant.support_url) or fresh_tenant.support_url)]
                    ])
                m = await bot.send_message(user.tg_user_id, msg_txt, reply_markup=kb_support)
                user.last_message_id = m.message_id
                user.vip_notified = True
                db.commit()
                return  # только уведомление
        except Exception as e:
            print(f"[vip-notify] {e}")

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

            # Уже уведомляли — просто главное меню (кнопка «Получить сигнал» теперь открывает мини-апп)
            await render_main(bot, tenant, user)
            db.commit()
            return

        # Шаг 1 — Регистрация
        if user.step in (UserStep.new, UserStep.asked_reg):
            text, img = tget(db, tenant.id, "step1", locale, default_text("step1", locale))
            url = f"{settings.service_host}/pocketoption/reg?tenant_id={tenant.id}&uid={user.tg_user_id}"
            btn_main = tget_label(db, tenant.id, "btn_main", locale)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=("🟢  Зарегистрироваться" if locale == "ru" else
                                                "🟢  Register"), url=url)],
                    [InlineKeyboardButton(text=btn_main, callback_data="menu:main")],
                ]
            )
            user.step = UserStep.asked_reg
            await send_screen(bot, user, "step1", locale, text, kb, img)
            db.commit()
            return

        # Шаг 2 — Депозит (если обязателен)
        text, img = tget(db, tenant.id, "step2", locale, default_text("step2", locale))
        dep_total = get_deposit_total(db, tenant.id, user)
        left = 0
        if cfg.require_deposit:
            text = text.replace("{{min_dep}}", str(cfg.min_deposit))
            left = max(0, cfg.min_deposit - dep_total)
        else:
            text = text.replace("{{min_dep}}", str(cfg.min_deposit))

        progress_line = (
            f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${left})"
            if locale == "ru"
            else f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${left})"
        )
        text = text + progress_line

        url = f"{settings.service_host}/pocketoption/dep?tenant_id={tenant.id}&uid={user.tg_user_id}"
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

    # NEW: поиск
    search_wait_query = State()  # TG ID / @username / TraderID


# Какие кнопки относятся к какому экрану (для удобного редактирования)
SCREEN_BUTTONS = {
    "lang": ["btn_main", "lang_label_ru", "lang_label_en", "lang_label_hi", "lang_label_es"],
    "main": ["btn_instruction", "btn_support", "btn_change_lang", "btn_get_signal"],
    "guide": ["btn_main"],
    "subscribe": ["btn_go_channel", "btn_ive_subscribed", "btn_main"],
    "step1": ["btn_main"],
    "step2": ["btn_main"],
    "unlocked": ["btn_get_signal", "btn_main"],
}

# ----------------------------- АДМИН КНОПКИ/МЕНЮ -----------------------------
def kb_admin_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Пользователи", callback_data="adm:people")],           # широкая
            [InlineKeyboardButton(text="🧷 Настройка постбэков", callback_data="adm:pb")],        # широкая (новая)
            [
                InlineKeyboardButton(text="🧩 Контент", callback_data="adm:content"),
                InlineKeyboardButton(text="🔗 Ссылки", callback_data="adm:links"),
            ],
            [
                InlineKeyboardButton(text="⚙️ Параметры", callback_data="adm:params"),
                InlineKeyboardButton(text="📣 Рассылка", callback_data="adm:broadcast"),
            ],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],              # широкая
        ]
    )



def kb_people_hub():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Поиск (TG/@username/TraderID)", callback_data="adm:people:search")],
            [
                InlineKeyboardButton(text="👥 Ваши юзеры", callback_data="adm:users"),
                InlineKeyboardButton(text="👑 Кандидаты PLATINUM", callback_data="adm:vip:list"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )



def kb_admin_links():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить Support URL", callback_data="adm:set:support"),
             InlineKeyboardButton(text="✏️ Изменить Реф. ссылку", callback_data="adm:set:ref")],
            [InlineKeyboardButton(text="✏️ Изменить ссылку депозита", callback_data="adm:set:dep"),
             InlineKeyboardButton(text="✏️ Изменить Web-app URL", callback_data="adm:set:miniapp")],
            [InlineKeyboardButton(text="✏️ Изменить ссылку канала", callback_data="adm:set:channel")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )


def kb_content_lang():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🇷🇺 RU", callback_data="adm:cl:ru"),
             InlineKeyboardButton(text="🇬🇧 EN", callback_data="adm:cl:en"),
             InlineKeyboardButton(text="🇮🇳 HI", callback_data="adm:cl:hi"),
             InlineKeyboardButton(text="🇪🇸 ES", callback_data="adm:cl:es")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )


def kb_content_keys(locale: str):
    # Показываем только экраны (без отдельных кнопок)
    screen_keys = ["lang", "main", "guide", "subscribe", "step1", "step2", "unlocked"]
    rows = []
    # выводим 2 колонки
    for i in range(0, len(screen_keys), 2):
        chunk = screen_keys[i:i+2]
        row = [InlineKeyboardButton(text=f"• {key_title(k, locale)}", callback_data=f"adm:cks:{k}:{locale}")
               for k in chunk]
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:content")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_content_edit(key: str, locale: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Текст экрана", callback_data=f"adm:ce:text:{key}:{locale}"),
             InlineKeyboardButton(text="🖼 Картинка экрана", callback_data=f"adm:ce:photo:{key}:{locale}")],
            [InlineKeyboardButton(text="🗑 Удалить картинку", callback_data=f"adm:ce:delphoto:{key}:{locale}"),
             InlineKeyboardButton(text="🔄 Сбросить к дефолту", callback_data=f"adm:ce:reset:{key}:{locale}")],
            [InlineKeyboardButton(text="👀 Предпросмотр", callback_data=f"adm:ce:preview:{key}:{locale}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"adm:cl:{locale}")],
        ]
    )


def kb_content_screen(key: str, locale: str):
    # Кнопки редактирования самого экрана
    rows = [
        [InlineKeyboardButton(text="📝 Текст экрана", callback_data=f"adm:ce:text:{key}:{locale}"),
         InlineKeyboardButton(text="🖼 Картинка экрана", callback_data=f"adm:ce:photo:{key}:{locale}")],
        [InlineKeyboardButton(text="🗑 Удалить картинку", callback_data=f"adm:ce:delphoto:{key}:{locale}"),
         InlineKeyboardButton(text="🔄 Сбросить к дефолту", callback_data=f"adm:ce:reset:{key}:{locale}")],
        [InlineKeyboardButton(text="👀 Предпросмотр", callback_data=f"adm:ce:preview:{key}:{locale}")],
    ]

    # Раздел кнопок этого экрана
    btns = SCREEN_BUTTONS.get(key, [])
    if btns:
        rows.append([InlineKeyboardButton(text="—", callback_data="adm:noop")])  # разделитель
        # выводим кнопки в 2 колонки
        for i in range(0, len(btns), 2):
            chunk = btns[i:i+2]
            rows.append([InlineKeyboardButton(text=f"🔤 {key_title(b, locale)}", callback_data=f"adm:ce:text:{b}:{locale}")
                         for b in chunk])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"adm:cl:{locale}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_params(cfg: TenantConfig):
    req_sub = bool(getattr(cfg, "require_subscription", False))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=("✅ Проверять подписку" if req_sub else "❌ Не проверять подписку"),
                callback_data="adm:param:toggle_sub")],
            [InlineKeyboardButton(
                text=("✅ Проверять депозит" if cfg.require_deposit else "❌ Не проверять депозит"),
                callback_data="adm:param:toggle_dep")],
            [InlineKeyboardButton(text=f"💵 Минимальный депозит: ${cfg.min_deposit}",
                                  callback_data="adm:param:set_min")],
            [InlineKeyboardButton(text="👑 Порог PLATINUM", callback_data="adm:vip:thr"),
             InlineKeyboardButton(text="↩️ Стоковая Web-app", callback_data="adm:param:stock_miniapp")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")],
        ]
    )


def kb_broadcast_segments():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Все пользователи", callback_data="adm:bs:all"),
             InlineKeyboardButton(text="📝 Зарегистрированные", callback_data="adm:bs:registered")],
            [InlineKeyboardButton(text="💰 С депозитом", callback_data="adm:bs:deposited")],
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

# Админ: карточка пользователя (рендер)
async def _render_admin_user_card(cb: CallbackQuery, tenant: Tenant, uid: int):
    db = SessionLocal()
    try:
        u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
        if not u:
            await cb.answer("Пользователь не найден"); return
        dep_total = get_deposit_total(db, tenant.id, u)
        cfg = get_cfg(db, tenant.id)
        has_access = dep_total >= cfg.min_deposit if cfg.require_deposit else (u.step >= UserStep.registered)
        upd = getattr(u, "updated_at", None)
        upd_str = upd.strftime("%Y-%m-%d %H:%M:%S") if upd else "—"
        pb_reg = db.query(Postback).filter(
            Postback.tenant_id == tenant.id,
            Postback.event == "registration",
            Postback.click_id == str(u.tg_user_id),
            Postback.token_ok.is_(True)
        ).order_by(Postback.created_at.asc()).first()
        trader_id = pb_reg.trader_id if pb_reg else (u.trader_id or "—")
    finally:
        db.close()

    uname = await _try_username(cb.message.bot, uid)
    locale = u.lang or (get_fresh_tenant(SessionLocal(), tenant.id) or tenant).lang_default or "ru"
    access_emoji = "✅" if has_access else "❌"
    vip_flag = "✅" if getattr(u, "is_vip", False) else "❌"

    txt = (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"TG ID: <code>{uid}</code>\n"
        f"Trader ID: <code>{trader_id}</code>\n"
        f"Username: {uname}\n"
        f"Язык: <code>{locale}</code>\n"
        f"Статус: <b>{u.step}</b>  |  Доступ: {access_emoji}\n"
        f"PLATINUM: {vip_flag}\n"
        f"Депозиты: <b>${dep_total}</b>\n"
        f"Обновлён: {upd_str}\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧾 Регистрация", callback_data=f"adm:vip:do:reg:{uid}"),
            InlineKeyboardButton(text="💳 Депозит", callback_data=f"adm:vip:do:dep:{uid}"),
        ],
        [
            InlineKeyboardButton(text="👑 ВКЛ PLATINUM", callback_data=f"adm:vip:env_on:{uid}"),
            InlineKeyboardButton(text="❌ ВЫКЛ PLATINUM", callback_data=f"adm:vip:env_off:{uid}"),
        ],
        [
            InlineKeyboardButton(text="⬅️ К списку рефов", callback_data="adm:users"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="adm:menu"),
        ],
    ])
    await _safe_edit_msg(cb, txt, kb)


# --- VIP список: пагинация по 10 ---
def _vip_list_build_rows(cands, page: int, page_cb_prefix: str):
    pages = max(1, ceil(len(cands) / USERS_PER_PAGE))
    page = max(1, min(page, pages))
    start = (page - 1) * USERS_PER_PAGE
    chunk = cands[start:start + USERS_PER_PAGE]

    # Делаем кандидатов кликабельными -> открываем карточку пользователя
    rows = [[InlineKeyboardButton(text=f"{flag} {tg_id} — ${total}", callback_data=f"adm:user:{tg_id}")]
            for tg_id, total, flag in chunk]

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"{page_cb_prefix}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"Стр. {page}/{pages}", callback_data="adm:noop"))
    if page < pages:
        nav.append(InlineKeyboardButton(text="Вперёд»", callback_data=f"{page_cb_prefix}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:people")])  # назад в «Пользователи»
    return rows, pages



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

            # Правило: первый запуск → экран языка; все последующие /start → только главное меню
            if user.lang:
                await render_main(bot, tenant, user)
                return

            await render_lang_screen(bot, tenant, user, current_lang=None)
        finally:
            db.close()

    @r.message(Command("subdebug"))
    async def subdebug(msg: Message):
        if msg.from_user.id != tenant.owner_tg_id:
            await msg.answer("⛔️ Нет доступа")
            return
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == msg.from_user.id
            ).first()
            if not user:
                await msg.answer("user not found");
                return

            t = get_fresh_tenant(db, tenant.id) or tenant
            ident, open_url = parse_channel_field(t.channel_url or "")

            chat_id = ident
            if isinstance(ident, str):
                s = ident.strip()
                if s.startswith("-100") and s[1:].isdigit():
                    with contextlib.suppress(Exception):
                        chat_id = int(s)

            try:
                m = await msg.bot.get_chat_member(chat_id, user.tg_user_id)
                status_text = f"OK status={getattr(m, 'status', None)!r}"
            except Exception as e:
                status_text = f"ERR {type(e).__name__}: {e}"

            await msg.answer(
                "🔎 <b>Диагностика подписки</b>\n"
                f"channel_url: <code>{t.channel_url or ''}</code>\n"
                f"ident: <code>{ident or ''}</code>\n"
                f"chat_id(type): <code>{repr(chat_id)} ({type(chat_id).__name__})</code>\n"
                f"open_url: <code>{open_url or ''}</code>\n"
                f"get_chat_member: <code>{status_text}</code>\n",
                disable_web_page_preview=True
            )
        finally:
            db.close()

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
            if locale not in ("ru", "en", "hi", "es"):
                await cb.answer()
                return

            user.lang = locale
            db.commit()

            # авто-удаляем кликнутое сообщение (чистый чат)
            with contextlib.suppress(Exception):
                await safe_delete_message(bot, cb.from_user.id, getattr(cb.message, "message_id", None))

            await render_main(bot, tenant, user)
            await cb.answer("Сохранено" if locale == "ru" else "Saved")

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

            # Если язык не выбран (первый запуск и клик по "Главное меню") — ставим дефолт
            if not user.lang:
                user.lang = (tenant.lang_default or "ru").lower()
                db.commit()

            # Сначала рендерим новый экран, потом удаляем старое сообщение —
            # чтобы не остаться без экрана при любой ошибке отправки
            await render_main(cb.message.bot, tenant, user)

            with contextlib.suppress(Exception):
                await safe_delete_message(cb.message.bot, cb.from_user.id, getattr(cb.message, "message_id", None))

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

            with contextlib.suppress(Exception):
                await safe_delete_message(bot, cb.from_user.id, getattr(cb.message, "message_id", None))

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

            with contextlib.suppress(Exception):
                await safe_delete_message(bot, cb.from_user.id, getattr(cb.message, "message_id", None))

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

            with contextlib.suppress(Exception):
                await safe_delete_message(bot, cb.from_user.id, getattr(cb.message, "message_id", None))

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

            with contextlib.suppress(Exception):
                await safe_delete_message(bot, cb.from_user.id, getattr(cb.message, "message_id", None))

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
        await msg.answer("<b>Панель администратора</b>", reply_markup=kb_admin_main())

    @r.callback_query(
        lambda c: (
                c.data in {
            "adm:menu", "adm:people", "adm:links", "adm:content", "adm:broadcast",
            "adm:stats", "adm:params", "adm:pb"
        }
                or (c.data or "").startswith("adm:set:")
                or (c.data or "").startswith("adm:cl:")
                or (c.data or "").startswith("adm:cks:")
                or (c.data or "").startswith("adm:ck:")
                or (c.data or "").startswith("adm:ce:")
                or (c.data or "").startswith("adm:param:")
                or (c.data or "").startswith("adm:bs:")
                or (c.data or "").startswith("adm:bc:")
                or (c.data or "").startswith("adm:users")
                or (c.data or "").startswith("adm:user:")
                or (c.data or "").startswith("adm:people:")
                or (
                        (c.data or "").startswith("adm:vip:")
                        and not (c.data or "").startswith("adm:vip:do:")
                        and not (c.data or "").startswith("adm:vip:env_")
                )
                or (c.data or "").startswith("adm:noop")
        )
    )
    async def admin_router(cb: CallbackQuery, state: FSMContext):
        def _is_owner() -> bool:
            return cb.from_user.id == tenant.owner_tg_id

        if not _is_owner():
            await cb.answer()
            return

        data = cb.data or ""

        # ----- Главное меню
        if data == "adm:menu":
            await state.clear()
            await _safe_edit_msg(cb, "<b>Панель администратора</b>", kb_admin_main())
            await cb.answer(); return

        # ----- People Hub (пользователи)
        if data == "adm:people":
            await state.clear()
            await _safe_edit_msg(cb, "👤 <b>Пользователи</b>", kb_people_hub())
            await cb.answer(); return

        if data == "adm:people:search":
            await state.set_state(AdminForm.search_wait_query)
            await cb.message.edit_text("Введите: TG ID, @username или TraderID", reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:people")]]
            ))
            await cb.answer();
            return

        # ----- Ссылки (редактирование)
        if data == "adm:links":
            await state.clear()
            await _safe_edit_msg(cb, "🔗 Ссылки", kb_admin_links())
            await cb.answer(); return

        # ----- Контент
        if data == "adm:content":
            await state.clear()
            await _safe_edit_msg(cb, "🧩 Контент: выберите язык", kb_content_lang())
            await cb.answer(); return

        # ----- Параметры
        if data == "adm:params":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()
            await _safe_edit_msg(cb, "⚙️ Параметры", kb_params(cfg))
            await cb.answer(); return

        # ----- Статистика
        if data == "adm:stats":
            db = SessionLocal()
            try:
                total = db.query(User).filter(User.tenant_id == tenant.id).count()
                reg = db.query(User).filter(User.tenant_id == tenant.id, User.step >= UserStep.registered).count()
                dep = db.query(User).filter(User.tenant_id == tenant.id, User.step == UserStep.deposited).count()
            finally:
                db.close()
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:menu")]])
            await _safe_edit_msg(cb, f"📊 <b>Статистика</b>\n👥 Всего: {total}\n📝 Зарегистрировались: {reg}\n💰 С депозитом: {dep}", kb)
            await cb.answer(); return

        # ----- Постбэки (URL инструкции)
        if data == "adm:pb":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
            finally:
                db.close()

            secret = tenant.postback_secret or settings.global_postback_secret
            base = settings.service_host.rstrip("/")

            reg = f"{base}/pb?tenant_id={tenant.id}&event=registration&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}"
            dep = f"{base}/pb?tenant_id={tenant.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"
            redep = f"{base}/pb?tenant_id={tenant.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"

            txt = (
                "<b>Постбэки Pocket Option</b>\n\n"
                "📝 <b>Регистрация</b>\n"
                f"<code>{reg}</code>\n"
                "Макросы в PP (1-в-1):\n"
                "• click_id → <code>click_id</code>\n"
                "• trader_id → <code>trader_id</code>\n\n"
            )
            if cfg.require_deposit:
                txt += (
                    "💳 <b>Первый депозит</b>\n"
                    f"<code>{dep}</code>\n"
                    "Макросы в PP (1-в-1):\n"
                    "• click_id → <code>click_id</code>\n"
                    "• trader_id → <code>trader_id</code>\n"
                    "• sumdep → <code>sumdep</code>\n\n"
                    "💵 <b>Повторный депозит</b>\n"
                    f"<code>{redep}</code>\n"
                    "Макросы в PP (1-в-1):\n"
                    "• click_id → <code>click_id</code>\n"
                    "• trader_id → <code>trader_id</code>\n"
                    "• sumdep → <code>sumdep</code>\n\n"
                    f"⚠️ Минимальный депозит: ${cfg.min_deposit}."
                )
            else:
                txt += "ℹ️ Для этого бота проверка депозита отключена."

            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:links")]])
            await _safe_edit_msg(cb, txt, kb)
            await cb.answer(); return

        # ----- LINKS input (сеттеры)
        if data == "adm:set:support":
            await state.set_state(AdminForm.waiting_support)
            await _safe_edit_msg(cb, "Пришлите <b>новый Support URL</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if data == "adm:set:ref":
            await state.set_state(AdminForm.waiting_ref)
            await _safe_edit_msg(cb, "Пришлите <b>новую реферальную ссылку</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if data == "adm:set:dep":
            await state.set_state(AdminForm.waiting_dep)
            await _safe_edit_msg(cb, "Пришлите <b>ссылку для депозита</b> одним сообщением.\n\n⬅️ /admin — отмена.")
            await cb.answer(); return

        if data == "adm:set:miniapp":
            await state.set_state(AdminForm.waiting_miniapp)
            await _safe_edit_msg(cb,
                "Пришлите <b>Web-app URL</b> одним сообщением.\n"
                "Совет: выложите мини-апп на GitHub Pages и пришлите HTTPS ссылку.\n\n⬅️ /admin — отмена."
            )
            await cb.answer(); return

        if data == "adm:set:channel":
            await state.set_state(AdminForm.waiting_channel)
            await _safe_edit_msg(cb,
                "Если у вас <b>публичный канал</b> — отправьте <code>@username</code> или ссылку на канал/группу "
                "(например: https://t.me/username).\n\n"
                "Если у вас <b>приватный канал</b> — отправьте ID канала и инвайт-ссылку в формате:\n"
                "<code>-1001234567890 | https://t.me/+invite</code>\n\n"
                "⚠️ Бот должен быть участником (в канале — админом)."
            )
            await cb.answer(); return

        # ----- PARAMS toggles
        if data == "adm:param:toggle_dep":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                cfg.require_deposit = not cfg.require_deposit
                db.commit()
            finally:
                db.close()
            await _safe_edit_msg(cb, "⚙️ Параметры", kb_params(cfg))
            await cb.answer("Сохранено"); return

        if data == "adm:param:toggle_sub":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                cfg.require_subscription = not bool(getattr(cfg, "require_subscription", False))
                db.commit()
            finally:
                db.close()
            await _safe_edit_msg(cb, "⚙️ Параметры", kb_params(cfg))
            await cb.answer("Сохранено"); return

        if data == "adm:param:set_min":
            await state.set_state(AdminForm.params_wait_min_dep)
            await _safe_edit_msg(cb, "Введи минимальную сумму депозита в $ (целое число).")
            await cb.answer(); return

        if data == "adm:param:stock_miniapp":
            db = SessionLocal()
            try:
                t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
                t.miniapp_url = None
                db.commit()
            finally:
                db.close()
            await _safe_edit_msg(cb, "✅ Вернул стоковую мини-апп (из ENV).", kb_admin_main())
            await cb.answer(); return

        # ----- CONTENT flow
        if data.startswith("adm:cl:"):
            lang = data.split(":")[2]
            await state.update_data(content_lang=lang)
            await state.set_state(AdminForm.content_wait_key)
            await _safe_edit_msg(cb, "🧩 Контент: выберите экран", kb_content_keys(lang))
            await cb.answer(); return

        if data.startswith("adm:cks:"):
            _, _, screen_key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=screen_key)
            db = SessionLocal()
            try:
                summary = editor_status_text(db, tenant.id, screen_key, lang)
            finally:
                db.close()
            await _safe_edit_msg(cb, summary, kb_content_screen(screen_key, lang))
            await cb.answer(); return

        if data.startswith("adm:ck:"):
            _, _, key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=key)
            db = SessionLocal()
            try:
                summary = editor_status_text(db, tenant.id, key, lang)
            finally:
                db.close()
            await _safe_edit_msg(cb, summary, kb_content_edit(key, lang))
            await cb.answer(); return

        if data.startswith("adm:ce:text:"):
            _, _, _, key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=key)
            await state.set_state(AdminForm.content_wait_text)
            await _safe_edit_msg(cb, f"Пришлите <b>новый текст</b> для «{key_title(key, lang)}» ({lang}) одним сообщением.")
            await cb.answer(); return

        if data.startswith("adm:ce:photo:"):
            _, _, _, key, lang = data.split(":")
            await state.update_data(content_lang=lang, content_key=key)
            await state.set_state(AdminForm.content_wait_photo)
            await _safe_edit_msg(cb, f"Пришлите <b>фото</b> для «{key_title(key, lang)}» ({lang}).")
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
            await _safe_edit_msg(cb, msg, kb_content_edit(key, lang))
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
            await _safe_edit_msg(cb, f"🔄 Сброшено к дефолту для «{key_title(key, lang)}» ({lang}).", kb_content_edit(key, lang))
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

        # ---- Ваши рефы: вход в список (стр.1)
        if data == "adm:users":
            page = 1
            db = SessionLocal()
            try:
                q = db.query(User).filter(User.tenant_id == tenant.id)
                total = q.count()
                users = q.order_by(User.updated_at.desc().nullslast()).offset((page - 1) * USERS_PER_PAGE).limit(
                    USERS_PER_PAGE).all()
            finally:
                db.close()

            rows = []
            for u in users:
                uname = await _try_username(cb.message.bot, u.tg_user_id) if u.tg_user_id else "—"
                title = f"{u.tg_user_id or '—'} • {uname}"
                rows.append([InlineKeyboardButton(text=title, callback_data=f"adm:user:{u.tg_user_id}")])

            nav = []
            if total > page * USERS_PER_PAGE:
                nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"adm:users:page:{page + 1}"))
            if nav:
                rows.append(nav)
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:people")])

            text = f"👥 <b>Ваши рефы</b>\nВсего: <b>{total}</b>\nСтр. {page}"
            await _safe_edit_msg(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
            await cb.answer(); return

        # ---- Ваши рефы: пагинация
        if data.startswith("adm:users:page:"):
            page = int(data.split(":")[3])
            if page < 1: page = 1
            db = SessionLocal()
            try:
                q = db.query(User).filter(User.tenant_id == tenant.id)
                total = q.count()
                users = q.order_by(User.updated_at.desc().nullslast()).offset((page - 1) * USERS_PER_PAGE).limit(
                    USERS_PER_PAGE).all()
            finally:
                db.close()

            rows = []
            for u in users:
                uname = await _try_username(cb.message.bot, u.tg_user_id) if u.tg_user_id else "—"
                title = f"{u.tg_user_id or '—'} • {uname}"
                rows.append([InlineKeyboardButton(text=title, callback_data=f"adm:user:{u.tg_user_id}")])

            nav = []
            if page > 1:
                nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"adm:users:page:{page - 1}"))
            if total > page * USERS_PER_PAGE:
                nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"adm:users:page:{page + 1}"))
            if nav:
                rows.append(nav)
            rows.append([InlineKeyboardButton(text="⬅️ Меню пользователей", callback_data="adm:people")])

            text = f"👥 <b>Ваши рефы</b>\nВсего: <b>{total}</b>\nСтр. {page}"
            await _safe_edit_msg(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
            await cb.answer(); return

        # ---- Карточка пользователя
        if data.startswith("adm:user:"):
            # аккуратно парсим uid
            try:
                uid = int(data.split(":")[2])
            except Exception:
                await cb.answer("Некорректный ID")
                return

            await _render_admin_user_card(cb, tenant, uid)
            await cb.answer()
            return

            db = SessionLocal()
            try:
                u = db.query(User).filter(
                    User.tenant_id == tenant.id,
                    User.tg_user_id == uid
                ).first()
                if not u:
                    await cb.answer("Пользователь не найден")
                    return

                # сумма депозитов
                dep_total = get_deposit_total(db, tenant.id, u)

                # статус по шагам
                step_map = {
                    UserStep.new: "new",
                    UserStep.asked_reg: "asked_reg",
                    UserStep.registered: "registered",
                    UserStep.asked_deposit: "asked_deposit",
                    UserStep.deposited: "deposited",
                }
                step = step_map.get(u.step, str(u.step))

                # доступ (учёт require_deposit)
                cfg = get_cfg(db, tenant.id)
                has_access = dep_total >= cfg.min_deposit if cfg.require_deposit else (u.step >= UserStep.registered)

                # дата обновления
                upd = getattr(u, "updated_at", None)
                upd_str = upd.strftime("%Y-%m-%d %H:%M:%S") if upd else "—"

                # Trader ID (из первого postback регистрации)
                pb_reg = db.query(Postback).filter(
                    Postback.tenant_id == tenant.id,
                    Postback.event == "registration",
                    Postback.click_id == str(u.tg_user_id),
                    Postback.token_ok.is_(True)
                ).order_by(Postback.created_at.asc()).first()
                trader_id = pb_reg.trader_id if pb_reg else (u.trader_id or "—")

            finally:
                db.close()

            uname = await _try_username(cb.message.bot, uid)
            locale = u.lang or (get_fresh_tenant(SessionLocal(), tenant.id) or tenant).lang_default or "ru"
            access_emoji = "✅" if has_access else "❌"
            vip_flag = "✅" if getattr(u, "is_vip", False) else "❌"

            txt = (
                f"👤 <b>Профиль пользователя</b>\n"
                f"TG ID: <code>{uid}</code>\n"
                f"Trader ID: <code>{trader_id}</code>\n"
                f"Username: {uname}\n"
                f"Язык: <code>{locale}</code>\n"
                f"Статус: <b>{step}</b>  |  Доступ: {access_emoji}\n"
                f"PLATINUM: {vip_flag}\n"
                f"Депозиты: <b>${dep_total}</b>\n"
                f"Обновлён: {upd_str}\n"
            )

            # --- КНОПКИ ДЕЙСТВИЙ В КАРТОЧКЕ ---
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="🧾 Ручная рега", callback_data=f"adm:vip:do:reg:{uid}"),
                    InlineKeyboardButton(text="💳 Ручной деп", callback_data=f"adm:vip:do:dep:{uid}"),
                ],
                [
                    InlineKeyboardButton(text="👑 Вкл PREM", callback_data=f"adm:vip:miniapp:set:{uid}"),
                    InlineKeyboardButton(text="❌ Выкл PREM", callback_data=f"adm:vip:unset:{uid}"),
                ],
                [
                    InlineKeyboardButton(text="↩️ Обычная мини-апп", callback_data=f"adm:vip:miniapp:stock:{uid}"),
                ],
                [
                    InlineKeyboardButton(text="⬅️ К списку рефов", callback_data="adm:users"),
                    InlineKeyboardButton(text="🏠 Главное меню", callback_data="adm:menu"),
                ],
            ])

            await _safe_edit_msg(cb, txt, kb)
            await cb.answer(); return

        # ----- VIP список (кандидаты) + пагинация
        if data == "adm:vip:list":
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                thr = int(cfg.vip_threshold or 500)
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                cands = []
                for u in users:
                    total = get_deposit_total(db, tenant.id, u)
                    if total >= thr:
                        cands.append((u.tg_user_id, total, "✅" if u.is_vip else "❌"))
                cands.sort(key=lambda x: -x[1])
            finally:
                db.close()

            page = 1
            rows, pages = _vip_list_build_rows(cands, 1, "adm:vip:list:page")
            txt = f"<b>Кандидаты PLATINUM (≥ ${thr}):</b>\nВсего: {len(cands)}"
            await cb.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),disable_web_page_preview=True)
            await cb.answer(); return

        if data.startswith("adm:vip:list:page:"):
            page = int(data.split(":")[-1])
            db = SessionLocal()
            try:
                cfg = get_cfg(db, tenant.id)
                thr = int(cfg.vip_threshold or 500)
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                cands = []
                for u in users:
                    total = get_deposit_total(db, tenant.id, u)
                    if total >= thr:
                        cands.append((u.tg_user_id, total, "✅" if u.is_vip else "❌"))
                cands.sort(key=lambda x: -x[1])
            finally:
                db.close()

            rows, pages = _vip_list_build_rows(cands, max(1, page), "adm:vip:list:page")
            txt = f"<b>Кандидаты PLATINUM (≥ ${thr}):</b>\nВсего: {len(cands)}"
            await _safe_edit_msg(cb, txt, InlineKeyboardMarkup(inline_keyboard=rows))
            await cb.answer(); return

        # ----- VIP: порог
        if data == "adm:vip:thr":
            await state.set_state(AdminForm.vip_wait_threshold)
            await _safe_edit_msg(cb, "Пришлите новое значение порога PLATINUM (целое число, $).")
            await cb.answer(); return

        # ----- VIP: выдача/мини-аппа — входы (кнопки есть в People Hub)
        if data == "adm:vip:grant":
            db = SessionLocal()
            try:
                users = db.query(User).filter(User.tenant_id == tenant.id).all()
                rows = []
                for u in users[:50]:
                    rows.append([InlineKeyboardButton(text=str(u.tg_user_id), callback_data=f"adm:vip:set:{u.tg_user_id}")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="Нет пользователей", callback_data="adm:people")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:people")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await _safe_edit_msg(cb, "Выберите пользователя для ВЫДАЧИ PLATINUM:", kb)
            await cb.answer(); return

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
                        rows.append([InlineKeyboardButton(text=label, callback_data=f"adm:vip:miniapp:set:{u.tg_user_id}")])
                rows = rows[:50] if rows else [[InlineKeyboardButton(text="Пока нет пользователей с доступом", callback_data="adm:people")]]
                rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:people")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
            finally:
                db.close()
            await _safe_edit_msg(cb, "Выберите пользователя для изменения PLATINUM мини-аппы:", kb)
            await cb.answer(); return

        # ----- VIP per-user miniapp settings
        if data.startswith("adm:vip:miniapp"):
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
                    [InlineKeyboardButton(text="🟣 Выдать PLATINUM-мини-апп (ENV)", callback_data=f"adm:vip:miniapp:env:{uid}")],
                    [InlineKeyboardButton(text="✏️ Задать кастомный PLATINUM URL", callback_data=f"adm:vip:miniapp:ask:{uid}")],
                    [InlineKeyboardButton(text="↩️ Вернуть обычную мини-апп", callback_data=f"adm:vip:miniapp:stock:{uid}")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:vip:miniapp")],
                ]
                status = []
                if has_vip: status.append("VIP=✅")
                if has_custom: status.append("Custom URL=✅")
                if not status: status.append("обычная мини-апп")
                title = f"Пользователь <code>{uid}</code>\nТекущее: " + ", ".join(status)
                await _safe_edit_msg(cb, title, InlineKeyboardMarkup(inline_keyboard=rows))
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
                    # чистый чат + обновление главного
                    with contextlib.suppress(Exception):
                        await safe_delete_message(bot, uid, getattr(u, "last_message_id", None))
                    await render_main(bot, tenant, u)
                    # уведомление
                    locale = u.lang or (get_fresh_tenant(SessionLocal(), tenant.id) or tenant).lang_default or "ru"
                    m = "🎉 Вам выдан доступ к премиум-боту!" if locale == "ru" else "🎉 You’ve been granted access to the premium bot!"
                    supp = _normalize_support_url((get_fresh_tenant(SessionLocal(), tenant.id) or tenant).support_url)
                    kb_support = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=("🆘 Поддержка" if locale == "ru" else "🆘 Support"), url=supp)]]) if supp else None
                    msg = await bot.send_message(uid, m, reply_markup=kb_support)
                    db2 = SessionLocal()
                    try:
                        u2 = db2.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                        if u2:
                            u2.last_message_id = msg.message_id
                            db2.commit()
                    finally:
                        db2.close()
                finally:
                    db.close()
                await _safe_edit_msg(cb, "✅ Назначена PLATINUM-мини-апп из ENV. Пользователь уже видит её в «Получить сигнал».", kb_admin_main())
                await cb.answer("Готово"); return

            if data.startswith("adm:vip:miniapp:ask:"):
                uid = int(data.rsplit(":", 1)[-1])
                await state.update_data(vip_user_id=uid)
                await state.set_state(AdminForm.vip_wait_miniapp_url)
                await _safe_edit_msg(cb, f"Пришлите PLATINUM Web-app URL для <code>{uid}</code> одним сообщением.\nЧтобы очистить, пришлите «-».")
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
                    with contextlib.suppress(Exception):
                        await safe_delete_message(bot, uid, getattr(u, "last_message_id", None))
                    await render_main(bot, tenant, u)
                finally:
                    db.close()
                await _safe_edit_msg(cb, "↩️ Вернул обычную мини-апп. Теперь «Получить сигнал» открывает не-PLATINUM версию.", kb_admin_main())
                await cb.answer("Готово"); return

        # ----- VIP: быстрое включение/выключение и URL (кнопки есть на карточке)
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
                with contextlib.suppress(Exception):
                    await safe_delete_message(bot, uid, getattr(u, "last_message_id", None))
                await render_main(bot, tenant, u)
            finally:
                db.close()
            try:
                fresh_tenant = get_fresh_tenant(SessionLocal(), tenant.id) or tenant
                locale = u.lang or fresh_tenant.lang_default or "ru"
                text = ("🎉 Вам выдан доступ к премиум-боту! Напишите в поддержку для подключения."
                        if locale == "ru" else
                        "🎉 You’ve been granted access to the premium bot! Contact support to get connected.")
                supp = _normalize_support_url(fresh_tenant.support_url) if fresh_tenant.support_url else None
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=("🆘 Поддержка" if locale == "ru" else "🆘 Support"), url=supp)]]) if supp else None
                msg = await bot.send_message(uid, text, reply_markup=kb)
                dbx = SessionLocal()
                try:
                    ux = dbx.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
                    if ux:
                        ux.last_message_id = msg.message_id
                        dbx.commit()
                finally:
                    dbx.close()
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
                with contextlib.suppress(Exception):
                    await safe_delete_message(bot, uid, getattr(u, "last_message_id", None))
                await render_main(bot, tenant, u)
            finally:
                db.close()
            await cb.answer("PLATINUM выключен"); return

        if data.startswith("adm:vip:url:ask:"):
            uid = int(data.rsplit(":", 1)[-1])
            await state.update_data(vip_user_id=uid)
            await state.set_state(AdminForm.vip_wait_url)
            await _safe_edit_msg(cb, f"Пришлите PLATINUM Web-app URL для <code>{uid}</code> одним сообщением.")
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
                with contextlib.suppress(Exception):
                    await safe_delete_message(bot, uid, getattr(u, "last_message_id", None))
                await render_main(bot, tenant, u)
            finally:
                db.close()
            await cb.answer("URL очищен"); return

        # ----- Рассылка
        if data == "adm:broadcast":
            await state.clear()
            await state.set_state(AdminForm.bcast_wait_segment)
            await _safe_edit_msg(
                cb,
                "📣 <b>Рассылка</b>\nВыберите сегмент получателей:",
                kb_broadcast_segments()
            )
            await cb.answer(); return

        if data.startswith("adm:bs:"):
            seg = data.split(":")[2]
            if seg not in {"all", "registered", "deposited"}:
                seg = "all"

            # Подсчитаем получателей заранее
            db = SessionLocal()
            try:
                q = db.query(User).filter(User.tenant_id == tenant.id)
                if seg == "registered":
                    q = q.filter(User.step >= UserStep.registered)
                elif seg == "deposited":
                    q = q.filter(User.step == UserStep.deposited)
                recipients = q.count()
            finally:
                db.close()

            await state.update_data(bcast_segment=seg, bcast_recipients=recipients)
            await state.set_state(AdminForm.bcast_wait_content)

            if recipients == 0:
                txt = "📣 <b>Рассылка</b>\nВыбранный сегмент пустой. Выберите другой сегмент."
                await _safe_edit_msg(cb, txt, kb_broadcast_segments())
                await cb.answer(); return

            await _safe_edit_msg(
                cb,
                f"📣 <b>Рассылка</b>\nПолучателей: <b>{recipients}</b>\n\n"
                "Пришлите контент (<b>текст/фото/видео/документ</b>).\n"
                "После этого появится кнопка «Запустить».",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:broadcast")]
                ])
            )
            await cb.answer(); return

        # --- Запуск рассылки
        if data == "adm:bc:run":
            data_state = await state.get_data()
            seg = data_state.get("bcast_segment", "all")
            text = data_state.get("bcast_text") or ""
            media_id = data_state.get("bcast_media")
            media_kind = data_state.get("bcast_media_kind")
            src_chat_id = data_state.get("bcast_src_chat")
            src_msg_id = data_state.get("bcast_src_msg")

            # Список получателей
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

            total = len(users)
            if total == 0:
                await cb.message.edit_text(
                    "📣 В выбранном сегменте нет получателей.",
                    reply_markup=kb_broadcast_segments()
                )
                await cb.answer()
                return

            await _safe_edit_msg(
                cb,
                f"📣 Рассылка запущена.\nПолучателей: <b>{total}</b>\n"
                "Скорость: адаптивная (зависит от ограничений Telegram и параллельности)\n\n"
                "Итог по завершении придёт сюда.",
                kb_admin_main()
            )
            await state.clear()

            async def _run_broadcast():
                sent = 0
                failed = 0
                errors: dict[str, int] = {}
                bot_local = cb.message.bot

                # Параллельность: по умолчанию 20, можно задать settings.broadcast_concurrency
                concurrency = int(getattr(settings, "broadcast_concurrency", 20) or 20)
                concurrency = max(1, min(concurrency, 100))
                sem = asyncio.Semaphore(concurrency)

                queue = asyncio.Queue()
                for uid in users:
                    await queue.put(uid)

                async def _send_one(uid: int):
                    nonlocal sent, failed, errors
                    try:
                        if src_chat_id and src_msg_id:
                            await bot_local.copy_message(
                                chat_id=uid,
                                from_chat_id=src_chat_id,
                                message_id=src_msg_id
                            )
                        else:
                            if media_kind == "photo":
                                await bot_local.send_photo(uid, media_id, caption=text or "")
                            elif media_kind == "video":
                                await bot_local.send_video(uid, media_id, caption=text or "")
                            elif media_kind == "document":
                                await bot_local.send_document(uid, media_id, caption=text or "")
                            elif media_kind == "animation":
                                await bot_local.send_animation(uid, media_id, caption=text or "")
                            else:
                                await bot_local.send_message(uid, text or "")
                        sent += 1
                    except TelegramRetryAfter as e:
                        # Телеграм просит подождать → ждём и пробуем ещё раз
                        await asyncio.sleep(float(getattr(e, "retry_after", 1)) + 0.5)
                        return await _send_one(uid)
                    except Exception as e:
                        failed += 1
                        msg = str(e).lower()
                        if "blocked by the user" in msg:
                            key = "blocked_by_user"
                        elif "initiate conversation" in msg or "can't initiate conversation" in msg:
                            key = "no_start_from_user"
                        elif "chat not found" in msg:
                            key = "chat_not_found"
                        elif "user is deactivated" in msg:
                            key = "user_deactivated"
                        else:
                            key = msg[:120]
                        errors[key] = errors.get(key, 0) + 1

                async def _worker():
                    while not queue.empty():
                        uid = await queue.get()
                        async with sem:
                            await _send_one(uid)
                        await asyncio.sleep(0)

                workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
                await asyncio.gather(*workers)

                parts = [f"📣 Рассылка завершена.\nОтправлено: <b>{sent}</b>\nОшибок: <b>{failed}</b>"]
                if failed:
                    mapping = {
                        "blocked_by_user": "пользователь заблокировал бота",
                        "no_start_from_user": "юзер не писал боту",
                        "chat_not_found": "чат не найден (id устарел/невалиден)",
                        "user_deactivated": "аккаунт удалён",
                    }
                    top = sorted(errors.items(), key=lambda x: -x[1])[:5]
                    parts.append("\nПричины ошибок:")
                    for k, n in top:
                        parts.append(f"• {mapping.get(k, k)}: <b>{n}</b>")

                summary = "\n".join(parts)
                with contextlib.suppress(Exception):
                    await bot_local.send_message(tenant.owner_tg_id, summary)

            asyncio.create_task(_run_broadcast())
            await cb.answer()
            return

    # -------------------- MESSAGE HANDLERS (Admin states) --------------------
    @r.message(AdminForm.bcast_wait_content)
    async def bcast_collect(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return

        data = await state.get_data()
        seg = data.get("bcast_segment", "all")
        recipients = int(data.get("bcast_recipients", 0) or 0)

        media_id = None
        media_kind = None
        text = None

        if msg.photo:
            media_id = msg.photo[-1].file_id;
            media_kind = "photo";
            text = msg.caption or ""
        elif msg.video:
            media_id = msg.video.file_id;
            media_kind = "video";
            text = msg.caption or ""
        elif msg.document:
            media_id = msg.document.file_id;
            media_kind = "document";
            text = msg.caption or ""
        elif msg.animation:
            media_id = msg.animation.file_id;
            media_kind = "animation";
            text = msg.caption or ""
        else:
            text = msg.text or ""

        if not (text or media_id):
            await msg.answer("Нужно отправить текст или медиа (фото/видео/документ/GIF). Попробуйте ещё раз.")
            return

        await state.update_data(
            bcast_text=text,
            bcast_media=media_id,
            bcast_media_kind=media_kind,
            bcast_src_chat=msg.chat.id,
            bcast_src_msg=msg.message_id,
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Запустить", callback_data="adm:bc:run")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:menu")],
            ]
        )
        head = f"<b>Предпросмотр рассылки</b>\nСегмент: <code>{seg}</code> • Получателей: <b>{recipients}</b>\n"

        if media_kind == "photo":
            await msg.answer_photo(media_id, caption=head + (text or ""), reply_markup=kb)
        elif media_kind == "video":
            await msg.answer_video(media_id, caption=head + (text or ""), reply_markup=kb)
        elif media_kind == "document":
            await msg.answer_document(media_id, caption=head + (text or ""), reply_markup=kb)
        elif media_kind == "animation":
            await msg.answer_animation(media_id, caption=head + (text or ""), reply_markup=kb)
        else:
            await msg.answer(head + (text or ""), reply_markup=kb)

        await state.set_state(AdminForm.bcast_confirm)

    # Ссылки
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
        raw = (msg.text or "").strip()
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tenant.id).first()
            t.channel_url = raw
            db.commit()
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Ссылка/ID канала сохранены. Добавьте бота в канал (в канале — админ).",
                         reply_markup=kb_admin_main())

    # Контент
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
        await msg.answer(f"✅ Текст сохранён для «{key_title(key, lang)}» ({lang}).",
                         reply_markup=kb_content_edit(key, lang))

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
        await msg.answer(f"✅ Картинка сохранена для «{key_title(key, lang)}» ({lang}).",
                         reply_markup=kb_content_edit(key, lang))

    # VIP / параметры
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
        await msg.answer(f"✅ Порог PLATINUM обновлён: ${val}.", reply_markup=kb_admin_main())

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
            with contextlib.suppress(Exception):
                await safe_delete_message(msg.bot, uid, getattr(u, "last_message_id", None))
            with contextlib.suppress(Exception):
                await render_main(msg.bot, tenant, u)
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ PLATINUM URL сохранён.", reply_markup=kb_admin_main())

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
            with contextlib.suppress(Exception):
                await safe_delete_message(msg.bot, uid, getattr(u, "last_message_id", None))
            with contextlib.suppress(Exception):
                await render_main(msg.bot, tenant, u)
        finally:
            db.close()
        await state.clear()
        await msg.answer("✅ Мини-апп для пользователя обновлена. Напишите ему в ЛС, чтобы он нажал /start.",
                         reply_markup=kb_admin_main())

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

    # Поиск: TG ID / @username / TraderID
    @r.message(AdminForm.search_wait_query)
    async def people_search(msg: Message, state: FSMContext):
        if msg.from_user.id != tenant.owner_tg_id:
            return
        q = (msg.text or "").strip()

        def _norm_username(s: str) -> Optional[str]:
            s = s.strip()
            if not s:
                return None
            if "t.me/" in s:
                s = s.split("t.me/", 1)[1]
            s = s.lstrip("@").split("/", 1)[0]
            return s or None

        db = SessionLocal()
        try:
            # 1) TG ID
            if q.isdigit():
                u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == int(q)).first()
                if u and u.tg_user_id:
                    await state.clear()
                    await msg.answer("Нашёл по TG ID.", reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text=f"Открыть {u.tg_user_id}", callback_data=f"adm:user:{u.tg_user_id}")]]
                    ))
                    return

            # 2) TraderID (точное совпадение)
            u2 = db.query(User).filter(User.tenant_id == tenant.id, User.trader_id == q).first()
            if not u2:
                # через Postback (registration)
                pb = db.query(Postback).filter(
                    Postback.tenant_id == tenant.id,
                    Postback.event == "registration",
                    Postback.trader_id == q,
                    Postback.token_ok.is_(True)
                ).order_by(Postback.created_at.asc()).first()
                if pb:
                    # попробуем tg_user_id
                    uid_guess = int(pb.click_id) if (pb.click_id and str(pb.click_id).isdigit()) else None
                    if uid_guess:
                        u2 = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid_guess).first()
                    if not u2:
                        u2 = db.query(User).filter(User.tenant_id == tenant.id, User.click_id == pb.click_id).first()
            if u2 and u2.tg_user_id:
                await state.clear()
                await msg.answer("Нашёл по TraderID.", reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text=f"Открыть {u2.tg_user_id}", callback_data=f"adm:user:{u2.tg_user_id}")]]
                ))
                return

            # 3) @username — перебор последних N и проверка через get_chat
            uname = _norm_username(q)
            if uname:
                # берём последних 300 по updated_at и спрашиваем username у Telegram
                N = 300
                users = db.query(User).filter(User.tenant_id == tenant.id).order_by(User.updated_at.desc().nullslast()).limit(N).all()
                matched = []
                for u in users:
                    if not u.tg_user_id:
                        continue
                    try:
                        ch = await msg.bot.get_chat(u.tg_user_id)
                        if getattr(ch, "username", None) and ch.username.lower() == uname.lower():
                            matched.append(u.tg_user_id)
                    except Exception:
                        continue

                if len(matched) == 1:
                    await state.clear()
                    await msg.answer("Нашёл по username.", reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text=f"Открыть {matched[0]}", callback_data=f"adm:user:{matched[0]}")]]
                    ))
                    return
                elif len(matched) > 1:
                    # покажем до 10
                    rows = [[InlineKeyboardButton(text=str(uid), callback_data=f"adm:user:{uid}") ] for uid in matched[:10]]
                    rows.append([InlineKeyboardButton(text="⬅️ Меню пользователей", callback_data="adm:people")])
                    await state.clear()
                    await msg.answer(f"Найдено: {len(matched)} (показываю до 10)", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
                    return

            await state.clear()
            await msg.answer("Ничего не нашёл. Попробуйте другой запрос.", reply_markup=kb_people_hub())
        finally:
            db.close()

    # ----- Ручные постбэки (быстрые кнопки на карточке)
    @r.callback_query(F.data.startswith("adm:vip:env_on:"))
    async def vip_env_on(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer();
            return
        uid = int(cb.data.rsplit(":", 1)[-1])
        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден");
                return
            u.is_vip = True
            u.vip_miniapp_url = None  # именно ENV
            u.updated_at = datetime.utcnow()
            db.commit()
        finally:
            db.close()
        await _render_admin_user_card(cb, tenant, uid)
        await cb.answer("VIP включен (ENV)")

    @r.callback_query(F.data.startswith("adm:vip:env_off:"))
    async def vip_env_off(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer();
            return
        uid = int(cb.data.rsplit(":", 1)[-1])
        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден");
                return
            u.is_vip = False
            u.vip_miniapp_url = None
            u.updated_at = datetime.utcnow()
            db.commit()
        finally:
            db.close()
        await _render_admin_user_card(cb, tenant, uid)
        await cb.answer("VIP выключен")

    @r.callback_query(F.data.startswith("adm:vip:do:reg:"))
    async def adm_vip_do_reg(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer();
            return
        uid = int(cb.data.split(":")[-1])

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден");
                return

            pb = Postback(
                tenant_id=tenant.id,
                event="registration",
                click_id=str(uid),
                trader_id="manual",
                sum=0,
                token_ok=True,
            )
            db.add(pb)
            if u.step in (UserStep.new, UserStep.asked_reg, UserStep.asked_deposit):
                u.step = UserStep.registered
            u.updated_at = datetime.utcnow()
            db.commit()
        finally:
            db.close()

        await _render_admin_user_card(cb, tenant, uid)
        await cb.answer("Регистрация проставлена")

    @r.callback_query(F.data.startswith("adm:vip:do:dep:"))
    async def adm_vip_do_dep(cb: CallbackQuery):
        if cb.from_user.id != tenant.owner_tg_id:
            await cb.answer();
            return
        uid = int(cb.data.split(":")[-1])

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.tenant_id == tenant.id, User.tg_user_id == uid).first()
            if not u:
                await cb.answer("Юзер не найден");
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
            if cfg.require_deposit:
                u.step = UserStep.deposited if sum_value >= cfg.min_deposit else UserStep.asked_deposit
            else:
                u.step = UserStep.deposited if u.step < UserStep.deposited else u.step
            u.updated_at = datetime.utcnow()
            db.commit()
        finally:
            db.close()

        await _render_admin_user_card(cb, tenant, uid)
        await cb.answer("Депозит проставлен")

    # Прогресс депозита обновить
    @r.callback_query(F.data == "prog:dep")
    async def refresh_progress(cb: CallbackQuery):
        db = SessionLocal()
        try:
            user = db.query(User).filter(
                User.tenant_id == tenant.id,
                User.tg_user_id == cb.from_user.id
            ).first()
            if not user:
                await cb.answer();
                return
            with contextlib.suppress(Exception):
                await safe_delete_message(bot, cb.from_user.id, getattr(cb.message, "message_id", None))
            await recompute_and_route(bot, tenant, user)
            locale = (user.lang or tenant.lang_default or "ru").lower()
            await cb.answer("Обновлено" if locale == "ru" else "Updated")
        finally:
            db.close()

    # подключаем роутер и запускаем поллинг
    dp.include_router(r)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        # Важно: не глотаем отмену, чтобы stop_task() мог дождаться завершения
        raise
    except Exception as e:
        print(f"[child] start_polling crashed for tenant_id={tenant.id}: {e!r}")
    finally:
        with contextlib.suppress(Exception):
            await bot.session.close()


# ---------------------- ПРОГРЕСС (централизованный роутер) ----------------------
async def recompute_and_route(bot: Bot, tenant: Tenant, user: User):
    db = SessionLocal()
    try:
        tenant = get_fresh_tenant(db, tenant.id) or tenant
        cfg = get_cfg(db, tenant.id)

        # 1) Подписка (если включена)
        if getattr(cfg, "require_subscription", False):
            ok = await is_user_subscribed(bot, tenant.channel_url or "", user.tg_user_id)
            if not ok:
                await render_subscribe(bot, tenant, user)
                db.commit()
                return

        # 2) Шаги/доступ
        await render_get(bot, tenant, user)
        db.commit()
    finally:
        db.close()
