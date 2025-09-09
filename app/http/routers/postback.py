from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

from sqlalchemy import func

from app.db import SessionLocal
from app.models import Postback, Tenant, User, UserStep, TenantText, TenantConfig
from app.settings import settings
import re
router = APIRouter()


# ----------------------- helpers -----------------------
def norm_event(raw: str) -> str:
    r = (raw or "").lower().strip()
    if r in {"reg", "registration", "signup", "sign_up"}:
        return "registration"
    # Любые повторы считаем депозитом
    if r in {"dep", "deposit", "payment", "deposit_repeat", "repeat_deposit", "redeposit", "redep"}:
        return "deposit"
    return r

def _extract_tg_id(val: Optional[str]) -> Optional[int]:
    """Пробуем вытащить TG ID из click_id: либо целиком цифры, либо первая длинная цифропоследовательность."""
    if not val:
        return None
    s = str(val).strip()
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return None
    m = re.search(r"\d{6,12}", s)  # TG ID обычно 7–11 цифр
    if m:
        try:
            return int(m.group(0))
        except Exception:
            return None
    return None

def _resolve_user(db, tenant_id: int, click_id: Optional[str], trader_id: Optional[str]):
    """
    Находим пользователя по tg_user_id (если click_id выглядит как число),
    иначе по click_id, иначе по trader_id. Возвращаем (user, uid_candidate).
    """
    uid_candidate = _extract_tg_id(click_id)
    u = None
    if uid_candidate:
        u = db.query(User).filter(User.tenant_id == tenant_id, User.tg_user_id == uid_candidate).first()
    if not u and click_id:
        u = db.query(User).filter(User.tenant_id == tenant_id, User.click_id == click_id).first()
    if not u and trader_id:
        u = db.query(User).filter(User.tenant_id == tenant_id, User.trader_id == trader_id).first()
    return u, uid_candidate




def _parse_sum(params: dict) -> float:
    # Берём из любых распространённых ключей
    candidates = [params.get("sum"), params.get("sumdep"), params.get("amount"), params.get("amt")]
    for v in candidates:
        if v is None:
            continue
        s = str(v).replace(",", ".").strip()
        if not s:
            continue
        try:
            return float(s)
        except Exception:
            continue
    return 0.0


def default_img_url(key: str, locale: str) -> str:
    base = settings.service_host.rstrip("/")
    return f"{base}/static/stock/{key}-{locale}.jpg"


def _dep_total(db, tenant_id: int, user: Optional[User], click_id_param: Optional[str]) -> int:
    """
    Возвращает суммарный депозит по ключу click_id.
    Предпочитаем tg_user_id пользователя (если есть), иначе – click_id из запроса.
    Считаем ВСЕ депозиты (все "deposit" после нормализации).
    """
    key = None
    if user and user.tg_user_id:
        key = str(user.tg_user_id)
    elif click_id_param:
        key = str(click_id_param)

    if not key:
        return 0

    total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
        Postback.tenant_id == tenant_id,
        Postback.event == "deposit",
        Postback.click_id == key,
        Postback.token_ok.is_(True),
    ).scalar() or 0
    return int(total)


# ----------------------- endpoint -----------------------
@router.get("/pb")
async def handle_postback(request: Request):
    params = dict(request.query_params)

    tenant_id = int(params.get("tenant_id") or 0)
    event = norm_event(params.get("event"))
    token = params.get("t")
    click_id = params.get("click_id")
    trader_id = params.get("trader_id")

    sum_val = _parse_sum(params)
    sum_int = int(sum_val)  # для хранения в int поле, если у тебя sum:int; при желании можно сменить на Decimal/Float

    raw_query_str = str(params)

    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tenant_id).first()
        if not t:
            raise HTTPException(status_code=404, detail="tenant not found")

        # проверка секрета
        token_ok = False
        if settings.tenant_secret_mode == "enabled":
            if t.postback_secret and token == t.postback_secret:
                token_ok = True
        else:
            if token == settings.global_postback_secret:
                token_ok = True

        # логируем и рвём, если секрет неправильный
        if not token_ok:
            db.add(Postback(
                tenant_id=tenant_id, event=event, click_id=click_id, trader_id=trader_id,
                sum=sum_int, token_ok=False, raw_query=raw_query_str
            ))
            db.commit()
            raise HTTPException(status_code=403, detail="forbidden")

        # мягкая идемпотентность: дублирующийся ТОЧНО такой же raw_query уже был
        # (не блокируем повторные депозиты на те же суммы, если raw_query отличается)
        existing = db.query(Postback).filter(
            Postback.tenant_id == tenant_id,
            Postback.event == event,
            Postback.click_id == click_id,
            Postback.token_ok.is_(True),
            Postback.raw_query == raw_query_str,
        ).first()
        if existing:
            return {"ok": True, "dup": True}

        # сохраняем входящий постбэк
        db.add(Postback(
            tenant_id=tenant_id, event=event, click_id=click_id, trader_id=trader_id,
            sum=sum_int, token_ok=True, raw_query=raw_query_str
        ))
        db.commit()

        # конфиг тенанта
        cfg = db.query(TenantConfig).filter(TenantConfig.tenant_id == tenant_id).first()
        if not cfg:
            cfg = TenantConfig(tenant_id=tenant_id, require_deposit=True, min_deposit=50)
            db.add(cfg)
            db.commit()
        # гарантируем vip_threshold
        if getattr(cfg, "vip_threshold", None) is None:
            cfg.vip_threshold = 500
            db.commit()

        # находим/создаём пользователя
        user = None
        if click_id and str(click_id).isdigit():
            user = db.query(User).filter(User.tenant_id == tenant_id, User.tg_user_id == int(click_id)).first()
        if not user and click_id:
            user = db.query(User).filter(User.tenant_id == tenant_id, User.click_id == click_id).first()
        if not user and trader_id:
            user = db.query(User).filter(User.tenant_id == tenant_id, User.trader_id == trader_id).first()

        notify = False

        # ----- registration -----
        if event == "registration":
            user, uid_candidate = _resolve_user(db, tenant_id, click_id, trader_id)

            if not user:
                user = User(
                    tenant_id=tenant_id,
                    tg_user_id=uid_candidate,  # если сможем извлечь
                    click_id=click_id,
                    trader_id=trader_id,
                    step=UserStep.registered,
                    updated_at=datetime.utcnow(),
                )
                db.add(user)
                db.commit()
                notify = True
            else:
                # обновим идентификаторы/статус
                if not user.tg_user_id and uid_candidate:
                    user.tg_user_id = uid_candidate
                if click_id:
                    user.click_id = click_id
                if trader_id:
                    user.trader_id = trader_id
                if user.step in (UserStep.new, UserStep.asked_reg, UserStep.asked_deposit):
                    user.step = UserStep.registered
                    notify = True
                user.updated_at = datetime.utcnow()
                db.commit()

            # если депозит не обязателен — сразу открыть доступ
            if notify and not cfg.require_deposit and user.step != UserStep.deposited:
                user.step = UserStep.deposited
                db.commit()

        # ----- deposit (вкл. повторные) -----
        elif event == "deposit":
            user, uid_candidate = _resolve_user(db, tenant_id, click_id, trader_id)
            dep_total_now = _dep_total(db, tenant_id, user, click_id)

            if not user:
                user = User(
                    tenant_id=tenant_id,
                    tg_user_id=uid_candidate,
                    click_id=click_id,
                    trader_id=trader_id,
                    step=(UserStep.deposited if (cfg.require_deposit is False or dep_total_now >= cfg.min_deposit)
                          else UserStep.asked_deposit),
                    updated_at=datetime.utcnow(),
                )
                db.add(user)
                db.commit()
                notify = True
            else:
                # подтянем идентификаторы
                if not user.tg_user_id and uid_candidate:
                    user.tg_user_id = uid_candidate
                if click_id:
                    user.click_id = click_id
                if trader_id:
                    user.trader_id = trader_id

                if cfg.require_deposit:
                    user.step = UserStep.deposited if dep_total_now >= cfg.min_deposit else UserStep.asked_deposit
                else:
                    if user.step < UserStep.deposited:
                        user.step = UserStep.deposited

                user.updated_at = datetime.utcnow()
                db.commit()
                notify = True

        # ----- вывод экрана/уведомления -----
        if notify and user and user.tg_user_id and t.child_bot_token:
            try:
                bot = Bot(token=t.child_bot_token, default=DefaultBotProperties(parse_mode="HTML"))
                locale = (user.lang or t.lang_default or "ru").lower()

                def get_tt(key: str, fallback_ru: str, fallback_en: str):
                    text = fallback_ru if locale == "ru" else fallback_en
                    image_id = None
                    tt = db.query(TenantText).filter(
                        TenantText.tenant_id == tenant_id,
                        TenantText.locale == locale,
                        TenantText.key == key
                    ).first()
                    if tt:
                        if tt.text: text = tt.text
                        if tt.image_file_id: image_id = tt.image_file_id
                    return text, image_id

                # удалить предыдущее сообщение
                try:
                    if user.last_message_id:
                        await bot.delete_message(user.tg_user_id, user.last_message_id)
                except Exception:
                    pass

                # --- VIP уведомление при достижении порога ---
                dep_total_vip = _dep_total(db, tenant_id, user, click_id)
                try:
                    vip_thr = int(getattr(cfg, "vip_threshold", 500) or 500)
                except Exception:
                    vip_thr = 500

                if dep_total_vip >= vip_thr and not getattr(user, "vip_notified", False):
                    if locale == "ru":
                        vip_text = "🎉 Поздравляем! Вам доступен премиум-бот. Напишите в поддержку для подключения."
                        support_caption = "🆘 Поддержка"
                    else:
                        vip_text = "🎉 Congrats! You’re eligible for the premium bot. Please contact support to get access."
                        support_caption = "🆘 Support"

                    kb_support = None
                    if t.support_url:
                        kb_support = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text=support_caption, url=t.support_url)]
                        ])

                    try:
                        m = await bot.send_message(user.tg_user_id, vip_text, reply_markup=kb_support)
                    except Exception:
                        m = await bot.send_message(user.tg_user_id, vip_text)

                    user.vip_notified = True
                    user.last_message_id = m.message_id
                    db.commit()
                    await bot.session.close()
                    return  # отправили единственное уведомление — выходим

                # --- обычные экраны (зависит от наличия доступа) ---
                dep_total = _dep_total(db, tenant_id, user, click_id)
                has_access = (not cfg.require_deposit and user.step >= UserStep.registered) or \
                             (cfg.require_deposit and dep_total >= cfg.min_deposit)

                if has_access:
                    # UNLOCKED
                    text, img = get_tt(
                        "unlocked",
                        "Доступ открыт. Нажмите «Получить сигнал».",
                        "Access granted. Press 'Get signal'."
                    )
                    webapp_base = (t.miniapp_url or settings.miniapp_url).rstrip("/")
                    webapp = f"{webapp_base}?tenant_id={tenant_id}&uid={user.tg_user_id}"
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text=("📈 Получить сигнал" if locale == "ru" else "📈 Get signal"),
                            web_app=WebAppInfo(url=webapp)
                        )],
                        [InlineKeyboardButton(
                            text=("🏠 Главное меню" if locale == "ru" else "🏠 Main menu"),
                            callback_data="menu:main"
                        )],
                    ])
                    try:
                        if img:
                            m = await bot.send_photo(user.tg_user_id, img, caption=text, reply_markup=kb)
                        else:
                            m = await bot.send_photo(
                                user.tg_user_id, default_img_url("unlocked", locale),
                                caption=text, reply_markup=kb
                            )
                    except Exception:
                        m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)
                else:
                    # STEP 2 (депозит)
                    left = max(0, int(cfg.min_deposit) - dep_total)
                    text, img = get_tt(
                        "step2",
                        "Шаг 2. Внесите депозит (≥ ${{min_dep}}).",
                        "Step 2. Make a deposit (≥ ${{min_dep}})."
                    )
                    text = text.replace("{{min_dep}}", str(cfg.min_deposit))
                    text += (
                        f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${left})"
                        if locale == "ru" else
                        f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${left})"
                    )
                    dep_url = f"{settings.service_host}/r/dep?tenant_id={tenant_id}&uid={user.tg_user_id}"
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text=("💳 Внести депозит" if locale == "ru" else "💳 Deposit"),
                            url=dep_url
                        )],
                        [InlineKeyboardButton(
                            text=(f"🔄 Прогресс: ${dep_total}/{cfg.min_deposit}" if locale == "ru"
                                  else f"🔄 Progress: ${dep_total}/{cfg.min_deposit}"),
                            callback_data="prog:dep"
                        )],
                        [InlineKeyboardButton(
                            text=("🏠 Главное меню" if locale == "ru" else "🏠 Main menu"),
                            callback_data="menu:main"
                        )],
                    ])
                    try:
                        if img:
                            m = await bot.send_photo(user.tg_user_id, img, caption=text, reply_markup=kb)
                        else:
                            m = await bot.send_photo(
                                user.tg_user_id, default_img_url("step2", locale),
                                caption=text, reply_markup=kb
                            )
                    except Exception:
                        m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)

                user.last_message_id = m.message_id
                db.commit()
                await bot.session.close()
            except Exception:
                # проглатываем исключения отправки — endpoint всё равно ok
                pass

        return {"ok": True}
    finally:
        db.close()
