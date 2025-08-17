from datetime import datetime

from fastapi import APIRouter, Request, HTTPException
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

from app.db import SessionLocal
from app.models import Postback, Tenant, User, UserStep, TenantText, TenantConfig
from app.settings import settings

from sqlalchemy import func
from app.models import Postback, Tenant, User, UserStep, TenantText, TenantConfig


router = APIRouter()

def norm_event(raw: str) -> str:
    r = (raw or "").lower()
    if r in ("reg", "registration", "signup", "sign_up"):
        return "registration"
    if r in ("dep", "deposit", "payment"):
        return "deposit"
    return r

def default_img_url(key: str, locale: str) -> str:
    base = settings.service_host.rstrip("/")
    return f"{base}/static/stock/{key}-{locale}.jpg"

@router.get("/pb")
async def handle_postback(request: Request):
    params = dict(request.query_params)

    tenant_id = int(params.get("tenant_id") or 0)
    event = norm_event(params.get("event"))
    token = params.get("t")
    click_id = params.get("click_id")
    trader_id = params.get("trader_id")
    sum_str = params.get("sum") or "0"
    try:
        sum_val = int(float(sum_str))
    except Exception:
        sum_val = 0

    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tenant_id).first()
        if not t:
            raise HTTPException(status_code=404, detail="tenant not found")

        # секрет
        token_ok = False
        if settings.tenant_secret_mode == "enabled":
            if t.postback_secret and token == t.postback_secret:
                token_ok = True
        else:
            if token == settings.global_postback_secret:
                token_ok = True
        if not token_ok:
            db.add(Postback(tenant_id=tenant_id, event=event, click_id=click_id, trader_id=trader_id,
                            sum=sum_val, token_ok=False, raw_query=str(params)))
            db.commit()
            raise HTTPException(status_code=403, detail="forbidden")

        # дедуп
        existing = db.query(Postback).filter(
            Postback.tenant_id == tenant_id,
            Postback.event == event,
            Postback.click_id == click_id,
            Postback.sum == sum_val,
            Postback.token_ok.is_(True),
        ).first()
        if existing:
            return {"ok": True, "dup": True}

        db.add(Postback(tenant_id=tenant_id, event=event, click_id=click_id, trader_id=trader_id,
                        sum=sum_val, token_ok=True, raw_query=str(params)))
        db.commit()

        # конфиг тенанта
        cfg = db.query(TenantConfig).filter(TenantConfig.tenant_id == tenant_id).first()
        if not cfg:
            cfg = TenantConfig(tenant_id=tenant_id, require_deposit=True, min_deposit=50)
            db.add(cfg); db.commit()

        # находим/создаём пользователя
        user = None
        if click_id and str(click_id).isdigit():
            user = db.query(User).filter(User.tenant_id == tenant_id, User.tg_user_id == int(click_id)).first()
        if not user and click_id:
            user = db.query(User).filter(User.tenant_id == tenant_id, User.click_id == click_id).first()
        if not user and trader_id:
            user = db.query(User).filter(User.tenant_id == tenant_id, User.trader_id == trader_id).first()

        notify = False

        if event == "registration":
            if not user:
                user = User(
                    tenant_id=tenant_id,
                    tg_user_id=int(click_id) if click_id and str(click_id).isdigit() else None,
                    click_id=click_id,
                    trader_id=trader_id,
                    step=UserStep.registered,
                    updated_at=datetime.utcnow(),
                )
                db.add(user); db.commit()
                notify = True
            else:
                if user.step in (UserStep.new, UserStep.asked_reg, UserStep.asked_deposit):
                    user.step = UserStep.registered
                    if click_id: user.click_id = click_id
                    if trader_id: user.trader_id = trader_id
                    user.updated_at = datetime.utcnow()
                    db.commit()
                    notify = True

            # если депозит не обязателен — сразу открываем доступ (используем шаг deposited как «доступ открыт»)
            if notify and not cfg.require_deposit:
                if user.step != UserStep.deposited:
                    user.step = UserStep.deposited
                    db.commit()


        elif event == "deposit":
            # сумма всех депозитов пользователя
            dep_total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
                Postback.tenant_id == tenant_id,
                Postback.event == "deposit",
                Postback.click_id == str(click_id),
                Postback.token_ok.is_(True),
            ).scalar() or 0
            dep_total = int(dep_total)
            if not user:
                user = User(
                    tenant_id=tenant_id,
                    tg_user_id=int(click_id) if click_id and str(click_id).isdigit() else None,
                    click_id=click_id,
                    trader_id=trader_id,
                    step=UserStep.deposited if dep_total >= cfg.min_deposit else UserStep.asked_deposit,
                    updated_at=datetime.utcnow(),
                )
                db.add(user);
                db.commit()
                notify = True
            else:
                # если уже открыт доступ
                if user.step == UserStep.deposited:
                    notify = True  # просто переобновим экран на "unlocked"
                else:
                    if dep_total >= cfg.min_deposit:
                        user.step = UserStep.deposited
                    else:
                        user.step = UserStep.asked_deposit
                    if click_id: user.click_id = click_id
                    if trader_id: user.trader_id = trader_id
                    user.updated_at = datetime.utcnow()
                    db.commit()
                    notify = True

        # показываем соответствующий экран (редактируемый)
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

                # удалить предыдущий экран
                try:
                    if user.last_message_id:
                        await bot.delete_message(user.tg_user_id, user.last_message_id)
                except Exception:
                    pass

                if user.step == UserStep.registered and cfg.require_deposit:
                    # сразу шаг 2 после регистрации, прогресс 0
                    dep_total = db.query(func.coalesce(func.sum(Postback.sum), 0)).filter(
                        Postback.tenant_id == tenant_id,
                        Postback.event == "deposit",
                        Postback.click_id == str(user.tg_user_id),
                        Postback.token_ok.is_(True),
                    ).scalar() or 0
                    dep_total = int(dep_total)
                    left = max(0, cfg.min_deposit - dep_total)

                    text, img = get_tt("step2",
                                       "Шаг 2. Внесите депозит (≥ ${{min_dep}}).",
                                       "Step 2. Make a deposit (≥ ${{min_dep}}).")
                    text = text.replace("{{min_dep}}", str(cfg.min_deposit))
                    text += (f"\n\n💵 Внесено: ${dep_total} / ${cfg.min_deposit} (осталось ${left})"
                             if locale == "ru" else
                             f"\n\n💵 Paid: ${dep_total} / ${cfg.min_deposit} (left ${left})")
                    dep_url = f"{settings.service_host}/r/dep?tenant_id={tenant_id}&uid={user.tg_user_id}"
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=("💳 Внести депозит" if locale == "ru" else "💳 Deposit"),
                                              url=dep_url)],
                        [InlineKeyboardButton(text=(f"🔄 Прогресс: ${dep_total}/{cfg.min_deposit}" if locale == "ru"
                                                    else f"🔄 Progress: ${dep_total}/{cfg.min_deposit}"),
                                              callback_data="prog:dep")],
                        [InlineKeyboardButton(text=("🏠 Главное меню" if locale == "ru" else "🏠 Main menu"),
                                              callback_data="menu:main")],
                    ])
                    try:
                        if img:
                            m = await bot.send_photo(user.tg_user_id, img, caption=text, reply_markup=kb)
                        else:
                            m = await bot.send_photo(user.tg_user_id, default_img_url("step2", locale), caption=text,reply_markup=kb)
                    except Exception:
                        m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)


                else:
                    # unlocked экран
                    text, img = get_tt("unlocked",
                                       "Доступ открыт. Нажмите «Получить сигнал».",
                                       "Access granted. Press 'Get signal'.")
                    webapp = f"{settings.miniapp_url}?tenant_id={tenant_id}&uid={user.tg_user_id}"
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=("📈 Получить сигнал" if locale=="ru" else "📈 Get signal"),
                                              web_app=WebAppInfo(url=webapp))],
                        [InlineKeyboardButton(text=("🏠 Главное меню" if locale=="ru" else "🏠 Main menu"), callback_data="menu:main")],
                    ])
                    try:
                        if img:
                            m = await bot.send_photo(user.tg_user_id, img, caption=text, reply_markup=kb)
                        else:
                            m = await bot.send_photo(user.tg_user_id, default_img_url("unlocked", locale), caption=text, reply_markup=kb)
                    except Exception:
                        m = await bot.send_message(user.tg_user_id, text, reply_markup=kb)

                user.last_message_id = m.message_id
                db.commit()
                await bot.session.close()
            except Exception:
                pass

        return {"ok": True}
    finally:
        db.close()
