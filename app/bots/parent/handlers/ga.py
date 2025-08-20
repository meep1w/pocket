from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from sqlalchemy import and_

from app.settings import settings
from app.db import SessionLocal
from app.models import (
    Tenant, TenantStatus,
    User, UserStep,
    TenantText, TenantConfig, Postback,
)

router = Router()


def _is_ga(uid: int) -> bool:
    return uid in settings.ga_admin_ids


def _ga_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список клиентов", callback_data="ga:list:1")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="ga:agg")],
        [InlineKeyboardButton(text="🧹 Очистка БД", callback_data="ga:clean:1")],
    ])


def _safe_edit(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup | None = None):
    # чтобы не падать на "message is not modified"
    async def _do():
        try:
            await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            try:
                await cb.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
    return _do()


def t_line(db, t: Tenant):
    total = db.query(User).filter(User.tenant_id == t.id).count()
    reg = db.query(User).filter(
        and_(User.tenant_id == t.id, User.step >= UserStep.registered)
    ).count()
    dep = db.query(User).filter(
        and_(User.tenant_id == t.id, User.step == UserStep.deposited)
    ).count()
    return f"#{t.id} {t.child_bot_username} — <b>{t.status}</b> | 👥 {total} / 📝 {reg} / 💰 {dep}"


# ===== Главное меню GA =====
@router.message(Command("ga"))
async def ga_menu(msg: Message):
    if not _is_ga(msg.from_user.id):
        return
    await msg.answer("Главное меню администратора:", reply_markup=_ga_menu_kb())


@router.callback_query(F.data == "ga:home")
async def ga_home(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    await _safe_edit(cb, "Главное меню администратора:", _ga_menu_kb())
    await cb.answer()


# ===== Список клиентов =====
@router.callback_query(F.data.startswith("ga:list:"))
async def ga_list(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    page = int(cb.data.split(":")[2])
    per = 10
    db = SessionLocal()
    try:
        q = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted)
        total = q.count()
        tenants = q.order_by(Tenant.id.desc()).offset((page-1)*per).limit(per).all()
        if not tenants:
            await _safe_edit(cb, "Клиентов пока нет.", _ga_menu_kb())
            await cb.answer(); return

        lines = [t_line(db, t) for t in tenants]

        rows = []
        for t in tenants:
            rows.append([
                InlineKeyboardButton(
                    text=("⏸ Пауза" if t.status == TenantStatus.active else "▶️ Запуск"),
                    callback_data=f"ga:toggle:{t.id}"
                ),
                InlineKeyboardButton(text="ℹ️ Детали", callback_data=f"ga:show:{t.id}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"ga:del:{t.id}"),
            ])

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"ga:list:{page-1}"))
        if page*per < total:
            nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"ga:list:{page+1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")])

        await _safe_edit(cb, "Клиенты:\n" + "\n".join(lines),
                         InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


# ===== Общая статистика =====
@router.callback_query(F.data == "ga:agg")
async def ga_agg(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return

    db = SessionLocal()
    try:
        tenants = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted).all()
        ids = [t.id for t in tenants]

        tenants_total = len(tenants)
        tenants_active = sum(1 for t in tenants if t.status == TenantStatus.active)
        tenants_paused = sum(1 for t in tenants if t.status == TenantStatus.paused)

        users_total = db.query(User).filter(User.tenant_id.in_(ids)).count() if ids else 0
        users_reg = db.query(User).filter(
            and_(User.tenant_id.in_(ids), User.step >= UserStep.registered)
        ).count() if ids else 0
        users_dep = db.query(User).filter(
            and_(User.tenant_id.in_(ids), User.step == UserStep.deposited)
        ).count() if ids else 0

        text = (
            "<b>Общая статистика</b>\n\n"
            f"Клиенты: {tenants_total} (активных: {tenants_active}, на паузе: {tenants_paused})\n"
            f"Пользователи: {users_total}\n"
            f"— зарегистрировались: {users_reg}\n"
            f"— с депозитом: {users_dep}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:list:1")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="ga:agg")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
        ])
        await _safe_edit(cb, text, kb)
        await cb.answer()
    finally:
        db.close()


# ===== Тоггл статуса клиента =====
@router.callback_query(F.data.startswith("ga:toggle:"))
async def ga_toggle(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден"); return
        t.status = TenantStatus.paused if t.status == TenantStatus.active else TenantStatus.active
        db.commit()
        await cb.answer("Ок")
        await ga_list(cb)
    finally:
        db.close()


# ===== Детали клиента =====
@router.callback_query(F.data.startswith("ga:show:"))
async def ga_show(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден"); return

        line = t_line(db, t)
        txt = (
            f"{line}\n"
            f"Владелец: <code>{t.owner_tg_id}</code>\n"
            f"Support: {t.support_url}\n"
            f"Ref: {t.ref_link}\n"
            f"Deposit: {t.deposit_link or '—'}"
        )
        rows = [
            [InlineKeyboardButton(text="🔁 Постбэки", callback_data=f"ga:pb:{t.id}")],
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"ga:del:{t.id}")],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:list:1")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
        ]
        await _safe_edit(cb, txt, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


# ===== Постбэки клиента =====
@router.callback_query(F.data.startswith("ga:pb:"))
async def ga_pb(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден"); return

        secret = t.postback_secret or settings.global_postback_secret
        base = settings.service_host.rstrip("/")
        reg = f"{base}/pb?tenant_id={t.id}&event=registration&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}"
        dep = f"{base}/pb?tenant_id={t.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"

        txt = (
            f"Постбэки для {t.child_bot_username}\n\n"
            f"Регистрация:\n<code>{reg}</code>\n"
            f"Депозит:\n<code>{dep}</code>\n\n"
            "PP макросы:\n"
            "Регистрация: click_id→click_id, trader_id→trader_id\n"
            "Депозит: click_id→click_id, trader_id→trader_id, sumdep→sum"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga:show:{t.id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
        ])
        await _safe_edit(cb, txt, kb)
        await cb.answer()
    finally:
        db.close()


# ===== УДАЛЕНИЕ КЛИЕНТА (полная очистка, потом удаление Tenant) =====
@router.callback_query(F.data.startswith("ga:del:"))
async def ga_del(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"ga:delc:{tid}")],
        [InlineKeyboardButton(text="↩️ Отмена", callback_data=f"ga:show:{tid}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
    ])
    await _safe_edit(cb, f"Вы уверены, что хотите <b>полностью удалить</b> клиента #{tid}?\n"
                         f"Будут удалены: пользователи, постбэки, контент и конфиги. Это необратимо.", kb)
    await cb.answer()


@router.callback_query(F.data.startswith("ga:delc:"))
async def ga_delc(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return

    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await _safe_edit(cb, "Клиент уже отсутствует.", _ga_menu_kb()); await cb.answer(); return

        # 1) Останавливаем детского бота — ставим paused
        if t.status == TenantStatus.active:
            t.status = TenantStatus.paused
            db.commit()

        # 2) Полная очистка связанных данных (дети → родитель)
        db.query(Postback).filter(Postback.tenant_id == t.id).delete(synchronize_session=False)
        db.query(User).filter(User.tenant_id == t.id).delete(synchronize_session=False)
        db.query(TenantText).filter(TenantText.tenant_id == t.id).delete(synchronize_session=False)
        db.query(TenantConfig).filter(TenantConfig.tenant_id == t.id).delete(synchronize_session=False)
        db.commit()

        # 3) Удаление самого тенанта
        db.delete(t)
        db.commit()

        await _safe_edit(cb, f"✅ Клиент #{tid} полностью удалён (включая связанные данные).", _ga_menu_kb())
        await cb.answer("Удалено")
        return

    except Exception as e:
        db.rollback()
        # Фолбэк: пометим как deleted, чтобы не зависло
        try:
            t = db.query(Tenant).filter(Tenant.id == tid).first()
            if t:
                t.status = TenantStatus.deleted
                db.commit()
        except Exception:
            db.rollback()
        await _safe_edit(cb, f"⚠️ Не удалось полностью удалить. Клиент помечен как deleted.\nОшибка: <code>{e}</code>",
                         _ga_menu_kb())
        await cb.answer("Помечен как deleted")
    finally:
        db.close()


# ===== ОЧИСТКА БД КЛИЕНТА (без удаления клиента) =====
# Список для выбора клиента
@router.callback_query(F.data.startswith("ga:clean:"))
async def ga_clean_router(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return

    parts = cb.data.split(":")
    # ga:clean:1  |  ga:clean:pick:{id}  |  ga:clean:confirm:{id}  |  ga:clean:run:{id}
    if len(parts) == 3 and parts[2].isdigit():
        # список с пагинацией
        page = int(parts[2])
        per = 10
        db = SessionLocal()
        try:
            q = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted)
            total = q.count()
            tenants = q.order_by(Tenant.id.desc()).offset((page-1)*per).limit(per).all()
            if not tenants:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")]
                ])
                await _safe_edit(cb, "Клиентов пока нет.", kb)
                await cb.answer(); return

            lines = [t_line(db, t) for t in tenants]
            rows = []
            for t in tenants:
                rows.append([
                    InlineKeyboardButton(text="🧹 Выбрать", callback_data=f"ga:clean:pick:{t.id}"),
                    InlineKeyboardButton(text="ℹ️", callback_data=f"ga:show:{t.id}"),
                ])
            nav = []
            if page > 1:
                nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"ga:clean:{page-1}"))
            if page*per < total:
                nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"ga:clean:{page+1}"))
            if nav:
                rows.append(nav)
            rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")])

            await _safe_edit(cb, "Выберите клиента для очистки БД:\n" + "\n".join(lines),
                             InlineKeyboardMarkup(inline_keyboard=rows))
            await cb.answer()
        finally:
            db.close()
        return

    if len(parts) == 3 and parts[2].startswith("pick"):
        tid = int(parts[2].split("pick")[1].strip(":") or parts[2].split(":")[1])
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tid).first()
            if not t:
                await cb.answer("Не найден"); return
            txt = (
                f"{t_line(db, t)}\n\n"
                "Эта операция <b>очистит БД бота</b> (удалит всех пользователей и постбэки).\n"
                "Настройки, ссылки и контент останутся нетронутыми."
            )
        finally:
            db.close()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Очистить БД бота", callback_data=f"ga:clean:confirm:{tid}")],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:clean:1")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
        ])
        await _safe_edit(cb, txt, kb)
        await cb.answer()
        return

    if len(parts) == 3 and parts[2].startswith("confirm"):
        tid = int(parts[2].split(":")[1])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить очистку", callback_data=f"ga:clean:run:{tid}")],
            [InlineKeyboardButton(text="↩️ Отмена", callback_data=f"ga:clean:pick:{tid}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
        ])
        await _safe_edit(cb,
                         f"Вы уверены, что хотите очистить БД клиента #{tid}?\n"
                         f"Будут удалены: пользователи и постбэки.", kb)
        await cb.answer()
        return

    if len(parts) == 3 and parts[2].startswith("run"):
        tid = int(parts[2].split(":")[1])
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tid).first()
            if not t:
                await _safe_edit(cb, "Клиент не найден.", _ga_menu_kb()); await cb.answer(); return

            # Дозволительно при активном клиенте; просто чистим данные
            db.query(Postback).filter(Postback.tenant_id == tid).delete(synchronize_session=False)
            db.query(User).filter(User.tenant_id == tid).delete(synchronize_session=False)
            db.commit()

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К выбору клиента", callback_data="ga:clean:1")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="ga:home")],
            ])
            await _safe_edit(cb, f"✅ БД клиента #{tid} очищена (пользователи и постбэки).", kb)
            await cb.answer("Готово")
        except Exception as e:
            db.rollback()
            await _safe_edit(cb, f"❌ Ошибка очистки: <code>{e}</code>", _ga_menu_kb())
            await cb.answer("Ошибка")
        finally:
            db.close()
        return

    # если что-то иное — просто домой
    await ga_home(cb)
