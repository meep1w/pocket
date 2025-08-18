# app/bots/parent/handlers/ga.py

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.exceptions import TelegramBadRequest

from app.settings import settings
from app.db import SessionLocal
from app.models import (
    Tenant, TenantStatus, User, UserStep,
    TenantText, TenantConfig, Postback
)

router = Router()


def _is_ga(uid: int) -> bool:
    return uid in settings.ga_admin_ids


def _safe_edit_text(message, text: str, reply_markup=None):
    async def _do():
        try:
            await message.edit_text(text, reply_markup=reply_markup)
        except TelegramBadRequest as e:
            # игнорируем "message is not modified"
            if "message is not modified" not in str(e).lower():
                raise
    return _do()


def t_line(db, t: Tenant) -> str:
    total = db.query(User).filter(User.tenant_id == t.id).count()
    reg = db.query(User).filter(
        User.tenant_id == t.id,
        User.step >= UserStep.registered
    ).count()
    dep = db.query(User).filter(
        User.tenant_id == t.id,
        User.step == UserStep.deposited
    ).count()
    return f"#{t.id} {t.child_bot_username} — <b>{t.status}</b> | 👥 {total} / 📝 {reg} / 💰 {dep}"


@router.message(Command("ga"))
async def ga_menu(msg: Message):
    if not _is_ga(msg.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список клиентов", callback_data="ga:list:1")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="ga:agg")],
    ])
    await msg.answer("Главное меню администратора:", reply_markup=kb)


@router.callback_query(lambda c: c.data and c.data.startswith("ga:list:"))
async def ga_list(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    page = int(cb.data.split(":")[2])
    per = 10
    db = SessionLocal()
    try:
        q = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted)
        total = q.count()
        tenants = (
            q.order_by(Tenant.id.desc())
             .offset((page - 1) * per)
             .limit(per)
             .all()
        )

        if not tenants:
            await _safe_edit_text(
                cb.message,
                "Клиентов пока нет.",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="ga:menu")]
                ])
            )
            await cb.answer()
            return

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
            nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"ga:list:{page - 1}"))
        if page * per < total:
            nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"ga:list:{page + 1}"))
        if nav:
            rows.append(nav)

        await _safe_edit_text(
            cb.message,
            "Клиенты:\n" + "\n".join(lines),
            InlineKeyboardMarkup(inline_keyboard=rows)
        )
        await cb.answer()
    finally:
        db.close()


@router.callback_query(lambda c: c.data == "ga:agg")
async def ga_agg(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    db = SessionLocal()
    try:
        tenants = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted).all()
        tenant_ids = [t.id for t in tenants]

        tenants_total = len(tenants)
        tenants_active = sum(1 for t in tenants if t.status == TenantStatus.active)
        tenants_paused = sum(1 for t in tenants if t.status == TenantStatus.paused)

        users_total = db.query(User).filter(User.tenant_id.in_(tenant_ids)).count() if tenant_ids else 0
        users_reg = db.query(User).filter(
            User.tenant_id.in_(tenant_ids),
            User.step >= UserStep.registered
        ).count() if tenant_ids else 0
        users_dep = db.query(User).filter(
            User.tenant_id.in_(tenant_ids),
            User.step == UserStep.deposited
        ).count() if tenant_ids else 0

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
        ])
        await _safe_edit_text(cb.message, text, kb)
        await cb.answer()
    finally:
        db.close()


@router.callback_query(lambda c: c.data and c.data.startswith("ga:toggle:"))
async def ga_toggle(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден")
            return

        # переключаем статус
        t.status = TenantStatus.paused if t.status == TenantStatus.active else TenantStatus.active
        db.commit()

        # child-runner должен смотреть в статус и останавливать/запускать бота сам
        await cb.answer("Готово")
        # обновим список на той же странице
        await ga_list(cb)
    finally:
        db.close()


@router.callback_query(lambda c: c.data and c.data.startswith("ga:show:"))
async def ga_show(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден")
            return

        line = t_line(db, t)
        txt = (
            f"{line}\n"
            f"Владелец: <code>{t.owner_tg_id}</code>\n"
            f"Support: {t.support_url or '—'}\n"
            f"Ref: {t.ref_link or '—'}\n"
            f"MiniApp: {t.miniapp_url or settings.miniapp_url or '—'}\n"
            f"Deposit URL: {t.deposit_link or '—'}"
        )
        rows = [
            [InlineKeyboardButton(text="🔁 Постбэки", callback_data=f"ga:pb:{t.id}")],
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"ga:del:{t.id}")],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:list:1")],
        ]
        await _safe_edit_text(cb.message, txt, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


@router.callback_query(lambda c: c.data and c.data.startswith("ga:pb:"))
async def ga_pb(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден")
            return

        secret = t.postback_secret or settings.global_postback_secret
        base = settings.service_host.rstrip("/")
        reg = f"{base}/pb?tenant_id={t.id}&event=registration&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}"
        dep = f"{base}/pb?tenant_id={t.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"

        txt = (
            f"Постбэки для {t.child_bot_username}\n\n"
            f"📝 Регистрация:\n<code>{reg}</code>\n\n"
            f"💳 Депозит:\n<code>{dep}</code>\n\n"
            "Макросы в PocketPartners (вписать 1-в-1):\n"
            "• Регистрация: click_id→<code>click_id</code>, trader_id→<code>trader_id</code>\n"
            "• Депозит: click_id→<code>click_id</code>, trader_id→<code>trader_id</code>, sumdep→<code>sum</code>"
        )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga:show:{t.id}")]
        ])
        await _safe_edit_text(cb.message, txt, kb)
        await cb.answer()
    finally:
        db.close()


# -------- Удаление клиента (с подтверждением) --------

@router.callback_query(lambda c: c.data and c.data.startswith("ga:del:"))
async def ga_del(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден")
            return

        text = (
            f"Удалить клиента #{t.id} {t.child_bot_username}?\n\n"
            "Это полностью удалит:\n"
            "• запись клиента;\n"
            "• пользователей, постбеки, контент, конфиг.\n\n"
            "Действие необратимо."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"ga:delconfirm:{t.id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"ga:show:{t.id}")],
        ])
        await _safe_edit_text(cb.message, text, kb)
        await cb.answer()
    finally:
        db.close()


@router.callback_query(lambda c: c.data and c.data.startswith("ga:delconfirm:"))
async def ga_del_confirm(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer()
        return

    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        # соберём всё, что связано с tenant_id
        users = db.query(User).filter(User.tenant_id == tid).all()
        for u in users:
            db.delete(u)

        pbs = db.query(Postback).filter(Postback.tenant_id == tid).all()
        for p in pbs:
            db.delete(p)

        texts = db.query(TenantText).filter(TenantText.tenant_id == tid).all()
        for tt in texts:
            db.delete(tt)

        cfg = db.query(TenantConfig).filter(TenantConfig.tenant_id == tid).first()
        if cfg:
            db.delete(cfg)

        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if t:
            db.delete(t)

        db.commit()

        # сообщение об успехе (и кнопка вернуться к списку)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:list:1")]
        ])
        await _safe_edit_text(cb.message, f"Клиент #{tid} удалён ✅", kb)
        await cb.answer("Удалено")
    finally:
        db.close()
