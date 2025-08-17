from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from app.settings import settings
from app.db import SessionLocal
from app.models import Tenant, TenantStatus, User, UserStep

router = Router()

def _is_ga(uid: int) -> bool:
    return uid in settings.ga_admin_ids

def t_line(db, t: Tenant):
    total = db.query(User).filter(User.tenant_id==t.id).count()
    reg = db.query(User).filter(User.tenant_id==t.id, User.step>=UserStep.registered).count()
    dep = db.query(User).filter(User.tenant_id==t.id, User.step==UserStep.deposited).count()
    return f"#{t.id} {t.child_bot_username} — <b>{t.status}</b> | 👥 {total} / 📝 {reg} / 💰 {dep}"

@router.message(Command("ga"))
async def ga_menu(msg: Message):
    if not _is_ga(msg.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список клиентов", callback_data="ga:list:1")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="ga:agg")]
    ])
    await msg.answer("Главное меню администратора:", reply_markup=kb)

@router.callback_query(lambda c: c.data and c.data.startswith("ga:list:"))
async def ga_list(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    page = int(cb.data.split(":")[2])
    per = 10
    db = SessionLocal()
    try:
        q = db.query(Tenant).filter(Tenant.status!=TenantStatus.deleted)
        total = q.count()
        tenants = q.order_by(Tenant.id.desc()).offset((page-1)*per).limit(per).all()
        if not tenants:
            await cb.message.edit_text("Клиентов пока нет.")
            await cb.answer(); return
        lines = [t_line(db, t) for t in tenants]
        rows = []
        for t in tenants:
            rows.append([
                InlineKeyboardButton(text=("⏸ Пауза" if t.status==TenantStatus.active else "▶️ Запуск"), callback_data=f"ga:toggle:{t.id}"),
                InlineKeyboardButton(text="ℹ️ Детали", callback_data=f"ga:show:{t.id}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"ga:del:{t.id}"),
            ])
        nav = []
        if page>1: nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"ga:list:{page-1}"))
        if page*per<total: nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"ga:list:{page+1}"))
        if nav: rows.append(nav)
        await cb.message.edit_text("Клиенты:\n" + "\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


@router.callback_query(lambda c: c.data == "ga:agg")
async def ga_agg(cb: CallbackQuery):
    # Только для ГА
    if cb.from_user.id not in settings.ga_admin_ids:
        await cb.answer(); return

    db = SessionLocal()
    try:
        # Берём только тенанты не в статусе deleted
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
        await cb.message.edit_text(text, reply_markup=kb)
        await cb.answer()
    finally:
        db.close()



@router.callback_query(lambda c: c.data and c.data.startswith("ga:toggle:"))
async def ga_toggle(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id==tid).first()
        if not t: await cb.answer("not found"); return
        t.status = TenantStatus.paused if t.status==TenantStatus.active else TenantStatus.active
        db.commit()
        await cb.answer("Ок")
        await ga_list(cb)
    finally:
        db.close()

@router.callback_query(lambda c: c.data and c.data.startswith("ga:show:"))
async def ga_show(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id==tid).first()
        if not t: await cb.answer("not found"); return
        line = t_line(db, t)
        txt = (f"{line}\nВладелец: <code>{t.owner_tg_id}</code>\nSupport: {t.support_url}\nRef: {t.ref_link}")
        rows = [[InlineKeyboardButton(text="🔁 Постбэки", callback_data=f"ga:pb:{t.id}")],
                [InlineKeyboardButton(text="📝 Заметка", callback_data=f"ga:note:{t.id}")],
                [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:list:1")]]
        await cb.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()

@router.callback_query(lambda c: c.data and c.data.startswith("ga:pb:"))
async def ga_pb(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id==tid).first()
        if not t: await cb.answer("not found"); return
        secret = t.postback_secret or settings.global_postback_secret
        base = settings.service_host
        reg = f"{base}/pb?tenant_id={t.id}&event=registration&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}"
        dep = f"{base}/pb?tenant_id={t.id}&event=deposit&t={secret}&click_id={{click_id}}&trader_id={{trader_id}}&sum={{sumdep}}"
        txt = (f"Постбэки для {t.child_bot_username}\n\n"
               f"Регистрация:\n<code>{reg}</code>\n"
               f"Депозит:\n<code>{dep}</code>\n\n"
               "PP макросы (вписать 1-в-1):\n"
               "Регистрация: click_id→click_id, trader_id→trader_id\n"
               "Депозит: click_id→click_id, trader_id→trader_id, sumdep→sum")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga:show:{t.id}")]])
        await cb.message.edit_text(txt, reply_markup=kb, disable_web_page_preview=True)
        await cb.answer()
    finally:
        db.close()
