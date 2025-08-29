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


# --------------------- helpers ---------------------
def _is_ga(uid: int) -> bool:
    return uid in settings.ga_admin_ids


def _safe_edit(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup | None = None):
    async def _do():
        try:
            await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            try:
                await cb.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
    return _do()


def _t_line(db, t: Tenant) -> str:
    total = db.query(User).filter(User.tenant_id == t.id).count()
    reg = db.query(User).filter(
        and_(User.tenant_id == t.id, User.step >= UserStep.registered)
    ).count()
    dep = db.query(User).filter(
        and_(User.tenant_id == t.id, User.step == UserStep.deposited)
    ).count()
    return f"#{t.id} @{t.child_bot_username or 'no_username'} — <b>{t.status}</b> | 👥 {total} / 📝 {reg} / 💰 {dep}"


def _tenant_button_title(db, t: Tenant) -> str:
    # компактная подпись на кнопке: имя и мини-цифры
    total = db.query(User).filter(User.tenant_id == t.id).count()
    reg = db.query(User).filter(and_(User.tenant_id == t.id, User.step >= UserStep.registered)).count()
    dep = db.query(User).filter(and_(User.tenant_id == t.id, User.step == UserStep.deposited)).count()
    name = f"@{t.child_bot_username}" if t.child_bot_username else f"Bot #{t.id}"
    return f"{name}  |  👥{total} 📝{reg} 💰{dep}"


# --------------------- /ga (главное) ---------------------
@router.message(Command("ga"))
async def ga_menu(msg: Message):
    if not _is_ga(msg.from_user.id):
        return

    db = SessionLocal()
    try:
        tenants = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted).order_by(Tenant.id.desc()).all()
        ids = [t.id for t in tenants]

        tenants_total = len(tenants)
        tenants_active = sum(1 for t in tenants if t.status == TenantStatus.active)
        tenants_paused = sum(1 for t in tenants if t.status == TenantStatus.paused)

        users_total = db.query(User).filter(User.tenant_id.in_(ids)).count() if ids else 0
        users_reg = db.query(User).filter(and_(User.tenant_id.in_(ids), User.step >= UserStep.registered)).count() if ids else 0
        users_dep = db.query(User).filter(and_(User.tenant_id.in_(ids), User.step == UserStep.deposited)).count() if ids else 0

        text = (
            "<b>GA панель</b>\n\n"
            f"Клиенты: {tenants_total} (активных: {tenants_active}, на паузе: {tenants_paused})\n"
            f"Пользователи: {users_total} (📝 {users_reg}, 💰 {users_dep})\n\n"
            "Выбери бота:"
        )

        # Кнопки по ботам (пагинация простая: по 8 в столбик)
        rows = []
        per = 8
        for t in tenants[:per]:
            rows.append([
                InlineKeyboardButton(text=_tenant_button_title(db, t), callback_data=f"ga:go:{t.id}")
            ])

        if len(tenants) > per:
            rows.append([InlineKeyboardButton(text="📋 Все боты", callback_data="ga:list:1")])

        rows.append([InlineKeyboardButton(text="🧨 Пурж удалённых", callback_data="ga:purge_deleted")])

        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        await msg.answer(text, reply_markup=kb)
    finally:
        db.close()


# --------------------- список (пагинация) ---------------------
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
            await _safe_edit(cb, "Клиентов пока нет.")
            await cb.answer(); return

        rows = []
        for t in tenants:
            rows.append([InlineKeyboardButton(text=_tenant_button_title(db, t), callback_data=f"ga:go:{t.id}")])

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"ga:list:{page-1}"))
        if page*per < total:
            nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"ga:list:{page+1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")])

        await _safe_edit(cb, "<b>Список ботов</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


@router.callback_query(F.data == "ga:home")
async def ga_home(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    # просто перегенерим /ga
    fake = Message(
        message_id=cb.message.message_id,
        date=cb.message.date,
        chat=cb.message.chat,
        message_thread_id=None
    )
    fake.from_user = cb.from_user
    await ga_menu(fake)  # пересоберём главную
    await cb.answer()


# --------------------- карточка бота ---------------------
@router.callback_query(F.data.startswith("ga:go:"))
async def ga_go(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])

    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            await cb.answer("Не найден"); return

        line = _t_line(db, t)
        txt = (
            f"{line}\n"
            f"Владелец: <code>{t.owner_tg_id}</code>\n"
            f"Support: {t.support_url or '—'}\n"
            f"Ref: {t.ref_link or '—'}\n"
            f"Deposit: {t.deposit_link or '—'}\n"
            f"MiniApp: {t.miniapp_url or '—'}\n"
            f"Channel: {t.channel_url or '—'}"
        )
        rows = [
            [InlineKeyboardButton(
                text=("⏸ Пауза" if t.status == TenantStatus.active else "▶️ Запуск"),
                callback_data=f"ga:toggle:{t.id}"
            )],
            [InlineKeyboardButton(text="🔁 Постбэки", callback_data=f"ga:pb:{t.id}")],
            [InlineKeyboardButton(text="🧹 Очистка БД (жёстко)", callback_data=f"ga:clean:pick:{t.id}")],
            [InlineKeyboardButton(text="🗑 Удалить бота", callback_data=f"ga:del:{t.id}")],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="ga:list:1")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")],
        ]
        await _safe_edit(cb, txt, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


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
        await ga_go(cb)
    finally:
        db.close()


# --------------------- постбэки ---------------------
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
            f"Постбэки для @{t.child_bot_username or t.id}\n\n"
            f"Регистрация:\n<code>{reg}</code>\n"
            f"Депозит:\n<code>{dep}</code>\n\n"
            "PP макросы:\n"
            "Регистрация: click_id→click_id, trader_id→trader_id\n"
            "Депозит: click_id→click_id, trader_id→trader_id, sumdep→sum"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к боту", callback_data=f"ga:go:{t.id}")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")],
        ])
        await _safe_edit(cb, txt, kb)
        await cb.answer()
    finally:
        db.close()


# --------------------- удаление бота ---------------------
@router.callback_query(F.data.startswith("ga:del:"))
async def ga_del(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    tid = int(cb.data.split(":")[2])

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"ga:delc:{tid}")],
        [InlineKeyboardButton(text="↩️ Отмена", callback_data=f"ga:go:{tid}")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")],
    ])
    await _safe_edit(cb, f"Удалить клиента #{tid} полностью? Это необратимо.", kb)
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
            await _safe_edit(cb, "Клиент уже отсутствует."); await cb.answer(); return

        # ставим на паузу (остановка поллинга в раннере)
        if t.status == TenantStatus.active:
            t.status = TenantStatus.paused
            db.commit()

        # ЧИСТИМ ВСЁ, чтобы не было «хвостов»
        db.query(Postback).filter(Postback.tenant_id == t.id).delete(synchronize_session=False)
        db.query(User).filter(User.tenant_id == t.id).delete(synchronize_session=False)
        db.query(TenantText).filter(TenantText.tenant_id == t.id).delete(synchronize_session=False)
        db.query(TenantConfig).filter(TenantConfig.tenant_id == t.id).delete(synchronize_session=False)
        db.commit()

        # Удаляем сам тенант
        db.delete(t)
        db.commit()

        # После этого владелец сможет подключить новый бот — никаких ЧС/блокировок нет.
        await _safe_edit(cb, f"✅ Клиент #{tid} полностью удалён.", None)
        await cb.answer("Удалено")
        return

    except Exception as e:
        db.rollback()
        # Фолбэк: пометим как deleted (на всякий случай)
        try:
            t = db.query(Tenant).filter(Tenant.id == tid).first()
            if t:
                t.status = TenantStatus.deleted
                db.commit()
        except Exception:
            db.rollback()
        await _safe_edit(cb, f"⚠️ Не удалось полностью удалить. Помечен как deleted.\nОшибка: <code>{e}</code>")
        await cb.answer("Помечен как deleted")
    finally:
        db.close()


# --------------------- Жёсткая очистка БД (без удаления тенанта) ---------------------
@router.callback_query(F.data.startswith("ga:clean:"))
async def ga_clean_router(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return

    parts = cb.data.split(":")
    # ga:clean:pick:{id}
    if len(parts) == 4 and parts[2] == "pick":
        tid = int(parts[3])
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tid).first()
            if not t:
                await cb.answer("Не найден"); return
            txt = (
                f"{_t_line(db, t)}\n\n"
                "ЖЁСТКАЯ очистка:\n— удалит пользователей и постбэки\n— удалит контент и конфиги\n"
                "— обнулит support/ref/deposit/miniapp/channel в карточке клиента\n"
                "Клиент останется (как «с нуля»)."
            )
        finally:
            db.close()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Подтвердить очистку", callback_data=f"ga:clean:run:{tid}")],
            [InlineKeyboardButton(text="↩️ Назад", callback_data=f"ga:go:{tid}")],
        ])
        await _safe_edit(cb, txt, kb)
        await cb.answer()
        return

    if len(parts) == 4 and parts[2] == "run":
        tid = int(parts[3])
        db = SessionLocal()
        try:
            t = db.query(Tenant).filter(Tenant.id == tid).first()
            if not t:
                await _safe_edit(cb, "Клиент не найден."); await cb.answer(); return

            db.query(Postback).filter(Postback.tenant_id == tid).delete(synchronize_session=False)
            db.query(User).filter(User.tenant_id == tid).delete(synchronize_session=False)
            db.query(TenantText).filter(TenantText.tenant_id == tid).delete(synchronize_session=False)
            db.query(TenantConfig).filter(TenantConfig.tenant_id == tid).delete(synchronize_session=False)

            t.support_url = None
            t.ref_link = None
            t.deposit_link = None
            t.miniapp_url = None
            t.channel_url = None

            db.commit()
            await _safe_edit(cb, f"✅ Жёсткая очистка БД клиента #{tid} выполнена.",
                             InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="⬅️ Назад к боту", callback_data=f"ga:go:{tid}")],
                                 [InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")]
                             ]))
            await cb.answer("Готово")
        except Exception as e:
            db.rollback()
            await _safe_edit(cb, f"❌ Ошибка очистки: <code>{e}</code>")
            await cb.answer("Ошибка")
        finally:
            db.close()
        return

    # fallback
    await ga_home(cb)


# --------------------- Пурж удалённых тенантов ---------------------
@router.callback_query(F.data == "ga:purge_deleted")
async def ga_purge_deleted(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    db = SessionLocal()
    try:
        count = db.query(Tenant).filter(Tenant.status == TenantStatus.deleted).count()
    finally:
        db.close()

    if count == 0:
        await _safe_edit(cb, "Удалённых клиентов нет — чистить нечего.")
        await cb.answer(); return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Подтвердить пурж ({count})", callback_data="ga:purge_deleted_run")],
        [InlineKeyboardButton(text="↩️ Отмена", callback_data="ga:home")],
    ])
    await _safe_edit(cb,
                     f"Найдено удалённых клиентов: <b>{count}</b>.\n"
                     f"Все они будут полностью удалены вместе с данными.",
                     kb)
    await cb.answer()


@router.callback_query(F.data == "ga:purge_deleted_run")
async def ga_purge_deleted_run(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return

    db = SessionLocal()
    purged = 0
    failed = 0
    errors = []
    try:
        tenants = db.query(Tenant).filter(Tenant.status == TenantStatus.deleted).all()
        for t in tenants:
            try:
                db.query(Postback).filter(Postback.tenant_id == t.id).delete(synchronize_session=False)
                db.query(User).filter(User.tenant_id == t.id).delete(synchronize_session=False)
                db.query(TenantText).filter(TenantText.tenant_id == t.id).delete(synchronize_session=False)
                db.query(TenantConfig).filter(TenantConfig.tenant_id == t.id).delete(synchronize_session=False)
                db.commit()

                db.delete(t)
                db.commit()
                purged += 1
            except Exception as e:
                db.rollback()
                failed += 1
                errors.append(f"#{t.id}: {e}")
    finally:
        db.close()

    details = ""
    if failed:
        joined = "\n".join(errors[:10])
        details = f"\n\nОшибки ({failed}):\n<code>{joined}</code>"
        if failed > 10:
            details += "\n…"

    await _safe_edit(cb, f"🧨 Пурж завершён.\nУдалено: <b>{purged}</b>\nОшибок: <b>{failed}</b>{details}")
    await cb.answer("Готово")
