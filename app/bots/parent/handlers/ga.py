from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import and_
import os
import time
import json

from app.settings import settings
from app.db import SessionLocal
from app.models import (
    Tenant, TenantStatus,
    User, UserStep,
    TenantText, TenantConfig, Postback,
)

router = Router()

# где runner следит за bump-файлом
BUMPER_PATH = "/tmp/pb_runner.bump"
STATUS_DIR = "/tmp/children_status"

PAGE_SIZE = 5  # по 5 ботов на страницу


# --------------------- helpers ---------------------
def _is_ga(uid: int) -> bool:
    return uid in settings.ga_admin_ids


async def _safe_edit(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup | None = None):
    try:
        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        try:
            # если текст менять нельзя (например, старый), попробуем хотя бы клавиатуру
            await cb.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass


def _t_counts(db, tenant_id: int) -> tuple[int, int, int]:
    total = db.query(User).filter(User.tenant_id == tenant_id).count()
    reg = db.query(User).filter(and_(User.tenant_id == tenant_id, User.step >= UserStep.registered)).count()
    dep = db.query(User).filter(and_(User.tenant_id == tenant_id, User.step == UserStep.deposited)).count()
    return total, reg, dep


def _tenant_button_title(db, t: Tenant) -> str:
    total, reg, dep = _t_counts(db, t.id)
    name = f"@{t.child_bot_username}" if t.child_bot_username else f"Bot #{t.id}"
    return f"{name}  |  👥{total} 📝{reg} 💰{dep}"


def _bump_runner():
    # создаём/обновляем файл, runner увидит mtime и перезапустит детей
    try:
        with open(BUMPER_PATH, "a", encoding="utf-8"):
            pass
        os.utime(BUMPER_PATH, (time.time(), time.time()))
        return True
    except Exception:
        return False


def _read_child_status(tenant_id: int) -> dict:
    """
    Читаем /tmp/children_status/{tenant_id}.json, если есть.
    Возвращаем {"phase": "...", "detail": "...", "ts": 0.0} или пустые поля.
    """
    path = os.path.join(STATUS_DIR, f"{tenant_id}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "phase": data.get("phase", ""),
            "detail": data.get("detail", ""),
            "ts": float(data.get("ts", 0.0)),
        }
    except Exception:
        return {"phase": "", "detail": "", "ts": 0.0}


def _fmt_ts(ts: float) -> str:
    if not ts:
        return "—"
    try:
        lt = time.localtime(ts)
        return time.strftime("%Y-%m-%d %H:%M:%S", lt)
    except Exception:
        return str(ts)


def _channel_display(raw: str | None) -> tuple[str, str]:
    """
    Возвращаем (ident, open_url) для показа канала.
    raw может быть:
      "@name" | "-100..." | "https://t.me/username" | "-100... | https://t.me/+invite"
    """
    if not raw:
        return "—", "—"
    raw = raw.strip()
    ident = raw
    open_url = raw

    # Если формат "-100... | url"
    if " | " in raw:
        left, right = raw.split(" | ", 1)
        ident = left.strip()
        open_url = right.strip()
    else:
        # если "@name" → сделаем ссылку
        if raw.startswith("@"):
            ident = raw
            open_url = f"https://t.me/{raw[1:]}"
        # если "t.me/..." → ссылка есть
        elif "t.me/" in raw:
            ident = raw
            open_url = raw if raw.startswith("http") else "https://" + raw.lstrip("/")

    return ident, open_url


async def _owner_username(bot, owner_id: int) -> str:
    try:
        ch = await bot.get_chat(owner_id)
        if getattr(ch, "username", None):
            return f"@{ch.username}"
    except Exception:
        pass
    return "—"


async def _render_ga_home_text_kb() -> tuple[str, InlineKeyboardMarkup]:
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
            f"Клиентов: <b>{tenants_total}</b>\n"
            f"Активных: <b>{tenants_active}</b> | На паузе: <b>{tenants_paused}</b>\n"
            f"Пользователи: <b>{users_total}</b> (📝 {users_reg}, 💰 {users_dep})\n\n"
            "Выберите бота:"
        )

        rows = []
        for t in tenants[:PAGE_SIZE]:
            rows.append([InlineKeyboardButton(text=_tenant_button_title(db, t), callback_data=f"ga:go:{t.id}")])

        if len(tenants) > PAGE_SIZE:
            rows.append([InlineKeyboardButton(text="📋 Список (стр. 1)", callback_data="ga:list:1")])

        rows.append([InlineKeyboardButton(text="🔄 Перезапустить детей", callback_data="ga:bump")])
        rows.append([InlineKeyboardButton(text="🧨 Пурж удалённых", callback_data="ga:purge_deleted")])

        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        return text, kb
    finally:
        db.close()


# --------------------- FSM для быстрых правок ссылок ---------------------
class GAForm(StatesGroup):
    wait_support = State()
    wait_ref = State()
    wait_dep = State()
    wait_miniapp = State()
    wait_channel = State()


# --------------------- /ga (главное) ---------------------
@router.message(Command("ga"))
async def ga_menu(msg: Message):
    if not _is_ga(msg.from_user.id):
        return
    text, kb = await _render_ga_home_text_kb()
    await msg.answer(text, reply_markup=kb)


# --------------------- список (пагинация по 5) ---------------------
@router.callback_query(F.data.startswith("ga:list:"))
async def ga_list(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    try:
        page = int(cb.data.split(":")[2])
    except Exception:
        page = 1

    db = SessionLocal()
    try:
        q = db.query(Tenant).filter(Tenant.status != TenantStatus.deleted)
        total = q.count()
        tenants = q.order_by(Tenant.id.desc()).offset((page-1)*PAGE_SIZE).limit(PAGE_SIZE).all()
        if not tenants:
            await _safe_edit(cb, "Клиентов пока нет.")
            await cb.answer(); return

        rows = []
        for t in tenants:
            rows.append([InlineKeyboardButton(text=_tenant_button_title(db, t), callback_data=f"ga:go:{t.id}")])

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"ga:list:{page-1}"))
        if page * PAGE_SIZE < total:
            nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"ga:list:{page+1}"))
        if nav:
            rows.append(nav)

        rows.append([InlineKeyboardButton(text="🔄 Перезапустить детей", callback_data="ga:bump")])
        rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")])

        await _safe_edit(cb, "<b>Список ботов</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()
    finally:
        db.close()


@router.callback_query(F.data == "ga:home")
async def ga_home(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    text, kb = await _render_ga_home_text_kb()
    await _safe_edit(cb, text, kb)
    await cb.answer()


# --------------------- bump (перезапуск детей) ---------------------
@router.callback_query(F.data == "ga:bump")
async def ga_bump(cb: CallbackQuery):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return
    ok = _bump_runner()
    await cb.answer("Ok" if ok else "Ошибка")
    await ga_home(cb)


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

        total, reg, dep = _t_counts(db, t.id)
        uname = f"@{t.child_bot_username}" if t.child_bot_username else "—"
        status = t.status

        owner_name = await _owner_username(cb.bot, t.owner_tg_id)
        ch_ident, ch_open = _channel_display(t.channel_url)

        st = _read_child_status(t.id)
        st_line = f"{st.get('phase') or '—'}"
        if st.get("detail"):
            st_line += f" | {st['detail']}"
        if st.get("ts"):
            st_line += f" | { _fmt_ts(st['ts']) }"

        txt = (
            f"<b>Клиент #{t.id}</b>\n"
            f"Имя бота: {uname}\n"
            f"Статус: <b>{status}</b>\n"
            f"Статус ребёнка: <code>{st_line}</code>\n"
            f"Пользователи: 👥 {total}\n"
            f"Регистрации: 📝 {reg}\n"
            f"Депозиты: 💰 {dep}\n"
            f"Владелец: <code>{t.owner_tg_id}</code>, {owner_name}\n\n"
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
            [
                InlineKeyboardButton(text="✏️ Support", callback_data=f"ga:set:support:{t.id}"),
                InlineKeyboardButton(text="✏️ Ref", callback_data=f"ga:set:ref:{t.id}"),
            ],
            [
                InlineKeyboardButton(text="✏️ Deposit", callback_data=f"ga:set:dep:{t.id}"),
                InlineKeyboardButton(text="✏️ MiniApp", callback_data=f"ga:set:miniapp:{t.id}"),
            ],
            [InlineKeyboardButton(text="✏️ Channel", callback_data=f"ga:set:channel:{t.id}")],
            [InlineKeyboardButton(text="🧹 Очистка БД (жёстко)", callback_data=f"ga:clean:pick:{t.id}")],
            [InlineKeyboardButton(text="🗑 Удалить бота", callback_data=f"ga:del:{t.id}")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="ga:home")],
        ]
        # Можно сделать клик по каналу: отдельная кнопка «Открыть канал», если есть URL
        if ch_open and ch_open != "—":
            rows.insert(2, [InlineKeyboardButton(text="🔎 Открыть канал", url=ch_open)])

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
            f"<b>Постбэки для</b> {t.child_bot_username or t.id}\n\n"
            f"📝 Регистрация:\n<code>{reg}</code>\n\n"
            f"💳 Депозит:\n<code>{dep}</code>\n\n"
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

        # ЧИСТИМ ВСЁ
        db.query(Postback).filter(Postback.tenant_id == t.id).delete(synchronize_session=False)
        db.query(User).filter(User.tenant_id == t.id).delete(synchronize_session=False)
        db.query(TenantText).filter(TenantText.tenant_id == t.id).delete(synchronize_session=False)
        db.query(TenantConfig).filter(TenantConfig.tenant_id == t.id).delete(synchronize_session=False)
        db.commit()

        # Удаляем сам тенант
        db.delete(t)
        db.commit()

        await _safe_edit(cb, f"✅ Клиент #{tid} полностью удалён.", None)
        await cb.answer("Удалено")
        return

    except Exception as e:
        db.rollback()
        # Фолбэк: пометим как deleted
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
            total, reg, dep = _t_counts(db, t.id)
            txt = (
                f"<b>Очистка клиента #{tid}</b>\n"
                f"Пользователи: 👥 {total} | 📝 {reg} | 💰 {dep}\n\n"
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


# --------------------- Быстрые правки ссылок (FSM) ---------------------
@router.callback_query(F.data.startswith("ga:set:"))
async def ga_set_router(cb: CallbackQuery, state: FSMContext):
    if not _is_ga(cb.from_user.id):
        await cb.answer(); return

    parts = cb.data.split(":")
    # ga:set:{field}:{tenant_id}
    if len(parts) != 4:
        await cb.answer(); return

    field = parts[2]
    tid = int(parts[3])

    await state.update_data(ga_tid=tid)

    prompts = {
        "support": "Пришлите <b>новый Support URL</b> одним сообщением.\n\n⬅️ /ga — отмена.",
        "ref": "Пришлите <b>новую реферальную ссылку</b> одним сообщением.\n\n⬅️ /ga — отмена.",
        "dep": "Пришлите <b>ссылку для депозита</b> одним сообщением.\n\n⬅️ /ga — отмена.",
        "miniapp": "Пришлите <b>Web-app URL</b> одним сообщением.\n\n⬅️ /ga — отмена.",
        "channel": (
            "Если <b>публичный канал</b> — отправьте <code>@username</code> или ссылку на канал/группу "
            "(например: https://t.me/username).\n\n"
            "Если <b>приватный канал</b> — отправьте ID и инвайт-ссылку в формате:\n"
            "<code>-1001234567890 | https://t.me/+invite</code>\n\n"
            "⚠️ Важно: бот должен быть участником (в канале — админом)."
        ),
    }

    form_map = {
        "support": GAForm.wait_support,
        "ref": GAForm.wait_ref,
        "dep": GAForm.wait_dep,
        "miniapp": GAForm.wait_miniapp,
        "channel": GAForm.wait_channel,
    }
    if field not in form_map:
        await cb.answer(); return

    await state.set_state(form_map[field])
    await _safe_edit(cb, prompts[field])
    await cb.answer()


async def _apply_field(tid: int, field: str, value: str) -> bool:
    db = SessionLocal()
    try:
        t = db.query(Tenant).filter(Tenant.id == tid).first()
        if not t:
            return False
        if field == "support":
            t.support_url = value
        elif field == "ref":
            t.ref_link = value
        elif field == "dep":
            t.deposit_link = value
        elif field == "miniapp":
            t.miniapp_url = value
        elif field == "channel":
            t.channel_url = value
        else:
            return False
        db.commit()
        return True
    except Exception:
        db.rollback()
        return False
    finally:
        db.close()


@router.message(GAForm.wait_support)
async def ga_set_support(msg: Message, state: FSMContext):
    if not _is_ga(msg.from_user.id):
        return
    data = await state.get_data()
    tid = data.get("ga_tid")
    ok = await _apply_field(tid, "support", (msg.text or "").strip())
    await state.clear()
    await msg.answer("✅ Support URL обновлён." if ok else "❌ Не удалось обновить Support URL.")
    # показать карточку
    fake_cb = CallbackQuery(id="0", from_user=msg.from_user, chat_instance="", message=msg)
    fake_cb.data = f"ga:go:{tid}"
    await ga_go(fake_cb)  # type: ignore[arg-type]


@router.message(GAForm.wait_ref)
async def ga_set_ref(msg: Message, state: FSMContext):
    if not _is_ga(msg.from_user.id):
        return
    data = await state.get_data()
    tid = data.get("ga_tid")
    ok = await _apply_field(tid, "ref", (msg.text or "").strip())
    await state.clear()
    await msg.answer("✅ Реферальная ссылка обновлена." if ok else "❌ Не удалось обновить ссылку.")
    fake_cb = CallbackQuery(id="0", from_user=msg.from_user, chat_instance="", message=msg)
    fake_cb.data = f"ga:go:{tid}"
    await ga_go(fake_cb)  # type: ignore[arg-type]


@router.message(GAForm.wait_dep)
async def ga_set_dep(msg: Message, state: FSMContext):
    if not _is_ga(msg.from_user.id):
        return
    data = await state.get_data()
    tid = data.get("ga_tid")
    ok = await _apply_field(tid, "dep", (msg.text or "").strip())
    await state.clear()
    await msg.answer("✅ Ссылка для депозита обновлена." if ok else "❌ Не удалось обновить ссылку.")
    fake_cb = CallbackQuery(id="0", from_user=msg.from_user, chat_instance="", message=msg)
    fake_cb.data = f"ga:go:{tid}"
    await ga_go(fake_cb)  # type: ignore[arg-type]


@router.message(GAForm.wait_miniapp)
async def ga_set_miniapp(msg: Message, state: FSMContext):
    if not _is_ga(msg.from_user.id):
        return
    data = await state.get_data()
    tid = data.get("ga_tid")
    ok = await _apply_field(tid, "miniapp", (msg.text or "").strip())
    await state.clear()
    await msg.answer("✅ Web-app URL обновлён." if ok else "❌ Не удалось обновить Web-app URL.")
    fake_cb = CallbackQuery(id="0", from_user=msg.from_user, chat_instance="", message=msg)
    fake_cb.data = f"ga:go:{tid}"
    await ga_go(fake_cb)  # type: ignore[arg-type]


@router.message(GAForm.wait_channel)
async def ga_set_channel(msg: Message, state: FSMContext):
    if not _is_ga(msg.from_user.id):
        return
    data = await state.get_data()
    tid = data.get("ga_tid")
    ok = await _apply_field(tid, "channel", (msg.text or "").strip())
    await state.clear()
    await msg.answer("✅ Канал обновлён." if ok else "❌ Не удалось обновить канал.")
    fake_cb = CallbackQuery(id="0", from_user=msg.from_user, chat_instance="", message=msg)
    fake_cb.data = f"ga:go:{tid}"
    await ga_go(fake_cb)  # type: ignore[arg-type]
