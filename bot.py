"""
Бот поддержки канала Umbrellafan — один файл, без Redis и Docker.

Каждый пользователь, начавший диалог, получает свою тему (топик) в вашей группе.
Админы отвечают прямо в теме. Ответы, правки и реакции работают в обе стороны.

Запуск:
    pip install -r requirements.txt
    BOT_TOKEN=123:abc GROUP_ID=-100123456789 python bot.py
(или впишите значения в файл .env рядом: BOT_TOKEN=... и GROUP_ID=...)
"""
import asyncio
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict, deque
from contextlib import suppress
from html import escape
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, Filter
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton as Btn,
    InlineKeyboardMarkup as Markup,
    Message,
    MessageReactionUpdated,
    ReactionTypeCustomEmoji,
    ReactionTypeEmoji,
    ReplyParameters,
)

# ───────────────────────────── НАСТРОЙКИ ─────────────────────────────
_env = Path(__file__).with_name(".env")
if _env.exists():
    for _line in _env.read_text(encoding="utf-8").splitlines():
        if "=" in _line and not _line.lstrip().startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip("\"'"))

TOKEN = os.getenv("BOT_TOKEN", "")            # токен от @BotFather
GROUP_ID = int(os.getenv("GROUP_ID", "0"))    # ID группы с включёнными темами (-100...)
BRAND = os.getenv("BRAND", "Umbrellafan")     # название канала
DB_PATH = os.getenv("DB_PATH", "support.db")
# Владельцы бота (могут назначать админов): ID через запятую. Необязательно:
# создатель группы автоматически считается владельцем.
OWNER_IDS = {int(x) for x in os.getenv("OWNER_IDS", "").replace(" ", "").split(",") if x}

# Антифлуд: FLOOD_COUNT сообщений за FLOOD_WINDOW секунд → спам-блок на SPAM_BLOCK секунд
FLOOD_COUNT, FLOOD_WINDOW, SPAM_BLOCK = 10, 10, 10

# ────────────────────────────── ТЕКСТЫ ───────────────────────────────
B = escape(BRAND)
T = {
    "menu": f"☂️ <b>Поддержка канала {B}</b>\n\nВыберите, что вам нужно:",
    "why": (
        "❓ <b>Почему не стоит писать просто так</b>\n\n"
        "В поддержке отвечают живые люди, и их время ограничено. Сообщения вроде "
        "«привет», «есть кто?», «тест» и бессмысленный флуд только мешают тем, "
        "у кого реальный вопрос.\n\n"
        "✅ <b>Пишите, если:</b>\n"
        "• хотите разместить рекламу в канале (напишите, что рекламируете, "
        "приложите ссылку, укажите сроки и бюджет);\n"
        "• есть вопрос или жалоба по каналу;\n"
        "• хотите предложить сотрудничество или идею.\n\n"
        "❌ <b>Не пишите:</b> просто так, ради флуда, с оскорблениями или повторяя одно и то же.\n\n"
        "Формулируйте суть сразу одним сообщением — так ответ придёт быстрее."
    ),
    "rules": (
        "📜 <b>Правила общения</b>\n\n"
        "1. Не спамьте и не флудите: много сообщений подряд = блок.\n"
        "2. Не оскорбляйте модераторов и других людей.\n"
        "3. Не отправляйте рекламу, ссылки и рассылки, которые мы не просили.\n"
        "4. Не дублируйте одно и то же сообщение, ждите ответа.\n"
        "5. Пишите по теме и понятно: что случилось и что нужно.\n"
        "6. Не отправляйте запрещённый и вредоносный контент.\n\n"
        "За нарушение правил выдаётся мут или бан."
    ),
    "dialog_on": (
        "💬 <b>Диалог с модератором начат</b>\n\n"
        "Пишите своё сообщение — оно уйдёт модераторам, ответ придёт сюда.\n"
        "Когда вопрос решён, завершите диалог кнопкой ниже."
    ),
    "dialog_off": "✅ <b>Диалог завершён</b>\n\nЕсли понадобится помощь, нажмите /start.",
    "need_dialog": "💬 Чтобы написать модератору, сначала начните диалог.",
    "first": "✅ <b>Сообщение отправлено</b>\n\nМодератор ответит, как только освободится.",
    "flood": (
        f"🛑 <b>Остановитесь, не флудите и не спамьте!</b>\n\n"
        f"Спам-блок на {SPAM_BLOCK} секунд. За повторный флуд можно получить мут или бан."
    ),
    "muted_user": "🔇 <b>Вы в муте</b> до {until}. Ваши сообщения не доставляются модераторам.",
    "unmuted_user": "🔊 Мут снят, вы снова можете писать.",
    "fail": "⚠️ Не удалось доставить сообщение. Попробуйте позже.",
    "blocked_bot": "⚠️ <b>Не доставлено</b>: пользователь заблокировал бота.",
    "not_sent": "⚠️ <b>Не доставлено</b>: Telegram не принял сообщение.",
    "to_banned": "🚫 Пользователь забанен, сообщение не отправлено. Снять бан: /unban",
    "to_muted": "🔇 Пользователь в муте до {until}, сообщение отправлено, но он не сможет ответить.",
    "ban_on": "🚫 Пользователь забанен. Бот больше ему не отвечает и ничего не присылает в тему.",
    "ban_off": "🔓 Пользователь разбанен.",
    "mute_on": "🔇 Пользователь в муте до {until}. Его сообщения не приходят в тему.",
    "mute_off": "🔊 Мут снят.",
    "silent_on": "🔕 Тихий режим включён. Сообщения из темы не уходят пользователю.",
    "silent_off": "🔔 Тихий режим выключен. Сообщения снова уходят пользователю.",
    "deleted": "✅ Сообщение удалено с обеих сторон.",
    "del_hint": "⚠️ Ответьте командой /delete на нужное сообщение.",
    "del_fail": "⚠️ Не удалось удалить (можно в течение 48 часов).",
    "no_user": "⚠️ Пользователь не найден. Используйте команду в его теме или укажите ID.",
    "dialog_started_note": "▶️ Пользователь начал диалог.",
    "dialog_ended_note": "⏹ Пользователь завершил диалог.",
    "card": (
        "👤 <b>Новый пользователь</b>\n\n"
        "<b>Имя:</b> {name}\n<b>ID:</b> <code>{id}</code>\n<b>Username:</b> {username}\n"
        "<b>Регистрация:</b> {created}\n\n"
        "Всё, что админы пишут в этой теме, уходит пользователю.\n\n"
        "/ban, /unban — бан (бот молчит и ничего не шлёт)\n"
        "/mute [30m|2h|1d], /unmute — временный мут\n"
        "/silent — тихий режим (не отправлять пользователю)\n"
        "/delete — удалить сообщение (ответом на него)\n"
        "/info — данные пользователя"
    ),
    "info": (
        "👤 {name}\n\n<b>ID:</b> <code>{id}</code>\n<b>Username:</b> {username}\n"
        "<b>Диалог:</b> {dialog}\n<b>Бан:</b> {banned}\n<b>Мут:</b> {muted}\n"
        "<b>Тихий режим:</b> {silent}\n<b>Регистрация:</b> {created}"
    ),
}


def menu_kb() -> Markup:
    return Markup(inline_keyboard=[
        [Btn(text="❓ Почему не стоит писать просто так", callback_data="why")],
        [Btn(text="💬 Начать диалог с модератором", callback_data="dialog")],
        [Btn(text="📜 Правила общения", callback_data="rules")],
    ])


def back_kb(dialog_btn: bool = False) -> Markup:
    rows = []
    if dialog_btn:
        rows.append([Btn(text="💬 Начать диалог с модератором", callback_data="dialog")])
    rows.append([Btn(text="⬅️ Назад", callback_data="menu")])
    return Markup(inline_keyboard=rows)


END_KB = Markup(inline_keyboard=[[Btn(text="❌ Завершить диалог", callback_data="end")]])

# ──────────────────────────── БАЗА ДАННЫХ ────────────────────────────
db = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
db.row_factory = sqlite3.Row
db.executescript(
    """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY, name TEXT, username TEXT, thread_id INTEGER,
        banned INTEGER DEFAULT 0, silent INTEGER DEFAULT 0, welcomed INTEGER DEFAULT 0,
        created TEXT, silent_msg INTEGER
    );
    CREATE TABLE IF NOT EXISTS links(
        chat_id INTEGER, msg_id INTEGER, to_chat INTEGER, to_msg INTEGER, ts INTEGER,
        PRIMARY KEY(chat_id, msg_id)
    );
    CREATE TABLE IF NOT EXISTS admins(id INTEGER PRIMARY KEY);
    CREATE INDEX IF NOT EXISTS users_thread ON users(thread_id);
    """
)
for _col in ("dialog INTEGER DEFAULT 0", "muted_until INTEGER DEFAULT 0"):  # миграция старой базы
    with suppress(sqlite3.OperationalError):
        db.execute(f"ALTER TABLE users ADD COLUMN {_col}")


def get_user(uid: int):
    return db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def user_by_thread(thread_id: int):
    return db.execute("SELECT * FROM users WHERE thread_id=?", (thread_id,)).fetchone()


def upsert_user(u) -> sqlite3.Row:
    username = f"@{u.username}" if u.username else "—"
    if get_user(u.id) is None:
        db.execute(
            "INSERT INTO users(id,name,username,created) VALUES(?,?,?,?)",
            (u.id, u.full_name, username, time.strftime("%Y-%m-%d %H:%M")),
        )
    else:
        db.execute("UPDATE users SET name=?, username=? WHERE id=?", (u.full_name, username, u.id))
    return get_user(u.id)


def set_field(uid: int, **fields) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    db.execute(f"UPDATE users SET {cols} WHERE id=?", (*fields.values(), uid))


def add_link(a_chat: int, a_msg: int, b_chat: int, b_msg: int) -> None:
    ts = int(time.time())
    db.execute("INSERT OR REPLACE INTO links VALUES(?,?,?,?,?)", (a_chat, a_msg, b_chat, b_msg, ts))
    db.execute("INSERT OR REPLACE INTO links VALUES(?,?,?,?,?)", (b_chat, b_msg, a_chat, a_msg, ts))


def get_link(chat_id: int, msg_id: int):
    r = db.execute("SELECT to_chat, to_msg FROM links WHERE chat_id=? AND msg_id=?", (chat_id, msg_id)).fetchone()
    return (r["to_chat"], r["to_msg"]) if r else None


def is_muted(row) -> bool:
    return (row["muted_until"] or 0) > time.time()


def fmt_until(ts: int) -> str:
    return time.strftime("%d.%m %H:%M", time.localtime(ts))


# ─────────────────────────────── БОТ ──────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("support")

bot = Bot(TOKEN or "0:placeholder", default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True))
dp = Dispatcher()

_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_hits: dict[int, deque] = defaultdict(deque)
_spam_until: dict[int, float] = {}
_notice: dict[int, float] = {}
_roles: dict[int, tuple[float, str]] = {}


# ───────────────────────────── Антифлуд ───────────────────────────────
def flood_state(uid: int) -> str:
    """'ok' — можно, 'blocked' — идёт спам-блок (молча игнорируем), 'trigger' — флуд только что обнаружен."""
    now = time.monotonic()
    if _spam_until.get(uid, 0) > now:
        return "blocked"
    q = _hits[uid]
    while q and now - q[0] > FLOOD_WINDOW:
        q.popleft()
    q.append(now)
    if len(q) >= FLOOD_COUNT:
        q.clear()
        _spam_until[uid] = now + SPAM_BLOCK
        return "trigger"
    return "ok"


# ───────────────────────────── Админы ─────────────────────────────────
async def get_role(uid: int) -> str:
    """'owner' | 'admin' | '' — владелец, админ бота или админ группы."""
    if uid in OWNER_IDS:
        return "owner"
    cached = _roles.get(uid)
    if cached and time.time() - cached[0] < 60:
        return cached[1]
    role = ""
    if db.execute("SELECT 1 FROM admins WHERE id=?", (uid,)).fetchone():
        role = "admin"
    try:
        member = await bot.get_chat_member(GROUP_ID, uid)
        if member.status == ChatMemberStatus.CREATOR:
            role = "owner"
        elif member.status == ChatMemberStatus.ADMINISTRATOR and not role:
            role = "admin"
    except Exception:
        pass
    _roles[uid] = (time.time(), role)
    return role


async def message_role(m: Message) -> str:
    if m.sender_chat and m.sender_chat.id == GROUP_ID:  # админ, пишущий анонимно
        return "admin"
    return await get_role(m.from_user.id) if m.from_user else ""


class IsAdmin(Filter):
    async def __call__(self, m: Message) -> bool:
        return bool(await message_role(m))


class IsOwner(Filter):
    async def __call__(self, m: Message) -> bool:
        return await message_role(m) == "owner"


# ───────────────────────────── Хелперы ────────────────────────────────
def reply_to(m: Message, target_chat: int) -> ReplyParameters | None:
    if not m.reply_to_message:
        return None
    link = get_link(m.chat.id, m.reply_to_message.message_id)
    if link and link[0] == target_chat:
        return ReplyParameters(message_id=link[1], allow_sending_without_reply=True)
    return None


async def copy_any(m: Message, chat_id: int, thread_id: int | None, reply) -> Message:
    if m.forward_origin:
        return await bot.forward_message(chat_id, m.chat.id, m.message_id, message_thread_id=thread_id)
    return await bot.copy_message(
        chat_id, m.chat.id, m.message_id, message_thread_id=thread_id, reply_parameters=reply
    )


async def ensure_topic(u, broken: int | None = None) -> int:
    """ID темы пользователя; создаёт её при необходимости."""
    async with _locks[u.id]:
        row = upsert_user(u)
        if row["thread_id"] and row["thread_id"] != broken:
            return row["thread_id"]
        name = (u.full_name or "")[:128] or f"User {u.id}"
        try:
            topic = await bot.create_forum_topic(GROUP_ID, name)
        except TelegramBadRequest as ex:
            if "TITLE_EMPTY" not in ex.message and "non-empty" not in ex.message:
                raise
            topic = await bot.create_forum_topic(GROUP_ID, f"User {u.id}")
        set_field(u.id, thread_id=topic.message_thread_id, silent=0, silent_msg=None)
        row = get_user(u.id)
        card = await bot.send_message(
            GROUP_ID,
            T["card"].format(
                name=f'<a href="tg://user?id={u.id}">{escape(row["name"])}</a>',
                id=u.id, username=escape(row["username"]), created=row["created"],
            ),
            message_thread_id=topic.message_thread_id,
        )
        with suppress(TelegramBadRequest):
            await card.pin(disable_notification=True)
        log.info("topic %s created for user %s", topic.message_thread_id, u.id)
        return topic.message_thread_id


async def note_in_topic(row, text: str) -> None:
    if row["thread_id"]:
        with suppress(Exception):
            await bot.send_message(GROUP_ID, text, message_thread_id=row["thread_id"])


# ─────────────────── ЛИЧКА: меню и кнопки ────────────────────────────
@dp.message(Command("start"), F.chat.type == "private")
async def on_start(m: Message):
    if not m.from_user:
        return
    if upsert_user(m.from_user)["banned"]:
        return
    await m.answer(T["menu"], reply_markup=menu_kb())


async def show(c: CallbackQuery, text: str, kb: Markup) -> None:
    with suppress(TelegramBadRequest):
        await c.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.in_({"menu", "why", "rules", "dialog", "end"}))
async def on_button(c: CallbackQuery):
    row = upsert_user(c.from_user)
    if row["banned"] or not c.message:
        return await c.answer()
    act = c.data
    if act == "menu":
        await show(c, T["menu"], menu_kb())
    elif act == "why":
        await show(c, T["why"], back_kb(dialog_btn=True))
    elif act == "rules":
        await show(c, T["rules"], back_kb(dialog_btn=True))
    elif act == "dialog":
        if is_muted(row):
            return await c.answer(f"Вы в муте до {fmt_until(row['muted_until'])}", show_alert=True)
        set_field(c.from_user.id, dialog=1, welcomed=0)
        await show(c, T["dialog_on"], END_KB)
        await note_in_topic(row, T["dialog_started_note"])
    elif act == "end":
        set_field(c.from_user.id, dialog=0)
        await show(c, T["dialog_off"], Markup(inline_keyboard=[]))
        await note_in_topic(row, T["dialog_ended_note"])
    await c.answer()


# ─────────────── ЛИЧКА: пользователь → тема ──────────────────────────
@dp.message(Command("delete"), F.chat.type == "private")
async def on_user_delete(m: Message):
    r = m.reply_to_message
    link = get_link(m.chat.id, r.message_id) if r and r.from_user and not r.from_user.is_bot else None
    if not link:
        return await m.reply(T["del_hint"])
    try:
        await bot.delete_message(*link)
        await r.delete()
        await m.delete()
    except TelegramBadRequest:
        await m.reply(T["del_fail"])


@dp.message(F.chat.type == "private")
async def on_user_message(m: Message):
    if not m.from_user:
        return
    uid = m.from_user.id
    row = upsert_user(m.from_user)
    if row["banned"]:
        return  # бан: бот полностью молчит

    state = flood_state(uid)
    if state == "blocked":
        return
    if state == "trigger":
        return await m.reply(T["flood"])

    if is_muted(row):
        if time.time() - _notice.get(uid, 0) > 60:  # не чаще раза в минуту
            _notice[uid] = time.time()
            await m.reply(T["muted_user"].format(until=fmt_until(row["muted_until"])))
        return
    if not row["dialog"]:
        return await m.answer(T["need_dialog"], reply_markup=menu_kb())

    try:
        thread = await ensure_topic(m.from_user)
        reply = reply_to(m, GROUP_ID)
        try:
            sent = await copy_any(m, GROUP_ID, thread, reply)
        except TelegramBadRequest as ex:
            if "thread not found" not in ex.message.lower():
                raise
            thread = await ensure_topic(m.from_user, broken=thread)  # тему удалили — создаём новую
            sent = await copy_any(m, GROUP_ID, thread, None)
        add_link(m.chat.id, m.message_id, GROUP_ID, sent.message_id)
        if not row["welcomed"]:
            set_field(uid, welcomed=1)
            await m.answer(T["first"], reply_markup=END_KB)
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after)
        await m.reply(T["fail"])
    except Exception:
        log.exception("доставка в группу не удалась (проверьте GROUP_ID, темы и права бота)")
        await m.reply(T["fail"])


@dp.edited_message(F.chat.type == "private")
async def on_user_edit(m: Message):
    if not m.from_user:
        return
    row = get_user(m.from_user.id)
    if row and not row["banned"] and not is_muted(row):
        await sync_edit(m)


# ─────────────── ГРУППА: команды админов ─────────────────────────────
def in_topic_cmd(name: str):
    return Command(name), F.chat.id == GROUP_ID, F.message_thread_id, IsAdmin()


def cmd_args(m: Message) -> list[str]:
    return (m.text or "").split()[1:]


def resolve_target(m: Message, args: list[str]):
    for a in args:
        if a.lstrip("-").isdigit() and len(a) >= 6:
            return get_user(int(a))
    return user_by_thread(m.message_thread_id) if m.message_thread_id else None


def parse_duration(args: list[str]) -> int:
    """Секунды: 30m / 2h / 1d / 15 (минуты); по умолчанию 1 час."""
    for a in args:
        mt = re.fullmatch(r"(\d+)([mмhчdд]?)", a.lower())
        if mt and not (a.isdigit() and len(a) >= 6):
            unit = {"": 60, "m": 60, "м": 60, "h": 3600, "ч": 3600, "d": 86400, "д": 86400}[mt.group(2)]
            return max(60, min(int(mt.group(1)) * unit, 365 * 86400))
    return 3600


@dp.message(*in_topic_cmd("ban"))
async def on_ban(m: Message):
    u = resolve_target(m, cmd_args(m))
    if not u:
        return await m.reply(T["no_user"])
    set_field(u["id"], banned=1, dialog=0)
    await m.reply(T["ban_on"])  # самому пользователю ничего не отправляем


@dp.message(*in_topic_cmd("unban"))
async def on_unban(m: Message):
    u = resolve_target(m, cmd_args(m))
    if not u:
        return await m.reply(T["no_user"])
    set_field(u["id"], banned=0)
    await m.reply(T["ban_off"])


@dp.message(*in_topic_cmd("mute"))
async def on_mute(m: Message):
    args = cmd_args(m)
    u = resolve_target(m, args)
    if not u:
        return await m.reply(T["no_user"])
    until = int(time.time()) + parse_duration(args)
    set_field(u["id"], muted_until=until, dialog=0)
    with suppress(Exception):
        await bot.send_message(u["id"], T["muted_user"].format(until=fmt_until(until)))
    await m.reply(T["mute_on"].format(until=fmt_until(until)))


@dp.message(*in_topic_cmd("unmute"))
async def on_unmute(m: Message):
    u = resolve_target(m, cmd_args(m))
    if not u:
        return await m.reply(T["no_user"])
    set_field(u["id"], muted_until=0)
    with suppress(Exception):
        await bot.send_message(u["id"], T["unmuted_user"])
    await m.reply(T["mute_off"])


@dp.message(*in_topic_cmd("silent"))
async def on_silent(m: Message):
    u = user_by_thread(m.message_thread_id)
    if not u:
        return await m.reply(T["no_user"])
    if u["silent"]:
        if u["silent_msg"]:
            with suppress(TelegramBadRequest):
                await bot.unpin_chat_message(GROUP_ID, u["silent_msg"])
        set_field(u["id"], silent=0, silent_msg=None)
        return await m.reply(T["silent_off"])
    msg = await m.reply(T["silent_on"])
    with suppress(TelegramBadRequest):
        await msg.pin(disable_notification=True)
    set_field(u["id"], silent=1, silent_msg=msg.message_id)


@dp.message(*in_topic_cmd("info"))
async def on_info(m: Message):
    u = resolve_target(m, cmd_args(m))
    if not u:
        return await m.reply(T["no_user"])
    await m.reply(
        T["info"].format(
            name=f'<a href="tg://user?id={u["id"]}">{escape(u["name"])}</a>',
            id=u["id"], username=escape(u["username"]),
            dialog="открыт" if u["dialog"] else "закрыт",
            banned="да" if u["banned"] else "нет",
            muted=f"до {fmt_until(u['muted_until'])}" if is_muted(u) else "нет",
            silent="включён" if u["silent"] else "выключен",
            created=u["created"],
        )
    )


@dp.message(*in_topic_cmd("delete"))
async def on_admin_delete(m: Message):
    r = m.reply_to_message
    link = get_link(m.chat.id, r.message_id) if r else None
    if not link:
        return await m.reply(T["del_hint"])
    try:
        await bot.delete_message(*link)
        await r.delete()
        await m.reply(T["deleted"])
    except TelegramBadRequest:
        await m.reply(T["del_fail"])


# ── управление админами бота (команды работают в любом месте группы) ──
def target_id(m: Message) -> int | None:
    for a in cmd_args(m):
        if a.lstrip("-").isdigit():
            return int(a)
    r = m.reply_to_message
    if r and r.from_user and not r.from_user.is_bot:
        return r.from_user.id
    return None


@dp.message(Command("addadmin"), F.chat.id == GROUP_ID, IsOwner())
async def on_add_admin(m: Message):
    uid = target_id(m)
    if not uid:
        return await m.reply("Использование: /addadmin <ID> или ответом на сообщение человека.")
    db.execute("INSERT OR IGNORE INTO admins VALUES(?)", (uid,))
    _roles.pop(uid, None)
    await m.reply(f"✅ <code>{uid}</code> теперь админ бота.")


@dp.message(Command("deladmin"), F.chat.id == GROUP_ID, IsOwner())
async def on_del_admin(m: Message):
    uid = target_id(m)
    if not uid:
        return await m.reply("Использование: /deladmin <ID> или ответом на сообщение человека.")
    db.execute("DELETE FROM admins WHERE id=?", (uid,))
    _roles.pop(uid, None)
    await m.reply(f"✅ <code>{uid}</code> больше не админ бота.")


@dp.message(Command("admins"), F.chat.id == GROUP_ID, IsAdmin())
async def on_admins(m: Message):
    ids = [r["id"] for r in db.execute("SELECT id FROM admins")]
    lst = "\n".join(f"• <code>{i}</code>" for i in ids) or "—"
    await m.reply(
        f"👮 <b>Админы бота</b>\n{lst}\n\n"
        "Админы группы и её создатель имеют доступ автоматически.\n"
        "/addadmin и /deladmin — только для владельца."
    )


# ───────────── ГРУППА: админ → пользователь ──────────────────────────
@dp.message(F.chat.id == GROUP_ID, F.message_thread_id, ~F.text.startswith("/"))
async def on_admin_message(m: Message):
    # служебные сообщения не пересылаем
    if m.forum_topic_created or m.forum_topic_edited or m.forum_topic_closed or m.forum_topic_reopened or m.pinned_message:
        if m.pinned_message or m.forum_topic_edited:
            with suppress(TelegramBadRequest):
                await m.delete()
        return
    if not m.from_user or m.from_user.id == bot.id or not await message_role(m):
        return  # обычные участники группы не пишут от имени поддержки
    u = user_by_thread(m.message_thread_id)
    if not u or u["silent"]:
        return
    if u["banned"]:
        return await m.reply(T["to_banned"])
    try:
        sent = await copy_any(m, u["id"], None, reply_to(m, u["id"]))
        add_link(m.chat.id, m.message_id, u["id"], sent.message_id)
        if is_muted(u):
            await m.reply(T["to_muted"].format(until=fmt_until(u["muted_until"])))
    except TelegramForbiddenError:
        await m.reply(T["blocked_bot"])
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after)
        await m.reply(T["not_sent"])
    except Exception:
        log.exception("доставка пользователю не удалась")
        await m.reply(T["not_sent"])


@dp.edited_message(F.chat.id == GROUP_ID, F.message_thread_id, IsAdmin())
async def on_admin_edit(m: Message):
    u = user_by_thread(m.message_thread_id)
    if u and not u["silent"] and not u["banned"]:
        await sync_edit(m)


# ───────────────────────── Правки и реакции ──────────────────────────
async def sync_edit(m: Message) -> None:
    link = get_link(m.chat.id, m.message_id)
    if not link:
        return
    chat_id, msg_id = link
    try:
        if m.text is not None:
            await bot.edit_message_text(
                m.text, chat_id=chat_id, message_id=msg_id, entities=m.entities, parse_mode=None
            )
        elif m.caption is not None or m.photo or m.video or m.document:
            await bot.edit_message_caption(
                chat_id=chat_id, message_id=msg_id, caption=m.caption,
                caption_entities=m.caption_entities, parse_mode=None,
            )
    except (TelegramBadRequest, TelegramForbiddenError) as ex:
        log.debug("правка не синхронизирована: %s", ex)


@dp.message_reaction()
async def on_reaction(e: MessageReactionUpdated):
    link = get_link(e.chat.id, e.message_id)
    if not link:
        return
    if e.chat.id == GROUP_ID:  # реакция админа → пользователю
        if e.user is None or not await get_role(e.user.id):
            return
        u = get_user(link[0])
        if not u or u["silent"] or u["banned"]:
            return
    elif e.chat.type == "private":  # реакция пользователя → в тему
        u = get_user(e.chat.id)
        if not u or u["banned"] or is_muted(u):
            return
    else:
        return
    first = next((r for r in e.new_reaction if r.type in ("emoji", "custom_emoji")), None)
    reaction = []
    if isinstance(first, ReactionTypeEmoji):
        reaction = [ReactionTypeEmoji(emoji=first.emoji)]
    elif isinstance(first, ReactionTypeCustomEmoji):
        reaction = [ReactionTypeCustomEmoji(custom_emoji_id=first.custom_emoji_id)]
    with suppress(TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter):
        await bot.set_message_reaction(link[0], link[1], reaction=reaction)


# ──────────────────── Ошибки не должны убивать бота ──────────────────
@dp.errors()
async def on_error(event: ErrorEvent) -> bool:
    log.error("ошибка в обработчике: %r", event.exception, exc_info=event.exception)
    return True


# ─────────────────────────────── СТАРТ ────────────────────────────────
async def check_group() -> None:
    """Только предупреждает в логе, бот не падает."""
    try:
        chat = await bot.get_chat(GROUP_ID)
        if not chat.is_forum:
            log.error("В группе НЕ включены темы: Настройки группы → Темы. Без этого бот не заработает.")
        me = await bot.get_chat_member(GROUP_ID, bot.id)
        if me.status != ChatMemberStatus.ADMINISTRATOR:
            log.error("Бот не админ группы. Сделайте его админом с правом «Управление темами».")
        else:
            if not me.can_manage_topics:
                log.error("У бота нет права «Управление темами».")
            if not (me.can_delete_messages and me.can_pin_messages):
                log.warning("Нет прав на удаление/закрепление сообщений: /delete и закрепы не будут работать.")
    except Exception as ex:
        log.error("Не удалось проверить группу %s: %r (бот в группе? верный GROUP_ID?)", GROUP_ID, ex)


async def setup_commands() -> None:
    try:
        await bot.set_my_commands(
            [BotCommand(command="start", description="Главное меню"),
             BotCommand(command="delete", description="Удалить сообщение (ответом)")],
            scope=BotCommandScopeAllPrivateChats(),
        )
        await bot.set_my_commands(
            [BotCommand(command="ban", description="Бан"), BotCommand(command="unban", description="Разбан"),
             BotCommand(command="mute", description="Мут на время (30m, 2h, 1d)"),
             BotCommand(command="unmute", description="Снять мут"),
             BotCommand(command="silent", description="Тихий режим"),
             BotCommand(command="delete", description="Удалить сообщение (ответом)"),
             BotCommand(command="info", description="Данные пользователя"),
             BotCommand(command="admins", description="Админы бота")],
            scope=BotCommandScopeAllGroupChats(),
        )
    except Exception as ex:
        log.warning("команды меню не установлены: %r", ex)


async def main() -> None:
    if not TOKEN or not GROUP_ID:
        raise SystemExit("Укажите BOT_TOKEN и GROUP_ID (переменные окружения, .env или в начале bot.py).")
    await check_group()
    await setup_commands()
    await bot.delete_webhook()
    log.info("Поддержка %s запущена", BRAND)
    while True:  # перезапуск при любых сбоях сети/Telegram
        try:
            await dp.start_polling(
                bot, allowed_updates=["message", "edited_message", "callback_query", "message_reaction"],
                handle_signals=False,
            )
            return
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except Exception:
            log.exception("polling упал, перезапуск через 5 секунд")
            await asyncio.sleep(5)


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
