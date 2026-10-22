import asyncio
import logging
import os
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
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    Message,
    MessageReactionUpdated,
    ReactionTypeCustomEmoji,
    ReactionTypeEmoji,
    ReplyParameters,
)

# ───────────────────────────── НАСТРОЙКИ ─────────────────────────────
# Читаем .env, если он лежит рядом (формат KEY=VALUE)
_env = Path(__file__).with_name(".env")
if _env.exists():
    for _line in _env.read_text(encoding="utf-8").splitlines():
        if "=" in _line and not _line.lstrip().startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip("\"'"))

TOKEN = os.getenv("BOT_TOKEN", "")            # токен от @BotFather
GROUP_ID = int(os.getenv("GROUP_ID", "0"))    # ID группы с включёнными темами, вида -100...
BRAND = os.getenv("BRAND", "Umbrellafan")     # название канала
DB_PATH = os.getenv("DB_PATH", "support.db")

# Антиспам: не больше LIMIT сообщений за PERIOD секунд от одного пользователя
LIMIT, PERIOD = 5, 10

# ────────────────────────────── ТЕКСТЫ ───────────────────────────────
T = {
    "start": f"☂️ <b>Поддержка канала {escape(BRAND)}</b>\n\nНапишите свой вопрос, и мы ответим прямо здесь.",
    "first": "✅ <b>Сообщение отправлено</b>\n\nМы ответим, как только сможем.",
    "too_fast": "⏳ Слишком часто. Подождите немного и напишите снова.",
    "fail": "⚠️ Не удалось доставить сообщение. Попробуйте позже.",
    "banned_user": "🚫 Вы заблокированы в поддержке.",
    "blocked_bot": "⚠️ <b>Не доставлено</b>: пользователь заблокировал бота.",
    "not_sent": "⚠️ <b>Не доставлено</b>: Telegram не принял сообщение.",
    "ban_on": "🔒 Пользователь заблокирован. Его сообщения не приходят в тему.",
    "ban_off": "🔓 Пользователь разблокирован.",
    "silent_on": "🔕 Тихий режим включён. Сообщения из темы не уходят пользователю.",
    "silent_off": "🔔 Тихий режим выключен. Сообщения снова уходят пользователю.",
    "deleted": "✅ Сообщение удалено с обеих сторон.",
    "del_hint": "⚠️ Ответьте командой /delete на нужное сообщение.",
    "del_fail": "⚠️ Не удалось удалить (можно в течение 48 часов).",
    "card": (
        "👤 <b>Новый пользователь</b>\n\n"
        "<b>Имя:</b> {name}\n<b>ID:</b> <code>{id}</code>\n<b>Username:</b> {username}\n"
        "<b>Регистрация:</b> {created}\n\n"
        "Всё, что вы пишете в этой теме, уходит пользователю.\n\n"
        "/ban — заблокировать / разблокировать\n"
        "/silent — тихий режим (не отправлять пользователю)\n"
        "/delete — удалить сообщение (ответом на него)\n"
        "/info — данные пользователя"
    ),
    "info": (
        "👤 {name}\n\n<b>ID:</b> <code>{id}</code>\n<b>Username:</b> {username}\n"
        "<b>Заблокирован:</b> {banned}\n<b>Тихий режим:</b> {silent}\n<b>Регистрация:</b> {created}"
    ),
}

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
    CREATE INDEX IF NOT EXISTS users_thread ON users(thread_id);
    """
)


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


# ─────────────────────────────── ЛОГИКА ──────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("support")

bot = Bot(TOKEN or "0:placeholder", default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True))
dp = Dispatcher()

_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_hits: dict[int, deque] = defaultdict(deque)


def is_flood(uid: int) -> bool:
    now, q = time.monotonic(), _hits[uid]
    while q and now - q[0] > PERIOD:
        q.popleft()
    if len(q) >= LIMIT:
        return True
    q.append(now)
    return False


def reply_to(m: Message, target_chat: int) -> ReplyParameters | None:
    if not m.reply_to_message:
        return None
    link = get_link(m.chat.id, m.reply_to_message.message_id)
    if link and link[0] == target_chat:
        return ReplyParameters(message_id=link[1], allow_sending_without_reply=True)
    return None


async def ensure_topic(u, broken: int | None = None) -> int:
    """Возвращает ID темы пользователя, создаёт её при необходимости."""
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


async def copy_any(m: Message, chat_id: int, thread_id: int | None, reply) -> Message:
    if m.forward_origin:
        return await bot.forward_message(chat_id, m.chat.id, m.message_id, message_thread_id=thread_id)
    return await bot.copy_message(
        chat_id, m.chat.id, m.message_id, message_thread_id=thread_id, reply_parameters=reply
    )


# ─────────────────────── ЛИЧКА: пользователь → тема ───────────────────
@dp.message(Command("start"), F.chat.type == "private")
async def on_start(m: Message):
    upsert_user(m.from_user)
    await m.answer(T["start"])


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
    row = upsert_user(m.from_user)
    if row["banned"]:
        return
    if is_flood(m.from_user.id):
        return await m.reply(T["too_fast"])
    try:
        thread = await ensure_topic(m.from_user)
        reply = reply_to(m, GROUP_ID)
        try:
            sent = await copy_any(m, GROUP_ID, thread, reply)
        except TelegramBadRequest as ex:
            if "thread not found" not in ex.message.lower():
                raise
            # тему удалили в группе — создаём новую
            thread = await ensure_topic(m.from_user, broken=thread)
            sent = await copy_any(m, GROUP_ID, thread, None)
        add_link(m.chat.id, m.message_id, GROUP_ID, sent.message_id)
        if not row["welcomed"]:
            set_field(m.from_user.id, welcomed=1)
            await m.answer(T["first"])
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after)
        await m.reply(T["fail"])
    except Exception:
        log.exception("delivery to group failed (проверьте GROUP_ID, темы и права бота)")
        await m.reply(T["fail"])


@dp.edited_message(F.chat.type == "private")
async def on_user_edit(m: Message):
    await sync_edit(m)


# ────────────────── ГРУППА: админ → пользователь ──────────────────────
def in_topic(m: Message) -> bool:
    return m.chat.id == GROUP_ID and m.message_thread_id is not None


@dp.message(Command("ban"), F.chat.id == GROUP_ID, F.message_thread_id)
async def on_ban(m: Message):
    u = user_by_thread(m.message_thread_id)
    if not u:
        return
    banned = not u["banned"]
    set_field(u["id"], banned=int(banned))
    if banned:
        with suppress(Exception):
            await bot.send_message(u["id"], T["banned_user"])
    await m.reply(T["ban_on"] if banned else T["ban_off"])


@dp.message(Command("silent"), F.chat.id == GROUP_ID, F.message_thread_id)
async def on_silent(m: Message):
    u = user_by_thread(m.message_thread_id)
    if not u:
        return
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


@dp.message(Command("info"), F.chat.id == GROUP_ID, F.message_thread_id)
async def on_info(m: Message):
    u = user_by_thread(m.message_thread_id)
    if not u:
        return
    await m.reply(
        T["info"].format(
            name=f'<a href="tg://user?id={u["id"]}">{escape(u["name"])}</a>',
            id=u["id"], username=escape(u["username"]),
            banned="да" if u["banned"] else "нет", silent="включён" if u["silent"] else "выключен",
            created=u["created"],
        )
    )


@dp.message(Command("delete"), F.chat.id == GROUP_ID, F.message_thread_id)
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


@dp.message(F.chat.id == GROUP_ID, F.message_thread_id, ~F.text.startswith("/"))
async def on_admin_message(m: Message):
    # служебные сообщения (создание/закрытие темы, закрепы) пересылать не нужно
    if m.forum_topic_created or m.forum_topic_edited or m.forum_topic_closed or m.forum_topic_reopened or m.pinned_message:
        with suppress(TelegramBadRequest):
            if m.pinned_message or m.forum_topic_edited:
                await m.delete()
        return
    if not m.from_user or m.from_user.id == bot.id:
        return
    u = user_by_thread(m.message_thread_id)
    if not u or u["silent"]:
        return
    try:
        sent = await copy_any(m, u["id"], None, reply_to(m, u["id"]))
        add_link(m.chat.id, m.message_id, u["id"], sent.message_id)
    except TelegramForbiddenError:
        await m.reply(T["blocked_bot"])
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after)
        await m.reply(T["not_sent"])
    except Exception:
        log.exception("delivery to user failed")
        await m.reply(T["not_sent"])


@dp.edited_message(F.chat.id == GROUP_ID, F.message_thread_id)
async def on_admin_edit(m: Message):
    u = user_by_thread(m.message_thread_id)
    if u and not u["silent"]:
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
        log.debug("edit not synced: %s", ex)


@dp.message_reaction()
async def on_reaction(e: MessageReactionUpdated):
    link = get_link(e.chat.id, e.message_id)
    if not link:
        return
    if e.chat.type != "private" and (e.chat.id != GROUP_ID or e.user is None):
        return
    if e.chat.id == GROUP_ID:  # реакция админа → пользователю (если не тихий режим)
        u = get_user(link[0])
        if not u or u["silent"]:
            return
    first = next((r for r in e.new_reaction if r.type in ("emoji", "custom_emoji")), None)
    reaction = []
    if isinstance(first, ReactionTypeEmoji):
        reaction = [ReactionTypeEmoji(emoji=first.emoji)]
    elif isinstance(first, ReactionTypeCustomEmoji):
        reaction = [ReactionTypeCustomEmoji(custom_emoji_id=first.custom_emoji_id)]
    with suppress(TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter):
        await bot.set_message_reaction(link[0], link[1], reaction=reaction)


# ─────────────────────────────── СТАРТ ────────────────────────────────
async def check_group() -> None:
    chat = await bot.get_chat(GROUP_ID)
    if not chat.is_forum:
        raise SystemExit("В группе не включены темы: Настройки группы → Темы.")
    me = await bot.get_chat_member(GROUP_ID, bot.id)
    if me.status != ChatMemberStatus.ADMINISTRATOR or not me.can_manage_topics:
        raise SystemExit("Сделайте бота админом группы с правом «Управление темами».")
    if not (me.can_delete_messages and me.can_pin_messages):
        log.warning("У бота нет прав на удаление/закрепление сообщений: /delete и закрепы работать не будут.")


async def main() -> None:
    if not TOKEN or not GROUP_ID:
        raise SystemExit("Укажите BOT_TOKEN и GROUP_ID (переменные окружения, .env или в начале bot.py).")
    await check_group()
    await bot.set_my_commands(
        [BotCommand(command="start", description="Перезапустить бота"),
         BotCommand(command="delete", description="Удалить сообщение (ответом)")],
        scope=BotCommandScopeAllPrivateChats(),
    )
    await bot.set_my_commands(
        [BotCommand(command="ban", description="Заблокировать / разблокировать"),
         BotCommand(command="silent", description="Тихий режим"),
         BotCommand(command="delete", description="Удалить сообщение (ответом)"),
         BotCommand(command="info", description="Данные пользователя")],
        scope=BotCommandScopeAllGroupChats(),
    )
    await bot.delete_webhook()
    log.info("Поддержка %s запущена", BRAND)
    await dp.start_polling(bot, allowed_updates=["message", "edited_message", "message_reaction"])


if __name__ == "__main__":
    asyncio.run(main())
