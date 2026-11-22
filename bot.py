"""Telegram-бот анализа требований hh.ru (технологии, тренды, дифф).

Многопользовательский: доступ по инвайт-коду, все сборы идут через очередь задач
(jobs) с дедупликацией одинаковых запросов, LLM-режим ограничен суточными квотами.
По расписанию собираются отслеживаемые запросы (tracked_queries); по кнопкам —
текущий топ технологий, тренд за период, дифф с прошлого сбора и фильтры.

Запуск:
    export TELEGRAM_BOT_TOKEN="123:ABC..."       # от @BotFather
    export HH_CLIENT_ID=... HH_CLIENT_SECRET=...  # приложение hh (для токена)
    .venv/bin/python bot.py
Токен также читается из token.txt рядом со скриптом.
"""

import asyncio
import hashlib
import json
import os
import secrets
from datetime import datetime, timedelta, timezone

import envfile
envfile.load()  # .env -> os.environ ДО чтения параметров модулями и константами ниже

from telegram import (BotCommand, BotCommandScopeChat, InlineKeyboardButton,
                      InlineKeyboardMarkup, ReplyKeyboardMarkup, Update)
from telegram.ext import (AIORateLimiter, Application, CallbackQueryHandler,
                          CommandHandler, ContextTypes, ConversationHandler,
                          MessageHandler, filters)

import analysis
import collector
import jobs
import llm
import storage

HERE = os.path.dirname(os.path.abspath(__file__))
MSK = timezone(timedelta(hours=3))
TG_LIMIT = 3800

DEFAULT_QUERY = os.environ.get("DEFAULT_QUERY_TEXT", "python")
DEFAULT_AREA = os.environ.get("DEFAULT_AREA", "113")
TOP_N_DEFAULT = int(os.environ.get("TOP_N_DEFAULT", "15"))
PERIOD_DEFAULT = int(os.environ.get("TREND_PERIOD_DAYS", "30"))
ANALYZE_TOP_N = int(os.environ.get("ANALYZE_TOP_N", "200"))
ONDEMAND_TOP_N = int(os.environ.get("ONDEMAND_TOP_N", "200"))
VACS_LIST_N = int(os.environ.get("VACS_LIST_N", "12"))
DETAIL_THROTTLE = float(os.environ.get("DETAIL_THROTTLE", "0.2"))
COLLECT_INTERVAL = int(float(os.environ.get("COLLECT_HOURS", "6")) * 3600)
# сколько секунд снимок кастомного запроса считается свежим: пока свеж — /top читает
# его из базы без пересбора и обращений к LLM. Принудительный сбор (⟳/refresh) — всегда.
CUSTOM_FRESH_SECONDS = int(float(os.environ.get("CUSTOM_FRESH_HOURS",
                                               os.environ.get("COLLECT_HOURS", "6"))) * 3600)
DB_PATH = os.environ.get("DB_PATH", os.path.join(HERE, "hh_stats.db"))

WORKERS = int(os.environ.get("WORKERS", "2"))
QUEUE_POLL_SEC = 2.0
LLM_QUOTA_USER = int(os.environ.get("LLM_DAILY_QUOTA_USER", "5"))
LLM_QUOTA_ADMIN = int(os.environ.get("LLM_DAILY_QUOTA_ADMIN", "50"))
LLM_QUOTA_GLOBAL = int(os.environ.get("LLM_DAILY_QUOTA_GLOBAL", "100"))
TRACK_LIMIT_USER = int(os.environ.get("TRACK_LIMIT_USER", "5"))
TRACKED_MAX_TOTAL = int(os.environ.get("TRACKED_MAX_TOTAL", "40"))
# Подписки используют тот же интервал, что основной запрос; короткий опрос
# расписания подхватывает новые фильтры без ожидания следующего шестичасового тика.
TRACK_INTERVAL = COLLECT_INTERVAL
TRACK_SWEEP_SECONDS = 900
QUERY_MAX_LEN = 64

AREA_NAMES = {"all": "Весь мир", "113": "Россия", "1": "Москва", "2": "СПб"}
STATE_DEFAULTS = {"query_text": "", "area": DEFAULT_AREA,
                  "period_days": PERIOD_DEFAULT, "top_n": TOP_N_DEFAULT}
AWAIT_TEXT = 1    # состояние ConversationHandler: свой запрос
AWAIT_INVITE = 2  # состояние ConversationHandler: инвайт-код при регистрации
AWAIT_EDIT = 3    # исправление сохранённого фильтра


# --- токен / доступ ----------------------------------------------------------
def get_token():
    tok = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not tok:
        path = os.path.join(HERE, "token.txt")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                tok = f.read().strip()
    if not tok:
        raise SystemExit("Не задан TELEGRAM_BOT_TOKEN (env или token.txt)")
    return tok


def _id_set(env):
    return {c.strip() for c in os.environ.get(env, "").split(",") if c.strip()}


# TELEGRAM_ALLOWED_CHATS больше не гейт — только сидинг существующих пользователей
# в таблицу users при первом запуске новой схемы.
SEED_CHATS = _id_set("TELEGRAM_ALLOWED_CHATS")
ADMINS = _id_set("TELEGRAM_ADMIN_CHATS")


def _is_admin(chat_id):
    return str(chat_id) in ADMINS


def _llm_quota_for(chat_id):
    return LLM_QUOTA_ADMIN if _is_admin(chat_id) else LLM_QUOTA_USER


async def _admin_only(update, context):
    """Проверить админство и сказать вслух, если его нет: молчание читается как поломка."""
    cid = update.effective_chat.id
    if _is_admin(cid):
        return True
    await context.bot.send_message(
        cid, f"Команда только для админа. Ваш chat_id: {cid}.")
    return False


def _gate(context, chat_id):
    """Статус доступа: 'active' | 'banned' | 'unknown'.

    Кэш в bot_data — гейт дёргается на каждое нажатие кнопки, а меняется редко.
    """
    if _is_admin(chat_id):
        return "active"
    cache = context.application.bot_data.setdefault("user_status", {})
    if chat_id not in cache:
        u = storage.get_user(_conn(context), chat_id)
        cache[chat_id] = u["status"] if u else "unknown"
    return cache[chat_id]


def _drop_gate_cache(context, chat_id=None):
    cache = context.application.bot_data.setdefault("user_status", {})
    if chat_id is None:
        cache.clear()
    else:
        cache.pop(chat_id, None)


NEED_INVITE = ("Бот доступен по приглашению. Пришлите инвайт-код одним сообщением "
               "(или /start, чтобы попробовать снова).")


async def _deny(context, chat_id, status):
    if status == "banned":
        await context.bot.send_message(chat_id, "Доступ закрыт.")
    else:
        await context.bot.send_message(chat_id, NEED_INVITE)


async def _pass(update, context):
    """Проверить доступ и отметить активность. False — дальше обработку не вести."""
    chat_id = update.effective_chat.id
    status = _gate(context, chat_id)
    if status != "active":
        await _deny(context, chat_id, status)
        return False
    _touch(context, chat_id, update.effective_user)
    return True


def _touch(context, chat_id, user):
    """Обновить last_seen не чаще раза в час — иначе запись в БД на каждую кнопку."""
    seen = context.application.bot_data.setdefault("touched", {})
    now = datetime.now(timezone.utc).timestamp()
    if now - seen.get(chat_id, 0) < 3600:
        return
    seen[chat_id] = now
    storage.touch_user(_conn(context), chat_id, user.username if user else None)


def norm_query(text):
    """Канонизировать поисковый запрос: регистр и пробелы не должны плодить снимки."""
    return " ".join((text or "").strip().lower().split())[:QUERY_MAX_LEN]


# --- форматирование ----------------------------------------------------------
def _esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _area_label(area):
    return AREA_NAMES.get(str(area), f"area {area}")


def _chunk(text_lines, header):
    """Собрать строки в сообщения <pre> под лимитом Telegram."""
    msgs, cur = [], header + "\n"
    for ln in text_lines:
        if len(cur) + len(ln) + 1 > TG_LIMIT:
            msgs.append(cur)
            cur = ""
        cur += ln + "\n"
    msgs.append(cur)
    return [f"<pre>{_esc(m)}</pre>" for m in msgs]


def render_top(res, state):
    if res.get("error") == "no_data":
        le = res.get("last_error")
        extra = f"\nПоследняя ошибка: {le}" if le else ""
        return [f"Данные ещё не собраны.{extra}\nНажмите ⟳ Собрать сейчас."]
    snap = res["snapshot"]
    header = (f"Топ требований: {state['query_text']} / {_area_label(state['area'])}\n"
              f"Снимок: {snap['ts_msk']}; вакансий: {snap['vacancy_count']}")
    lines = []
    for i, (tech, df, share, src) in enumerate(res["rows"], 1):
        lines.append(f"{i:>2}. {share*100:4.0f}%  {df:>4}  {tech}")
    return _chunk(lines, header)


def _fmt_movers(rose, fell, new, gone):
    lines = []
    if rose:
        lines.append("▲ выросли:")
        lines += [f"   +{d:4.1f} п.п.  {t}  ({a*100:.0f}%→{b*100:.0f}%)" for t, d, a, b in rose]
    if fell:
        lines.append("▼ упали:")
        lines += [f"   {d:5.1f} п.п.  {t}  ({a*100:.0f}%→{b*100:.0f}%)" for t, d, a, b in fell]
    if new:
        lines.append("✚ появились:")
        lines += [f"   {b*100:.0f}%  {t}" for t, b in new]
    if gone:
        lines.append("✖ исчезли:")
        lines += [f"   было {a*100:.0f}%  {t}" for t, a in gone]
    if not lines:
        lines.append("Заметных изменений нет.")
    return lines


def render_trend(res, state, title):
    if res.get("error") == "no_data":
        return ["Данные ещё не собраны."]
    if res.get("error") == "insufficient_history":
        since = res.get("have_since")
        s = f" (история с {since[:10]})" if since else ""
        return [f"Недостаточно истории для тренда{s}.\n"
                "История копится только для отслеживаемых запросов — /track добавит "
                "текущий, дальше снимки накопятся сами."]
    if res.get("error") == "need_two_snapshots":
        return ["Нужно минимум два сбора. Пока есть только один — подождите следующего "
                "или нажмите ⟳ Собрать сейчас."]
    a, b = res["from"], res["to"]
    header = (f"{title}: {state['query_text']} / {_area_label(state['area'])}\n"
              f"{a['ts_msk']}  →  {b['ts_msk']}")
    return _chunk(_fmt_movers(res["rose"], res["fell"], res["new"], res["gone"]), header)


def _fmt_date(s):
    return s[:10] if s else "—"


def _vac_url(row):
    return row.get("alternate_url") or f"https://hh.ru/vacancy/{row['vac_id']}"


def _btn_label(i, row):
    """Подпись кнопки-вакансии: номер, название, компания, дата (обрезано под кнопку)."""
    title = row.get("name") or "—"
    company = row.get("company") or "—"
    if len(title) > 30:
        title = title[:29] + "…"
    if len(company) > 20:
        company = company[:19] + "…"
    date = (row.get("published_at") or "")[5:10]  # MM-DD
    return f"{i}. {title} · {company} · {date}"


def render_vacancies(rows, snap, state):
    if not rows:
        return [f"<pre>{_esc('В последнем снимке нет вакансий.')}</pre>"], None
    header = (f"Актуальные вакансии: {state['query_text']} / {_area_label(state['area'])}\n"
              f"Снимок: {snap['ts_msk']} (показаны {len(rows)})\n"
              f"Нажмите на вакансию — покажу подробности.")
    kb_rows = [[InlineKeyboardButton(_btn_label(i, r), callback_data=f"vac:{r['vac_id']}")]
               for i, r in enumerate(rows, 1)]
    kb_rows.append([InlineKeyboardButton("⬅️ Меню", callback_data="home")])
    return [f"<pre>{_esc(header)}</pre>"], InlineKeyboardMarkup(kb_rows)


def render_vacancy_detail(row):
    skills = []
    if row.get("key_skills_json"):
        try:
            skills = [k.get("name") if isinstance(k, dict) else k
                      for k in json.loads(row["key_skills_json"])]
        except Exception:
            skills = []
    body = (row.get("description")
            or " ".join(x for x in (row.get("snippet_req"), row.get("snippet_resp")) if x)
            or "Подробного описания нет.")
    if len(body) > 2500:
        body = body[:2500] + "…"
    lines = [row["name"], f"{row.get('company') or '—'} · {_fmt_date(row['published_at'])}", ""]
    if skills:
        lines.append("Навыки: " + ", ".join(skills))
        lines.append("")
    lines.append(body)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Открыть на hh.ru", url=_vac_url(row))],
        [InlineKeyboardButton("⬅️ К списку", callback_data="vacs")],
    ])
    return _chunk(lines, ""), kb


# --- клавиатуры --------------------------------------------------------------
def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Топ требований", callback_data="top"),
         InlineKeyboardButton("📋 Вакансии", callback_data="vacs")],
        [InlineKeyboardButton("📈 Тренд за месяц", callback_data="trend"),
         InlineKeyboardButton("🔁 Что изменилось", callback_data="diff")],
        [InlineKeyboardButton("🧠 Проанализировать заново (LLM)", callback_data="re_llm")],
        [InlineKeyboardButton("⚙️ Фильтры", callback_data="flt"),
         InlineKeyboardButton("⟳ Загрузить свежие", callback_data="collect")],
    ])


# Нижняя клавиатура: те же действия, что в inline-меню, но всегда под рукой.
# Нажатие приходит обычным текстом — отсюда и диспетчеризация по подписи.
REPLY_ACTIONS = {
    "📊 Топ": "top",
    "📋 Вакансии": "vacs",
    "📈 Тренд": "trend",
    "🔁 Изменения": "diff",
    "⚙️ Фильтры": "flt",
    "⟳ Обновить": "collect",
    "📌 Отслеживать": "track",
    "ℹ️ Статус": "status",
}
REPLY_LAYOUT = [["📊 Топ", "📋 Вакансии"],
                ["📈 Тренд", "🔁 Изменения"],
                ["⚙️ Фильтры", "⟳ Обновить"],
                ["📌 Отслеживать", "ℹ️ Статус"]]
ADMIN_BUTTON = "🛠 Админ"
REPLY_ACTIONS[ADMIN_BUTTON] = "admin"  # кнопка видна только админам, но фильтр общий


def reply_kb(chat_id=None):
    """Нижняя клавиатура; админу добавляется вход в панель."""
    layout = list(REPLY_LAYOUT)
    if chat_id is not None and _is_admin(chat_id):
        layout = layout + [[ADMIN_BUTTON]]
    return ReplyKeyboardMarkup(layout, resize_keyboard=True, is_persistent=True)


def _mark(cur, val, label):
    return ("• " + label) if str(cur) == str(val) else label


def _saved_filter_token(query, area):
    return hashlib.blake2s(f"{query}\0{area}".encode(), digest_size=4).hexdigest()


def _resolve_saved_filter(saved_filters, callback_data):
    try:
        _, index_text, token = callback_data.split(":", 2)
        if not index_text.isdecimal():
            return None
        query, area = saved_filters[int(index_text)]
    except (ValueError, IndexError):
        return None
    return (query, area) if token == _saved_filter_token(query, area) else None


def filter_kb(state, tracked=False, saved_filters=(), tracked_filters=()):
    a, p, n, q = state["area"], state["period_days"], state["top_n"], state["query_text"]
    track_label = "📌 Отслеживается — снять" if tracked else "📌 Отслеживать этот запрос"
    current_saved = (q, str(a)) in saved_filters
    query_row = []
    if current_saved:
        query_row.append(InlineKeyboardButton(f"✏️ {q[:24]}", callback_data="edit_filter"))
    rows = [
        [InlineKeyboardButton(_mark(a, "all", "Весь мир"), callback_data="area:all"),
         InlineKeyboardButton(_mark(a, "113", "Россия"), callback_data="area:113")],
        [InlineKeyboardButton(_mark(p, 30, "30д"), callback_data="period:30"),
         InlineKeyboardButton(_mark(p, 60, "60д"), callback_data="period:60"),
         InlineKeyboardButton(_mark(p, 90, "90д"), callback_data="period:90")],
        [InlineKeyboardButton(_mark(n, 10, "топ-10"), callback_data="topn:10"),
         InlineKeyboardButton(_mark(n, 15, "топ-15"), callback_data="topn:15"),
         InlineKeyboardButton(_mark(n, 30, "топ-30"), callback_data="topn:30")],
        [InlineKeyboardButton("➕ Новый запрос", callback_data="text:custom")],
    ]
    if query_row:
        rows.insert(3, query_row)
    for index, (query, area) in enumerate(saved_filters):
        pin = "📌 " if (query, str(area)) in tracked_filters else ""
        label = f"{pin}{query} / {_area_label(area)}"
        if q == query and str(a) == str(area):
            label = "• " + label
        rows.append([InlineKeyboardButton(
            label[:60], callback_data=f"saved:{index}:{_saved_filter_token(query, area)}")])
    if current_saved:
        rows.extend([[InlineKeyboardButton(track_label, callback_data="track")],
                     [InlineKeyboardButton("🗑 Удалить текущий фильтр", callback_data="delete_filter")]])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def _filter_kb(context, chat_id, state):
    conn = _conn(context)
    saved = storage.user_saved_filters(conn, chat_id)
    tracked = jobs.user_tracked(conn, chat_id)
    active = (state["query_text"], str(state["area"])) in tracked
    return filter_kb(state, active, saved, tracked)


def _edit_area_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Весь мир", callback_data="move:all"),
         InlineKeyboardButton("Россия", callback_data="move:113")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="flt")],
    ])


def _rename_saved_filter(context, chat_id, old_query, old_area, new_query, new_area):
    conn = _conn(context)
    was_tracked = (old_query, str(old_area)) in jobs.user_tracked(conn, chat_id)
    if was_tracked and not jobs.is_tracked(conn, new_query, new_area):
        old_subscribers = jobs.subscribers(conn, old_query, old_area)
        source_remains = (len(old_subscribers) > 1 or
                          (old_query == DEFAULT_QUERY and str(old_area) == str(DEFAULT_AREA)))
        if source_remains and jobs.tracked_count(conn) >= TRACKED_MAX_TOTAL:
            return "limit"
    status = storage.rename_filter(conn, chat_id, old_query, old_area, new_query, new_area)
    if status == "renamed" and was_tracked:
        latest = storage.latest_ok_snapshot(conn, new_query, new_area)
        if not latest or _snapshot_age(latest) >= COLLECT_INTERVAL:
            jobs.enqueue(conn, "collect", new_query, new_area, ANALYZE_TOP_N, None, None)
            _wake_workers(context)
    return status


def _change_selected_area(context, chat_id, new_area):
    conn = _conn(context)
    st = _state(context, chat_id)
    query, old_area = st["query_text"], str(st["area"])
    if old_area == new_area:
        return "unchanged"
    saved = storage.user_saved_filters(conn, chat_id)
    if (query, old_area) in saved:
        if (query, new_area) in saved:
            storage.set_chat_state(conn, chat_id, area=new_area)
            return "selected"
        return _rename_saved_filter(context, chat_id, query, old_area, query, new_area)
    storage.set_chat_state(conn, chat_id, area=new_area)
    if query:
        storage.save_filter(conn, chat_id, query, new_area)
        return "created"
    return "selected"


def _select_python_filter(context, chat_id):
    conn = _conn(context)
    current_area = str(_state(context, chat_id)["area"])
    saved = storage.user_saved_filters(conn, chat_id)
    python_areas = [area for query, area in saved if query == "python"]
    selected_area = (current_area if current_area in python_areas else
                     python_areas[0] if python_areas else current_area)
    storage.set_chat_state(conn, chat_id, query_text="python", area=selected_area)
    if not python_areas:
        storage.save_filter(conn, chat_id, "python", selected_area)


def _select_remaining_filter(context, chat_id):
    conn = _conn(context)
    saved = storage.user_saved_filters(conn, chat_id)
    if saved:
        query, area = next(((query, area) for query, area in saved if query == "python"),
                           saved[0])
    else:
        query, area = "", DEFAULT_AREA
    storage.set_chat_state(conn, chat_id, query_text=query, area=area)


MENU_TEXT = ("Анализ требований вакансий hh.ru.\n"
             "Выберите действие:")
FILTER_TEXT = "Фильтры анализа (• — выбрано). Если список пуст, создайте запрос:"


# --- доступ к соединению -----------------------------------------------------
def _conn(context):
    return context.application.bot_data["conn"]


def _state(context, chat_id):
    return storage.get_chat_state(_conn(context), chat_id, STATE_DEFAULTS)


async def _send_messages(bot, chat_id, messages, keyboard=None):
    for i, m in enumerate(messages):
        await bot.send_message(chat_id, m, parse_mode="HTML",
                               disable_web_page_preview=True,
                               reply_markup=keyboard if i == len(messages) - 1 else None)


# --- команды -----------------------------------------------------------------
HELP = (
    "Бот анализирует требования вакансий hh.ru через LLM. Если модели недоступны, "
    "использует базовый словарный анализ.\n\n"
    "📊 Топ требований — самые частые технологии сейчас\n"
    "📋 Вакансии — список актуальных вакансий, по кнопке — подробности\n"
    "📈 Тренд за месяц — что выросло/упало/появилось/исчезло\n"
    "🔁 Что изменилось — дифф с прошлого сбора\n"
    "⚙️ Фильтры — регион, период, размер топа, сохранённые запросы\n"
    "⟳ Загрузить свежие (кнопка) или /refresh — скачать свежие вакансии и пересчитать\n"
    "🧠 Проанализировать заново (LLM) — пересчитать топ по уже загруженным, без hh\n"
    "/reanalyze_llm, /reanalyze_basic — то же командой (LLM или по словарю)\n"
    "📌 /track — собирать выбранный фильтр каждые 6 часов и получать изменения топа,\n"
    "/untrack, /tracked — снять отслеживание и посмотреть список\n\n"
    "Сборы идут через общую очередь: если запрос уже считается для кого-то другого, "
    "вы просто подпишетесь на его результат."
)


async def _reset_and_menu(context, chat_id, text):
    """Показать справку с нижней клавиатурой и следом inline-меню."""
    await context.bot.send_message(chat_id, text, reply_markup=reply_kb(chat_id))
    await context.bot.send_message(chat_id, MENU_TEXT, reply_markup=main_kb())


async def cmd_help(update, context):
    if not await _pass(update, context):
        return
    await _reset_and_menu(context, update.effective_chat.id, HELP)


async def cmd_top(update, context):
    if not await _pass(update, context):
        return
    await _do_top(context, update.effective_chat.id)


async def cmd_vacancies(update, context):
    if not await _pass(update, context):
        return
    await _do_vacancies(context, update.effective_chat.id)


async def cmd_trend(update, context):
    if not await _pass(update, context):
        return
    await _do_trend(context, update.effective_chat.id, kind="trend")


async def cmd_diff(update, context):
    if not await _pass(update, context):
        return
    await _do_trend(context, update.effective_chat.id, kind="diff")


async def cmd_filters(update, context):
    if not await _pass(update, context):
        return
    cid = update.effective_chat.id
    st = _state(context, cid)
    await update.message.reply_text(FILTER_TEXT, reply_markup=_filter_kb(context, cid, st))


async def cmd_status(update, context):
    if not await _pass(update, context):
        return
    cid = update.effective_chat.id
    conn = _conn(context)
    st = _state(context, cid)
    L = storage.latest_ok_snapshot(conn, st["query_text"], st["area"])
    last = storage.last_snapshot(conn, st["query_text"], st["area"])
    queued, running = jobs.queue_depth(conn)
    user_limit = _llm_quota_for(cid)
    left_user, left_global = storage.llm_quota_left(conn, cid, user_limit, LLM_QUOTA_GLOBAL)
    tracked = jobs.user_tracked(conn, cid)
    selected_query = (f"{st['query_text']} / {_area_label(st['area'])}"
                      if st["query_text"] else "не выбран")
    lines = [f"Ваш активный запрос: {selected_query}"]
    if L:
        method = L["method"] if "method" in L.keys() and L["method"] else "?"
        lines.append(f"Последний сбор: {L['ts_msk']}; вакансий {L['vacancy_count']} "
                     f"(деталей {L['enriched_count']}); метод: {method}")
        if method == "llm":
            models = json.loads(L["models_used_json"]) if L["models_used_json"] else []
            lines.append("Модели анализа: " + (", ".join(models) if models else
                         "не записаны для этого сбора"))
        elif method.startswith("basic"):
            lines.append("Модели анализа: не использовались (базовый режим)")
    else:
        lines.append("По этому запросу сборов ещё не было.")
    if tracked:
        lines.append("Отслеживаемые фильтры:")
        for query, area in tracked:
            snapshot = storage.latest_ok_snapshot(conn, query, area)
            finished = jobs.last_collect_finished(conn, query, area)
            latest_times = ([datetime.fromisoformat(snapshot["ts_utc"])] if snapshot else [])
            if finished:
                latest_times.append(datetime.fromisoformat(finished))
            if jobs.has_active(conn, "collect", query, area, None):
                schedule = "сбор в очереди или в работе"
            elif latest_times:
                due = (max(latest_times) + timedelta(seconds=TRACK_INTERVAL)).astimezone(
                    storage.MSK)
                schedule = f"следующий сбор около {due:%d.%m %H:%M} МСК"
            else:
                schedule = "первый сбор ожидается"
            lines.append(f"• {query} / {_area_label(area)} — {schedule}")
    else:
        lines.append("Отслеживаемых запросов нет — /track добавит текущий.")
    lines.append(f"Очередь: {queued} ждут, {running} в работе.")
    lines.append(f"Лимит LLM на сегодня: осталось {left_user} из {user_limit}.")
    if last and last["status"] == "error":
        lines.append(f"⚠️ Последний сбор с ошибкой: {last['error']}")
    if left_global == 0:
        lines.append("Общий дневной лимит LLM исчерпан — считаем по словарю.")
    if _is_admin(cid):
        providers = []
        if llm.gemini_key():
            providers.append("Gemini API: " + ", ".join(llm.gemini_models()))
        if llm.api_key():
            providers.append("OpenRouter: " + ", ".join(llm.models()))
        lines.append(f"LLM режим: {os.environ.get('LLM_MODE', 'auto')}; "
                     + ("; ".join(providers) or "нет ключей (GEMINI/OPENROUTER)"))
    await context.bot.send_message(cid, "\n".join(lines), reply_markup=main_kb())


def _top_n_for(query_text, area):
    """Отслеживаемые запросы собираем глубже — по ним копится история."""
    is_default = query_text == DEFAULT_QUERY and str(area) == str(DEFAULT_AREA)
    return ANALYZE_TOP_N if is_default else ONDEMAND_TOP_N


def _fmt_eta(seconds):
    if not seconds:
        return ""
    return f", ~{max(1, round(seconds / 60))} мин" if seconds >= 60 else ", меньше минуты"


async def _submit(context, chat_id, kind, query_text, area, mode=None, announce=True):
    """Поставить задачу в очередь с учётом квоты. Возвращает True, если задача есть.

    Квота списывается только за НОВУЮ llm-задачу: подписка на чужую уже идущую
    работу расхода не создаёт, поэтому не тарифицируется.
    """
    if not query_text:
        await context.bot.send_message(chat_id, "Сначала создайте фильтр: ⚙️ Фильтры → ➕ Новый запрос.")
        return False
    conn = _conn(context)
    effective = (mode or os.environ.get("LLM_MODE", "auto")).lower()
    wants_llm = effective in ("llm", "auto") and llm.available()
    if wants_llm and not jobs.has_active(conn, kind, query_text, area, mode):
        user_limit = _llm_quota_for(chat_id)
        verdict = storage.try_spend_llm_quota(conn, chat_id, user_limit, LLM_QUOTA_GLOBAL)
        if verdict == "user_exceeded":
            await context.bot.send_message(
                chat_id, f"Дневной лимит анализов через LLM исчерпан "
                         f"({user_limit}/сутки). Данные, собранные ранее, доступны "
                         f"по кнопкам — новый анализ будет доступен завтра.")
            return False
        if verdict == "global_exceeded":
            mode = "basic"
            await context.bot.send_message(
                chat_id, "Общий дневной лимит LLM исчерпан — посчитаю по словарю "
                         "(быстрее, чуть грубее).")
    top_n = _top_n_for(query_text, area)
    job_id, deduped, pos = jobs.enqueue(conn, kind, query_text, area, top_n, mode, chat_id)
    _wake_workers(context)
    if not announce:
        return True
    what = f"{query_text} / {_area_label(area)}"
    if deduped:
        await context.bot.send_message(
            chat_id, f"Этот запрос уже считается ({what}) — пришлю результат, как будет готов.")
    else:
        eta = _fmt_eta(jobs.eta_seconds(conn, job_id))
        place = "начинаю" if pos == 0 else f"в очереди {pos + 1}-й"
        await context.bot.send_message(chat_id, f"Принял: {what} — {place}{eta}.")
    return True


async def cmd_refresh(update, context):
    """Принудительный сбор активного запроса (режим по умолчанию, LLM_MODE)."""
    if not await _pass(update, context):
        return
    cid = update.effective_chat.id
    st = _state(context, cid)
    await _submit(context, cid, "collect", st["query_text"], st["area"])


async def _reanalyze_active(context, chat_id, mode):
    """Пересчитать топ активного запроса по сохранённым вакансиям — без загрузки из hh."""
    st = _state(context, chat_id)
    if not st["query_text"]:
        await context.bot.send_message(chat_id, "Сначала создайте фильтр: ⚙️ Фильтры → ➕ Новый запрос.")
        return
    conn = _conn(context)
    if not storage.latest_ok_snapshot(conn, st["query_text"], st["area"]):
        await context.bot.send_message(
            chat_id, "Нет сохранённых данных для пересчёта. Сначала соберите: ⟳ или /refresh.")
        return
    await _submit(context, chat_id, "reanalyze", st["query_text"], st["area"], mode=mode)


async def cmd_reanalyze_llm(update, context):
    """Пересчёт топа через LLM по сохранённым вакансиям (без загрузки из hh)."""
    if not await _pass(update, context):
        return
    await _reanalyze_active(context, update.effective_chat.id, mode="llm")


async def cmd_reanalyze_basic(update, context):
    """Пересчёт топа базовым режимом (словарь) по сохранённым вакансиям (без загрузки)."""
    if not await _pass(update, context):
        return
    await _reanalyze_active(context, update.effective_chat.id, mode="basic")


async def cmd_collect(update, context):
    if not await _admin_only(update, context):
        return
    cid = update.effective_chat.id
    await _submit(context, cid, "collect", DEFAULT_QUERY, DEFAULT_AREA)


# --- подписки на запрос (история и тренды) -----------------------------------
async def _do_track(context, chat_id):
    """Переключить отслеживание активного запроса: снять, если уже стоит."""
    conn = _conn(context)
    st = _state(context, chat_id)
    query, area = st["query_text"], str(st["area"])
    if not query:
        await context.bot.send_message(chat_id, "Сначала создайте фильтр: ⚙️ Фильтры → ➕ Новый запрос.")
        return
    mine = [(q, str(a)) for q, a in jobs.user_tracked(conn, chat_id)]
    if (query, area) in mine:
        jobs.untrack(conn, query, area, chat_id)
        await context.bot.send_message(
            chat_id, f"Больше не отслеживаю: {query} / {_area_label(area)}.")
        return
    if len(mine) >= TRACK_LIMIT_USER:
        await context.bot.send_message(
            chat_id, f"Можно отслеживать не больше {TRACK_LIMIT_USER} запросов. "
                     f"Снимите лишний: /tracked покажет список.")
        return
    if not jobs.is_tracked(conn, query, area) and jobs.tracked_count(conn) >= TRACKED_MAX_TOTAL:
        await context.bot.send_message(
            chat_id, "Достигнут общий предел отслеживаемых запросов — "
                     "фоновый сбор больше не потянет. Попробуйте позже.")
        return
    jobs.track(conn, query, area, chat_id)
    storage.save_filter(conn, chat_id, query, area)
    latest = storage.latest_ok_snapshot(conn, query, area)
    if not latest or _snapshot_age(latest) >= COLLECT_INTERVAL:
        jobs.enqueue(conn, "collect", query, area, ANALYZE_TOP_N, None, None)
        _wake_workers(context)
    await context.bot.send_message(
        chat_id, f"Отслеживаю: {query} / {_area_label(area)}. Раз в "
                 f"{TRACK_INTERVAL/3600:g} ч буду собирать снимок с LLM и сообщать "
                 "об изменениях топа — через пару сборов "
                 f"заработают «Тренд» и «Что изменилось».")


async def cmd_track(update, context):
    if not await _pass(update, context):
        return
    await _do_track(context, update.effective_chat.id)


async def cmd_untrack(update, context):
    if not await _pass(update, context):
        return
    cid = update.effective_chat.id
    st = _state(context, cid)
    if jobs.untrack(_conn(context), st["query_text"], str(st["area"]), cid):
        await context.bot.send_message(cid, "Снято с отслеживания.")
    else:
        await context.bot.send_message(cid, "Текущий запрос и не отслеживался.")


async def cmd_tracked(update, context):
    if not await _pass(update, context):
        return
    cid = update.effective_chat.id
    mine = jobs.user_tracked(_conn(context), cid)
    if not mine:
        await context.bot.send_message(
            cid, "Вы ничего не отслеживаете. /track — добавить текущий запрос.")
        return
    lines = [f"• {q} / {_area_label(a)}" for q, a in mine]
    await context.bot.send_message(
        cid, f"Отслеживаемые запросы ({len(mine)}/{TRACK_LIMIT_USER}):\n" + "\n".join(lines))


async def cmd_dict(update, context):
    if not await _admin_only(update, context):
        return
    await _do_dict(context, update.effective_chat.id)


async def _do_dict(context, cid):
    pend = storage.pending_aliases(_conn(context), limit=30)
    if not pend:
        await context.bot.send_message(cid, "Кандидатов в словарь нет.")
        return
    lines = ["Кандидаты в словарь (навык × встреч):"]
    lines += [f"  {c:>4}  {a}" for a, c in pend]
    await _send_messages(context.bot, cid, _chunk(lines, "")[:1] or ["—"])


# --- логирование и заглушка на непонятный ввод -------------------------------
async def log_update(update, context):
    """Лог каждого входящего апдейта (группа -1, не блокирует обработку)."""
    cid = update.effective_chat.id if update.effective_chat else "?"
    if update.callback_query:
        print(f"[upd] chat={cid} callback={update.callback_query.data!r}", flush=True)
    elif update.effective_message and update.effective_message.text:
        print(f"[upd] chat={cid} text={update.effective_message.text!r}", flush=True)


async def unknown(update, context):
    """Ответ на текст, который не распознан (вне диалога и не кнопка)."""
    cid = update.effective_chat.id
    if _gate(context, cid) != "active":
        return
    await _reset_and_menu(context, cid, "Не понял. Пользуйтесь кнопками ниже или /help.")


async def on_reply_button(update, context):
    """Нажатие кнопки нижней клавиатуры — приходит обычным текстом."""
    if not await _pass(update, context):
        return
    cid = update.effective_chat.id
    action = REPLY_ACTIONS.get((update.message.text or "").strip())
    if action == "top":
        await _do_top(context, cid)
    elif action == "vacs":
        await _do_vacancies(context, cid)
    elif action in ("trend", "diff"):
        await _do_trend(context, cid, kind=action)
    elif action == "collect":
        st = _state(context, cid)
        await _submit(context, cid, "collect", st["query_text"], st["area"])
    elif action == "track":
        await _do_track(context, cid)
    elif action == "status":
        await cmd_status(update, context)
    elif action == "flt":
        st = _state(context, cid)
        await context.bot.send_message(cid, FILTER_TEXT,
                                       reply_markup=_filter_kb(context, cid, st))
    elif action == "admin":
        if not await _admin_only(update, context):
            return ConversationHandler.END
        await context.bot.send_message(cid, ADMIN_TEXT, reply_markup=admin_kb())
    return ConversationHandler.END  # если нажали посреди диалога — выходим из него


# --- ядро действий -----------------------------------------------------------
def _snapshot_age(row):
    """Возраст ok-снимка в секундах (None, если снимка нет)."""
    if not row:
        return None
    return (datetime.now(timezone.utc) - datetime.fromisoformat(row["ts_utc"])).total_seconds()


async def _ensure_snapshot(context, chat_id, st):
    """Подготовить данные к показу, никогда не заставляя пользователя ждать.

    Свежий снимок — отдаём как есть. Устаревший — отдаём немедленно, а обновление
    ставим в очередь и дошлём. Снимка нет — ставим в очередь и говорить показывать
    нечего. Возвращает True, если что-то уже можно показать.
    """
    if not st["query_text"]:
        await context.bot.send_message(chat_id, "Сначала создайте фильтр: ⚙️ Фильтры → ➕ Новый запрос.")
        return False
    conn = _conn(context)
    snap = storage.latest_ok_snapshot(conn, st["query_text"], st["area"])
    age = _snapshot_age(snap)
    if age is not None and age < CUSTOM_FRESH_SECONDS:
        return True
    if age is None:
        await _submit(context, chat_id, "collect", st["query_text"], st["area"])
        return False
    await _submit(context, chat_id, "collect", st["query_text"], st["area"], announce=False)
    await context.bot.send_message(
        chat_id, f"Показываю данные от {snap['ts_msk']}, свежие уже собираю — пришлю.")
    return True


async def _do_top(context, chat_id):
    st = _state(context, chat_id)
    if not await _ensure_snapshot(context, chat_id, st):
        return
    res = analysis.current_top(_conn(context), st["query_text"], st["area"], st["top_n"])
    await _send_messages(context.bot, chat_id, render_top(res, st), main_kb())


async def _do_vacancies(context, chat_id):
    st = _state(context, chat_id)
    if not await _ensure_snapshot(context, chat_id, st):
        return
    conn = _conn(context)
    L = storage.latest_ok_snapshot(conn, st["query_text"], st["area"])
    if not L:
        await _send_messages(context.bot, chat_id,
                             ["Данные ещё не собраны. Нажмите ⟳ Собрать сейчас."], main_kb())
        return
    rows = storage.snapshot_vacancies(conn, L["run_id"], VACS_LIST_N)
    msgs, kb = render_vacancies(rows, dict(L), st)
    await _send_messages(context.bot, chat_id, msgs, kb)


async def _do_vacancy_detail(context, chat_id, vac_id):
    row = storage.get_vacancy(_conn(context), vac_id)
    if not row:
        await context.bot.send_message(
            chat_id, "Вакансия не найдена (снимок мог обновиться).", reply_markup=main_kb())
        return
    msgs, kb = render_vacancy_detail(row)
    await _send_messages(context.bot, chat_id, msgs, kb)


async def _do_trend(context, chat_id, kind):
    st = _state(context, chat_id)
    if not st["query_text"]:
        await context.bot.send_message(chat_id, "Сначала создайте фильтр: ⚙️ Фильтры → ➕ Новый запрос.")
        return
    conn = _conn(context)
    if kind == "diff":
        res = analysis.diff_since_last(conn, st["query_text"], st["area"], st["top_n"])
        msgs = render_trend(res, st, "Изменения с прошлого сбора")
    else:
        res = analysis.month_trend(conn, st["query_text"], st["area"],
                                   st["period_days"], st["top_n"])
        msgs = render_trend(res, st, f"Тренд за {st['period_days']}д")
    await _send_messages(context.bot, chat_id, msgs, main_kb())


# --- callbacks ---------------------------------------------------------------
async def on_callback(update, context):
    q = update.callback_query
    cid = q.message.chat.id
    if _gate(context, cid) != "active":
        await q.answer("Доступ по приглашению — отправьте /start.", show_alert=True)
        return
    await q.answer()
    _touch(context, cid, update.effective_user)
    data = q.data

    if data == "top":
        await _do_top(context, cid)
    elif data == "vacs":
        await _do_vacancies(context, cid)
    elif data.startswith("vac:"):
        await _do_vacancy_detail(context, cid, data.split(":", 1)[1])
    elif data == "trend":
        await _do_trend(context, cid, kind="trend")
    elif data == "diff":
        await _do_trend(context, cid, kind="diff")
    elif data == "collect":
        st = _state(context, cid)
        await _submit(context, cid, "collect", st["query_text"], st["area"])
    elif data == "re_llm":
        await _reanalyze_active(context, cid, mode="llm")
    elif data == "track":
        await _do_track(context, cid)
        st = _state(context, cid)
        await q.edit_message_reply_markup(reply_markup=_filter_kb(context, cid, st))
    elif data.startswith("adm:"):
        await _on_admin_callback(update, context, cid, data[len("adm:"):])
    elif data == "flt":
        st = _state(context, cid)
        await q.edit_message_text(FILTER_TEXT,
                                  reply_markup=_filter_kb(context, cid, st))
    elif data == "home":
        await q.edit_message_text(MENU_TEXT, reply_markup=main_kb())
    elif data.startswith("saved:"):
        saved = storage.user_saved_filters(_conn(context), cid)
        pair = _resolve_saved_filter(saved, data)
        if pair is None:
            await q.answer("Список изменился — откройте фильтры заново.", show_alert=True)
            return
        query, area = pair
        storage.set_chat_state(_conn(context), cid, query_text=query, area=area)
        st = _state(context, cid)
        await q.edit_message_reply_markup(reply_markup=_filter_kb(context, cid, st))
    elif data == "delete_filter":
        st = _state(context, cid)
        conn = _conn(context)
        query, area = st["query_text"], str(st["area"])
        jobs.untrack(conn, query, area, cid)
        storage.delete_filter(conn, cid, query, area)
        _select_remaining_filter(context, cid)
        st = _state(context, cid)
        await q.edit_message_reply_markup(reply_markup=_filter_kb(context, cid, st))
    elif data == "edit_area":
        st = _state(context, cid)
        pair = (st["query_text"], str(st["area"]))
        if pair not in storage.user_saved_filters(_conn(context), cid):
            await q.edit_message_text(FILTER_TEXT,
                                      reply_markup=_filter_kb(context, cid, st))
            return
        await q.edit_message_text(f"Новый регион для «{st['query_text']}»: ",
                                  reply_markup=_edit_area_kb())
    elif data.startswith("move:"):
        st = _state(context, cid)
        new_area = data.partition(":")[2]
        if new_area not in ("all", "113"):
            return
        status = _rename_saved_filter(context, cid, st["query_text"], st["area"],
                                      st["query_text"], new_area)
        if status in ("exists", "limit", "missing"):
            messages = {"exists": "Такой фильтр уже сохранён.",
                        "limit": "Достигнут общий лимит отслеживаемых фильтров.",
                        "missing": "Фильтр уже удалён."}
            await context.bot.send_message(cid, messages[status])
        st = _state(context, cid)
        await q.edit_message_text(FILTER_TEXT, reply_markup=_filter_kb(context, cid, st))
    elif ":" in data:
        key, _, val = data.partition(":")
        field = {"area": "area", "period": "period_days", "topn": "top_n",
                 "text": "query_text"}.get(key)
        if field == "query_text":
            await q.answer("Кнопка устарела. Создайте свой запрос через ➕ Новый запрос.",
                           show_alert=True)
            return
        elif field in ("period_days", "top_n"):
            storage.set_chat_state(_conn(context), cid, **{field: int(val)})
        elif field == "area":
            if val not in ("all", "113"):
                await context.bot.send_message(cid, "Теперь доступны регионы: Весь мир и Россия.")
                return
            status = _change_selected_area(context, cid, val)
            if status == "limit":
                await context.bot.send_message(
                    cid, "Достигнут общий лимит отслеживаемых фильтров.")
                return
        st = _state(context, cid)
        await q.edit_message_reply_markup(reply_markup=_filter_kb(context, cid, st))


# --- ConversationHandler: свой запрос ---------------------------------------
async def ask_custom(update, context):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text("Отправьте поисковый запрос (например: golang backend). "
                               "/cancel — отмена.")
    return AWAIT_TEXT


async def ask_edit_filter(update, context):
    q = update.callback_query
    await q.answer()
    cid = q.message.chat.id
    st = _state(context, cid)
    pair = (st["query_text"], str(st["area"]))
    if pair not in storage.user_saved_filters(_conn(context), cid):
        await q.message.reply_text("Выбранный фильтр не сохранён. Создайте новый запрос.")
        return ConversationHandler.END
    context.user_data["editing_filter"] = pair
    await q.message.reply_text(f"Отправьте исправленный запрос вместо «{pair[0]}». "
                               "Регион сохранится. /cancel — отмена.")
    return AWAIT_EDIT


async def save_custom(update, context):
    cid = update.effective_chat.id
    text = norm_query(update.message.text)
    if not text:
        await update.message.reply_text("Пустой запрос — оставил прежний.",
                                        reply_markup=main_kb())
        return ConversationHandler.END
    storage.set_chat_state(_conn(context), cid, query_text=text)
    storage.save_filter(_conn(context), cid, text, _state(context, cid)["area"])
    await update.message.reply_text(
        f"Запрос установлен: «{text}». Историю по нему можно копить командой /track.",
        reply_markup=main_kb())
    return ConversationHandler.END


async def save_edit_filter(update, context):
    cid = update.effective_chat.id
    pair = context.user_data.get("editing_filter")
    if pair is None:
        await update.message.reply_text("Редактирование отменено. Откройте фильтры заново.")
        return ConversationHandler.END
    text = norm_query(update.message.text)
    if not text:
        await update.message.reply_text("Пустой запрос. Отправьте исправленный текст или /cancel.")
        return AWAIT_EDIT
    old_query, area = pair
    status = _rename_saved_filter(context, cid, old_query, area, text, area)
    if status == "exists":
        await update.message.reply_text("Такой фильтр уже есть. Пришлите другой запрос или /cancel.")
        return AWAIT_EDIT
    if status == "limit":
        await update.message.reply_text("Достигнут общий лимит отслеживаемых фильтров.")
        return AWAIT_EDIT
    context.user_data.pop("editing_filter", None)
    if status == "missing":
        await update.message.reply_text("Фильтр уже удалён. Откройте список заново.",
                                        reply_markup=main_kb())
    else:
        await update.message.reply_text(
            f"Фильтр исправлен: «{text}» / {_area_label(area)}. "
            "Если отслеживание было включено, оно перенесено на новый запрос. "
            "История прежнего запроса осталась отдельно.",
            reply_markup=main_kb())
    return ConversationHandler.END


async def cancel_custom(update, context):
    context.user_data.pop("editing_filter", None)
    await update.message.reply_text("Отменено.", reply_markup=main_kb())
    return ConversationHandler.END


# --- ConversationHandler: регистрация по инвайт-коду -------------------------
async def cmd_start(update, context):
    cid = update.effective_chat.id
    status = _gate(context, cid)
    if status == "banned":
        await update.message.reply_text("Доступ закрыт.")
        return ConversationHandler.END
    if status == "active":
        _state(context, cid)
        await _reset_and_menu(context, cid, HELP)
        return ConversationHandler.END
    await update.message.reply_text(NEED_INVITE)
    return AWAIT_INVITE


async def save_invite(update, context):
    cid = update.effective_chat.id
    user = update.effective_user
    verdict = storage.redeem_invite(_conn(context), update.message.text,
                                    cid, user.username if user else None)
    if verdict == "banned":
        await update.message.reply_text("Доступ закрыт. Обратитесь к администратору.")
        return ConversationHandler.END
    if verdict == "unknown_code":
        await update.message.reply_text("Код не найден. Проверьте и пришлите ещё раз "
                                        "или /cancel.")
        return AWAIT_INVITE
    if verdict == "exhausted":
        await update.message.reply_text("Этот код уже использован. Нужен новый — "
                                        "попросите у того, кто вас пригласил.")
        return AWAIT_INVITE
    _drop_gate_cache(context, cid)
    _state(context, cid)
    await _reset_and_menu(context, cid, "Доступ открыт.\n\n" + HELP)
    return ConversationHandler.END


async def cancel_invite(update, context):
    await update.message.reply_text("Отменено. /start — попробовать снова.")
    return ConversationHandler.END


# --- админские действия ------------------------------------------------------
def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎟 Код на 1", callback_data="adm:invite:1"),
         InlineKeyboardButton("🎟 Код на 5", callback_data="adm:invite:5")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="adm:users"),
         InlineKeyboardButton("📈 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton("🎫 Выданные коды", callback_data="adm:invites")],
        [InlineKeyboardButton("⟳ Форс-сбор python", callback_data="adm:collect"),
         InlineKeyboardButton("📖 Словарь", callback_data="adm:dict")],
        [InlineKeyboardButton("⬅️ Меню", callback_data="home")],
    ])


ADMIN_TEXT = ("Панель админа.\n"
              "Блокировка — командой: /ban <chat_id>, /unban <chat_id>.")


async def _on_admin_callback(update, context, cid, action):
    """Кнопки админ-панели. Проверяем права здесь же: клавиатура могла остаться
    у бывшего админа — Telegram её не отзывает."""
    if not await _admin_only(update, context):
        return
    if action.startswith("invite:"):
        await _do_invite(context, cid, max_uses=int(action.split(":")[1]))
    elif action == "invites":
        await _do_invites(context, cid)
    elif action == "users":
        await _do_users(context, cid)
    elif action == "stats":
        await _do_stats(context, cid)
    elif action == "dict":
        await _do_dict(context, cid)
    elif action == "collect":
        await _submit(context, cid, "collect", DEFAULT_QUERY, DEFAULT_AREA)


async def _do_invite(context, chat_id, max_uses=1, note=None):
    code = secrets.token_urlsafe(6)
    storage.create_invite(_conn(context), code, chat_id, max_uses, note)
    # код отдельным сообщением и моноширинно — чтобы удобно копировать и пересылать
    await context.bot.send_message(
        chat_id, f"Инвайт-код (использований: {max_uses})"
                 + (f", заметка: {_esc(note)}" if note else "") + f":\n<code>{code}</code>",
        parse_mode="HTML")


async def _do_users(context, chat_id):
    conn = _conn(context)
    total, active = storage.user_counts(conn)
    rows = storage.list_users(conn, limit=40)
    lines = [f"Пользователей: {total} (активных за 7 дней: {active})"]
    for u in rows:
        flag = "🚫" if u["status"] == "banned" else "  "
        name = u["username"] or "—"
        lines.append(f"{flag} {u['chat_id']}  @{name}  "
                     f"фильтров: {u['saved_filter_count']}  "
                     f"LLM сегодня: {u['llm_jobs_today']}")
    await _send_messages(context.bot, chat_id, _chunk(lines, ""))


async def _do_invites(context, chat_id):
    rows = storage.list_invites(_conn(context))
    if not rows:
        await context.bot.send_message(chat_id, "Кодов ещё не выдавали.")
        return
    lines = ["Выданные коды (использовано/всего):"]
    for r in rows:
        left = "исчерпан" if r["used_count"] >= r["max_uses"] else "активен"
        lines.append(f"  {r['code']}  {r['used_count']}/{r['max_uses']}  {left}"
                     + (f"  — {r['note']}" if r["note"] else ""))
    await _send_messages(context.bot, chat_id, _chunk(lines, ""))


async def cmd_invite(update, context):
    """/invite [использований] [заметка] — выдать код доступа."""
    if not await _admin_only(update, context):
        return
    args = context.args or []
    has_count = bool(args) and args[0].isdigit()
    max_uses = int(args[0]) if has_count else 1
    note = " ".join(args[1:] if has_count else args) or None
    await _do_invite(context, update.effective_chat.id, max_uses, note)


async def cmd_invites(update, context):
    if not await _admin_only(update, context):
        return
    await _do_invites(context, update.effective_chat.id)


async def cmd_users(update, context):
    if not await _admin_only(update, context):
        return
    await _do_users(context, update.effective_chat.id)


async def _set_status(update, context, status, done_text):
    if not await _admin_only(update, context):
        return
    cid = update.effective_chat.id
    if not context.args or not context.args[0].lstrip("-").isdigit():
        await context.bot.send_message(cid, "Укажите chat_id: /ban 12345")
        return
    target = int(context.args[0])
    if status == "banned" and _is_admin(target):
        await context.bot.send_message(cid, "Администратора нельзя заблокировать через /ban.")
        return
    ok = storage.set_user_status(_conn(context), target, status)
    _drop_gate_cache(context, target)
    await context.bot.send_message(cid, done_text.format(target) if ok
                                   else f"Пользователь {target} не найден.")


async def cmd_ban(update, context):
    await _set_status(update, context, "banned", "Пользователь {} заблокирован.")


async def cmd_unban(update, context):
    await _set_status(update, context, "active", "Пользователь {} разблокирован.")


async def cmd_stats(update, context):
    if not await _admin_only(update, context):
        return
    await _do_stats(context, update.effective_chat.id)


async def _do_stats(context, cid):
    conn = _conn(context)
    total, active = storage.user_counts(conn)
    queued, running = jobs.queue_depth(conn)
    done, failed = jobs.stats_24h(conn)
    avg = jobs.avg_duration(conn)
    _, left_global = storage.llm_quota_left(conn, cid, _llm_quota_for(cid), LLM_QUOTA_GLOBAL)
    db_mb = os.path.getsize(DB_PATH) / 2**20 if os.path.exists(DB_PATH) else 0
    lines = [
        f"Пользователи: {total} (активных за 7 дней: {active})",
        f"Очередь: {queued} ждут, {running} в работе",
        f"За сутки: успешно {done}, с ошибкой {failed}",
        f"Средняя длительность задачи: {avg/60:.1f} мин" if avg else "Статистики по времени нет",
        f"Общая LLM-квота: осталось {left_global} из {LLM_QUOTA_GLOBAL}",
        f"Отслеживаемых запросов: {jobs.tracked_count(conn)} (предел {TRACKED_MAX_TOTAL})",
        f"Размер БД: {db_mb:.1f} МБ",
    ]
    await context.bot.send_message(cid, "\n".join(lines), reply_markup=admin_kb())


# --- воркеры очереди ---------------------------------------------------------
def _wake_workers(context):
    """Разбудить воркеров сразу после постановки задачи (иначе ждали бы опроса)."""
    ev = context.application.bot_data.get("wake")
    if ev is not None:
        ev.set()


def _can_receive(conn, chat_id):
    if _is_admin(chat_id):
        return True
    user = storage.get_user(conn, chat_id)
    return user is not None and user["status"] == "active"


async def _notify(bot, conn, chat_ids, text, keyboard=None):
    for cid in chat_ids:
        if not _can_receive(conn, cid):
            continue
        try:
            await bot.send_message(cid, text, reply_markup=keyboard)
        except Exception as e:  # чат удалён/бот заблокирован — остальным доставить надо
            print(f"[notify] chat={cid} не доставлено: {type(e).__name__}", flush=True)


async def _announce_result(app, job, res):
    """Разослать подписчикам итог задачи и сразу готовый топ."""
    conn = app.bot_data["conn"]
    chat_ids = jobs.watchers(conn, job["job_id"])
    if not chat_ids:
        return
    what = f"{job['query_text']} / {_area_label(job['area'])}"
    if res.get("status") != "ok":
        await _notify(app.bot, conn, chat_ids,
                      f"Не получилось обновить {what}: {res.get('error')}")
        return
    if job["kind"] == "collect":
        head = (f"Готово [{res.get('method', '?')}]: {what}. Вакансий: {res['n']} "
                f"(новых: {res['new']}, из кэша: {res['reused']}).")
    else:
        head = f"Пересчитано [{res.get('method', '?')}]: {what}, вакансий {res['n']}."
    await _notify(app.bot, conn, chat_ids, head)
    for cid in chat_ids:
        if not _can_receive(conn, cid):
            continue
        st = storage.get_chat_state(conn, cid, STATE_DEFAULTS)
        if st["query_text"] != job["query_text"] or str(st["area"]) != str(job["area"]):
            continue  # пользователь уже переключился на другой запрос — топ не навязываем
        top = analysis.current_top(conn, job["query_text"], job["area"], st["top_n"])
        await _send_messages(app.bot, cid, render_top(top, st), main_kb())


def _top_change_text(query, area, changes):
    if not any(changes.values()):
        return None
    lines = [f"🔔 Изменился топ требований: {query} / {_area_label(area)}"]
    for name, rank, count in changes["entered"]:
        lines.append(f"➕ {name[:65]}: #{rank}, {count} вакансий")
    for name, rank, count in changes["exited"]:
        lines.append(f"➖ {name[:65]}: было #{rank}, {count} вакансий")
    for name, old_rank, old_count, new_rank, new_count in changes["changed"]:
        lines.append(f"↕️ {name[:65]}: #{old_rank} → #{new_rank}, "
                     f"{old_count} → {new_count} вакансий")
    return "\n".join(lines)[:TG_LIMIT]


async def _notify_top_change(app, job, res):
    if job["kind"] != "collect" or res.get("status") != "ok" or res.get("method") != "llm":
        return
    conn = app.bot_data["conn"]
    recipients = jobs.subscribers(conn, job["query_text"], job["area"])
    if not recipients:
        return
    previous = storage.previous_ok_snapshot(conn, job["query_text"], job["area"],
                                            res["run_id"])
    if previous is None or previous["method"] != "llm":
        return
    changes = analysis.top_change(conn, job["query_text"], job["area"], res["run_id"])
    if changes is None:
        return
    message = _top_change_text(job["query_text"], job["area"], changes)
    if message:
        await _notify(app.bot, conn, recipients, message)


async def _run_job(app, job):
    conn = app.bot_data["conn"]
    loop = asyncio.get_running_loop()
    watchers = jobs.watchers(conn, job["job_id"])

    def progress(msg):
        # Прогресс интересен только тем, кто ждёт прямо сейчас. У плановых сборов
        # подписчиков нет — корутину даже не создаём, иначе она повисает в очереди
        # цикла и при остановке бота ругается «never awaited».
        if not watchers:
            return
        coro = _notify(app.bot, conn, watchers, msg)
        try:
            asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError:
            coro.close()  # цикл уже остановлен — идёт выключение

    if job["kind"] == "reanalyze":
        res = await asyncio.to_thread(collector.reanalyze_once, conn, job["query_text"],
                                      job["area"], job["mode"], progress)
    else:
        res = await asyncio.to_thread(collector.collect_once, conn, job["query_text"],
                                      job["area"], job["top_n"], DETAIL_THROTTLE,
                                      progress, job["mode"])
    ok = res.get("status") == "ok"
    jobs.finish(conn, job["job_id"], "done" if ok else "error",
                run_id=res.get("run_id"), method=res.get("method"), n=res.get("n"),
                error=res.get("error") or res.get("status") if not ok else None)
    await _announce_result(app, job, res)
    await _notify_top_change(app, job, res)
    if not ok and ADMINS:
        await _notify(app.bot, conn, [int(a) for a in ADMINS],
                      f"⚠️ Задача {job['job_id']} ({job['query_text']}) упала: "
                      f"{res.get('error') or res.get('status')}")


async def worker_loop(app, worker_id):
    conn = app.bot_data["conn"]
    wake = app.bot_data["wake"]
    while True:
        try:
            job = await asyncio.to_thread(jobs.claim_next, conn)
            if job is None:
                wake.clear()
                try:
                    await asyncio.wait_for(wake.wait(), timeout=QUEUE_POLL_SEC)
                except asyncio.TimeoutError:
                    pass
                continue
            print(f"[worker {worker_id}] задача {job['job_id']}: {job['kind']} "
                  f"{job['query_text']}/{job['area']} mode={job['mode']}", flush=True)
            await _run_job(app, job)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # воркер не должен умирать из-за одной задачи
            print(f"[worker {worker_id}] сбой: {type(e).__name__}: {e}", flush=True)
            await asyncio.sleep(QUEUE_POLL_SEC)


# --- планировщик / старт -----------------------------------------------------
async def scheduled_default(context: ContextTypes.DEFAULT_TYPE):
    """Основной отслеживаемый запрос: часто и через LLM."""
    conn = context.application.bot_data["conn"]
    jobs.enqueue(conn, "collect", DEFAULT_QUERY, DEFAULT_AREA, ANALYZE_TOP_N, None, None)
    _wake_workers(context)


async def scheduled_tracked(context: ContextTypes.DEFAULT_TYPE):
    """Пользовательские подписки: шестичасовой сбор с LLM.

    Дедуп в enqueue схлопнет пары, которые уже стоят в очереди.
    """
    conn = context.application.bot_data["conn"]
    pairs = jobs.tracked_pairs(conn, limit=TRACKED_MAX_TOTAL)
    queued = 0
    for query, area, _subs in pairs:
        if query == DEFAULT_QUERY and str(area) == str(DEFAULT_AREA):
            continue  # у него свой, более частый джоб
        latest = storage.latest_ok_snapshot(conn, query, area)
        if latest and _snapshot_age(latest) < TRACK_INTERVAL:
            continue
        finished = jobs.last_collect_finished(conn, query, area)
        if finished and (datetime.now(timezone.utc)
                         - datetime.fromisoformat(finished)).total_seconds() < TRACK_INTERVAL:
            continue
        _, deduped, _ = jobs.enqueue(conn, "collect", query, area,
                                     ANALYZE_TOP_N, None, None)
        queued += 0 if deduped else 1
    total = jobs.tracked_count(conn)
    if total > TRACKED_MAX_TOTAL:
        print(f"[sweep] отслеживаемых пар {total}, собираю только "
              f"{TRACKED_MAX_TOTAL} самых популярных", flush=True)
    print(f"[sweep] поставлено задач: {queued}", flush=True)
    _wake_workers(context)


async def scheduled_vacuum(context: ContextTypes.DEFAULT_TYPE):
    conn = context.application.bot_data["conn"]
    await asyncio.to_thread(collector.prune_expired, conn)
    await asyncio.to_thread(storage.vacuum, conn)
    await asyncio.to_thread(storage.prune_backups, DB_PATH, collector.history_retention_days())


def _snapshot_age(snapshot):
    return (datetime.now(timezone.utc) - datetime.fromisoformat(snapshot["ts_utc"])).total_seconds()


def _first_delay(conn):
    """Задержка до первого сбора: пусто/устарело -> сразу, иначе остаток интервала."""
    L = storage.latest_ok_snapshot(conn, DEFAULT_QUERY, DEFAULT_AREA)
    if not L:
        return 5
    age = _snapshot_age(L)
    return 5 if age >= COLLECT_INTERVAL else max(5, COLLECT_INTERVAL - age)


def _seed_users(conn):
    """Перевести прежний env-whitelist в таблицу users — строго один раз.

    Повторять на каждом старте нельзя: тогда любой chat_id, побывавший в env,
    оседал бы в users навсегда, и правка переменной не убирала бы доступ.
    """
    # дефолтный запрос отслеживается системно (chat_id=0) — тогда sweep един для всех
    jobs.track(conn, DEFAULT_QUERY, DEFAULT_AREA, 0)
    if storage.meta_flag(conn, "users_seeded"):
        return
    for chat_id in SEED_CHATS | ADMINS:
        try:
            storage.register_user(conn, int(chat_id), invite_code="seed")
        except ValueError:
            print(f"Пропущен нечисловой chat_id в whitelist: {chat_id!r}", flush=True)
    print(f"Стартовый список пользователей перенесён в users: "
          f"{len(SEED_CHATS | ADMINS)} шт.", flush=True)


USER_COMMANDS = [
    BotCommand("top", "Топ требований"),
    BotCommand("vacancies", "Актуальные вакансии"),
    BotCommand("trend", "Тренд за месяц"),
    BotCommand("diff", "Что изменилось"),
    BotCommand("filters", "Фильтры"),
    BotCommand("refresh", "Обновить данные (сбор сейчас)"),
    BotCommand("reanalyze_llm", "Пересчитать топ через LLM (без загрузки)"),
    BotCommand("reanalyze_basic", "Пересчитать топ по словарю (без загрузки)"),
    BotCommand("track", "Копить историю по текущему запросу"),
    BotCommand("untrack", "Перестать отслеживать текущий запрос"),
    BotCommand("tracked", "Мои отслеживаемые запросы"),
    BotCommand("status", "Статус и лимиты"),
    BotCommand("help", "Справка"),
]

ADMIN_COMMANDS = [
    BotCommand("invite", "Выдать инвайт-код"),
    BotCommand("invites", "Выданные коды"),
    BotCommand("users", "Список пользователей"),
    BotCommand("ban", "Заблокировать пользователя"),
    BotCommand("unban", "Разблокировать пользователя"),
    BotCommand("stats", "Статистика очереди и лимитов"),
    BotCommand("collect", "Форс-сбор отслеживаемого запроса"),
    BotCommand("dict", "Кандидаты в словарь технологий"),
]


async def post_init(app):
    app.bot_data["wake"] = asyncio.Event()
    app.bot_data["workers"] = [
        asyncio.create_task(worker_loop(app, i + 1)) for i in range(WORKERS)]
    print(f"Воркеров очереди: {WORKERS}", flush=True)
    await app.bot.set_my_commands(USER_COMMANDS)
    # админам — расширенное меню в их личном чате (scope перекрывает общий список)
    for chat_id in ADMINS:
        try:
            await app.bot.set_my_commands(
                USER_COMMANDS + ADMIN_COMMANDS,
                scope=BotCommandScopeChat(chat_id=int(chat_id)))
        except Exception as e:
            # чат админа боту ещё неизвестен (не писал ему) — не повод падать на старте
            print(f"Меню админа {chat_id} не выставлено: {type(e).__name__}: {e}", flush=True)


async def post_shutdown(app):
    for task in app.bot_data.get("workers", []):
        task.cancel()


def main():
    conn = storage.connect(DB_PATH)
    storage.init_schema(conn)
    _seed_users(conn)
    stale = jobs.requeue_stale(conn)
    if stale:
        print(f"Возвращено в очередь после рестарта: {stale}", flush=True)

    # concurrent_updates(True): апдейты разных пользователей обрабатываются
    # параллельно — долгий сбор одного не блокирует остальных.
    # AIORateLimiter: результат одной задачи рассылается всем подписчикам, без
    # троттлинга это упёрлось бы в лимиты Telegram.
    app = (Application.builder().token(get_token())
           .concurrent_updates(True).rate_limiter(AIORateLimiter())
           .post_init(post_init).post_shutdown(post_shutdown).build())
    app.bot_data["conn"] = conn

    # Нажатие нижней кнопки — обычный текст. Из состояний диалогов его надо исключить,
    # иначе «📊 Топ» уйдёт в поисковый запрос или в поле инвайт-кода; в fallbacks кнопка
    # выполняет действие и закрывает диалог.
    button_filter = filters.Text(list(REPLY_ACTIONS))
    dialog_text = filters.TEXT & ~filters.COMMAND & ~button_filter
    button_fallback = MessageHandler(button_filter, on_reply_button)

    invite_conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={AWAIT_INVITE: [MessageHandler(dialog_text, save_invite)]},
        # /start в fallbacks: иначе повторный /start во время ввода кода не матчится
        # ни состоянием, ни fallback'ом — и бот молча игнорирует пользователя
        fallbacks=[CommandHandler("cancel", cancel_invite),
                   CommandHandler("start", cmd_start), button_fallback],
        per_message=False,
    )
    custom_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(ask_custom, pattern=r"^text:custom$"),
                      CallbackQueryHandler(ask_edit_filter, pattern=r"^edit_filter$")],
        states={AWAIT_TEXT: [MessageHandler(dialog_text, save_custom)],
                AWAIT_EDIT: [MessageHandler(dialog_text, save_edit_filter)]},
        fallbacks=[CommandHandler("cancel", cancel_custom), button_fallback],
        per_message=False,
    )
    app.add_handler(invite_conv)
    app.add_handler(custom_conv)
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("top", cmd_top))
    app.add_handler(CommandHandler("vacancies", cmd_vacancies))
    app.add_handler(CommandHandler("trend", cmd_trend))
    app.add_handler(CommandHandler("diff", cmd_diff))
    app.add_handler(CommandHandler("filters", cmd_filters))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("reanalyze_llm", cmd_reanalyze_llm))
    app.add_handler(CommandHandler("reanalyze_basic", cmd_reanalyze_basic))
    app.add_handler(CommandHandler("track", cmd_track))
    app.add_handler(CommandHandler("untrack", cmd_untrack))
    app.add_handler(CommandHandler("tracked", cmd_tracked))
    app.add_handler(CommandHandler("collect", cmd_collect))
    app.add_handler(CommandHandler("dict", cmd_dict))
    app.add_handler(CommandHandler("invite", cmd_invite))
    app.add_handler(CommandHandler("invites", cmd_invites))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CallbackQueryHandler(on_callback))
    # кнопки нижней клавиатуры — до заглушки unknown, иначе она перехватит их текст
    app.add_handler(MessageHandler(button_filter, on_reply_button))
    # лог всех апдейтов (группа -1: выполняется до основных хендлеров, не мешает им)
    app.add_handler(MessageHandler(filters.ALL, log_update), group=-1)
    app.add_handler(CallbackQueryHandler(log_update), group=-1)
    # заглушка на нераспознанный текст/команды — добавляется последней в группе 0
    app.add_handler(MessageHandler(filters.TEXT, unknown))

    delay = _first_delay(conn)
    L = storage.latest_ok_snapshot(conn, DEFAULT_QUERY, DEFAULT_AREA)
    if L and delay > 5:
        print(f"Кэш свежий ({L['ts_msk']}); первый сбор через {delay/60:.0f} мин", flush=True)
    else:
        print("Кэш пуст или устарел — сбор при старте", flush=True)
    app.job_queue.run_repeating(scheduled_default, interval=COLLECT_INTERVAL,
                                first=delay, name="collect")
    app.job_queue.run_repeating(scheduled_tracked, interval=TRACK_SWEEP_SECONDS,
                                first=60, name="tracked")
    app.job_queue.run_repeating(scheduled_vacuum, interval=86400, first=3600, name="vacuum")
    print(f"Бот запущен. Автосбор и подписки каждые {COLLECT_INTERVAL/3600:g} ч.",
          flush=True)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
