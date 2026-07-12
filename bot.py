"""Telegram-бот анализа требований hh.ru (технологии, тренды, дифф).

Фоновый сбор дефолтного запроса (python/113) раз в COLLECT_HOURS часов через
JobQueue; по кнопкам — текущий топ технологий, тренд за период, дифф с прошлого
сбора и фильтры. Кастомный запрос собирается по требованию (анализ «сейчас»).

Запуск:
    export TELEGRAM_BOT_TOKEN="123:ABC..."       # от @BotFather
    export HH_CLIENT_ID=... HH_CLIENT_SECRET=...  # приложение hh (для токена)
    .venv/bin/python bot.py
Токен также читается из token.txt рядом со скриптом.
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import envfile
envfile.load()  # .env -> os.environ ДО чтения параметров модулями и константами ниже

from telegram import (BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
                      ReplyKeyboardRemove, Update)
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes, ConversationHandler, MessageHandler,
                          filters)

import analysis
import collector
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
ONDEMAND_TOP_N = int(os.environ.get("ONDEMAND_TOP_N", "100"))
VACS_LIST_N = int(os.environ.get("VACS_LIST_N", "12"))
DETAIL_THROTTLE = float(os.environ.get("DETAIL_THROTTLE", "0.2"))
COLLECT_INTERVAL = int(float(os.environ.get("COLLECT_HOURS", "6")) * 3600)
# сколько секунд снимок кастомного запроса считается свежим: пока свеж — /top читает
# его из базы без пересбора и обращений к LLM. Принудительный сбор (⟳/refresh) — всегда.
CUSTOM_FRESH_SECONDS = int(float(os.environ.get("CUSTOM_FRESH_HOURS",
                                               os.environ.get("COLLECT_HOURS", "6"))) * 3600)
DB_PATH = os.environ.get("DB_PATH", os.path.join(HERE, "hh_stats.db"))

AREA_NAMES = {"113": "Россия", "1": "Москва", "2": "СПб"}
STATE_DEFAULTS = {"query_text": DEFAULT_QUERY, "area": DEFAULT_AREA,
                  "period_days": PERIOD_DEFAULT, "top_n": TOP_N_DEFAULT}
AWAIT_TEXT = 1  # состояние ConversationHandler


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


ALLOWED = _id_set("TELEGRAM_ALLOWED_CHATS")
ADMINS = _id_set("TELEGRAM_ADMIN_CHATS")


def _allowed(chat_id):
    return not ALLOWED or str(chat_id) in ALLOWED


def _is_admin(chat_id):
    return str(chat_id) in ADMINS


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
                "Тренды копятся по расписанию для python/Россия — вернитесь позже."]
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
        [InlineKeyboardButton("⚙️ Фильтры", callback_data="flt"),
         InlineKeyboardButton("⟳ Собрать сейчас", callback_data="collect")],
    ])


def _mark(cur, val, label):
    return ("• " + label) if str(cur) == str(val) else label


def filter_kb(state):
    a, p, n, q = state["area"], state["period_days"], state["top_n"], state["query_text"]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_mark(a, "113", "Россия"), callback_data="area:113"),
         InlineKeyboardButton(_mark(a, "1", "Москва"), callback_data="area:1"),
         InlineKeyboardButton(_mark(a, "2", "СПб"), callback_data="area:2")],
        [InlineKeyboardButton(_mark(p, 30, "30д"), callback_data="period:30"),
         InlineKeyboardButton(_mark(p, 60, "60д"), callback_data="period:60"),
         InlineKeyboardButton(_mark(p, 90, "90д"), callback_data="period:90")],
        [InlineKeyboardButton(_mark(n, 10, "топ-10"), callback_data="topn:10"),
         InlineKeyboardButton(_mark(n, 15, "топ-15"), callback_data="topn:15"),
         InlineKeyboardButton(_mark(n, 30, "топ-30"), callback_data="topn:30")],
        [InlineKeyboardButton(_mark(q, "python", "python"), callback_data="text:python"),
         InlineKeyboardButton("✏️ свой запрос", callback_data="text:custom")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="home")],
    ])


MENU_TEXT = ("Анализ требований вакансий hh.ru.\n"
             "Выберите действие:")
FILTER_TEXT = "Фильтры анализа (• — выбрано):"


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
    "Бот анализирует, какие технологии требуют в вакансиях hh.ru (без ИИ, "
    "по key_skills и тексту описаний).\n\n"
    "📊 Топ требований — самые частые технологии сейчас\n"
    "📋 Вакансии — список актуальных вакансий, по кнопке — подробности\n"
    "📈 Тренд за месяц — что выросло/упало/появилось/исчезло\n"
    "🔁 Что изменилось — дифф с прошлого сбора\n"
    "⚙️ Фильтры — регион, период, размер топа, свой запрос\n"
    "⟳ Собрать сейчас (кнопка) или /refresh — принудительный сбор\n"
    "/collect_llm — сбор с анализом через LLM; /collect_basic — без LLM\n\n"
    "По расписанию собирается python/Россия; свой запрос считается «здесь и сейчас», "
    "без истории трендов."
)


async def _reset_and_menu(context, chat_id, text):
    """Убрать «призрачную» reply-клавиатуру (от прежнего бота на этом токене) и дать меню."""
    await context.bot.send_message(chat_id, text, reply_markup=ReplyKeyboardRemove())
    await context.bot.send_message(chat_id, MENU_TEXT, reply_markup=main_kb())


async def cmd_start(update, context):
    cid = update.effective_chat.id
    if not _allowed(cid):
        await update.message.reply_text("Доступ ограничен.")
        return
    _state(context, cid)
    await _reset_and_menu(context, cid, HELP)


async def cmd_top(update, context):
    if not _allowed(update.effective_chat.id):
        return
    await _do_top(context, update.effective_chat.id)


async def cmd_vacancies(update, context):
    if not _allowed(update.effective_chat.id):
        return
    await _do_vacancies(context, update.effective_chat.id)


async def cmd_trend(update, context):
    if not _allowed(update.effective_chat.id):
        return
    await _do_trend(context, update.effective_chat.id, kind="trend")


async def cmd_diff(update, context):
    if not _allowed(update.effective_chat.id):
        return
    await _do_trend(context, update.effective_chat.id, kind="diff")


async def cmd_filters(update, context):
    if not _allowed(update.effective_chat.id):
        return
    st = _state(context, update.effective_chat.id)
    await update.message.reply_text(FILTER_TEXT, reply_markup=filter_kb(st))


async def cmd_status(update, context):
    cid = update.effective_chat.id
    if not _allowed(cid):
        return
    conn = _conn(context)
    st = _state(context, cid)
    L = storage.latest_ok_snapshot(conn, DEFAULT_QUERY, DEFAULT_AREA)
    last = storage.last_snapshot(conn, DEFAULT_QUERY, DEFAULT_AREA)
    lines = [f"Ваш активный запрос: {st['query_text']} / {_area_label(st['area'])}",
             f"Отслеживается (тренды): {DEFAULT_QUERY} / {_area_label(DEFAULT_AREA)}",
             f"Автосбор каждые {COLLECT_INTERVAL/3600:g} ч."]
    if L:
        method = L["method"] if "method" in L.keys() and L["method"] else "?"
        lines.append(f"Последний сбор: {L['ts_msk']}; вакансий {L['vacancy_count']} "
                     f"(деталей {L['enriched_count']}); метод: {method}")
    else:
        lines.append("Успешных сборов ещё нет.")
    llm_state = "включён" if llm.available() else "нет ключа (OPENROUTER_API_KEY)"
    lines.append(f"LLM: {llm_state}; режим: {os.environ.get('LLM_MODE', 'auto')}")
    lines.append("Модели (приоритет): " + ", ".join(llm.models()))
    if last and last["status"] == "error":
        lines.append(f"⚠️ Последний сбор с ошибкой: {last['error']}")
    if collector.is_busy():
        lines.append("Идёт сбор…")
    await context.bot.send_message(cid, "\n".join(lines), reply_markup=main_kb())


async def _collect_active(context, chat_id, mode=None):
    """Принудительный сбор активного запроса чата (то, что показывают /top и /vacancies)."""
    if collector.is_busy():
        await context.bot.send_message(chat_id, "Сбор уже идёт — дождитесь окончания.")
        return
    st = _state(context, chat_id)
    is_default = st["query_text"] == DEFAULT_QUERY and str(st["area"]) == str(DEFAULT_AREA)
    top_n = ANALYZE_TOP_N if is_default else ONDEMAND_TOP_N
    label = {"llm": " (LLM)", "basic": " (базовый)"}.get(mode, "")
    await context.bot.send_message(
        chat_id, f"Запускаю сбор{label}: {st['query_text']} / {_area_label(st['area'])}…")
    await _run_collect(context, st["query_text"], st["area"], top_n,
                       notify_chat=chat_id, mode=mode)


async def cmd_refresh(update, context):
    """Принудительный сбор активного запроса (режим по умолчанию, LLM_MODE)."""
    cid = update.effective_chat.id
    if not _allowed(cid):
        return
    await _collect_active(context, cid)


async def cmd_collect_llm(update, context):
    """Сбор с обязательным анализом через LLM."""
    cid = update.effective_chat.id
    if not _allowed(cid):
        return
    await _collect_active(context, cid, mode="llm")


async def cmd_collect_basic(update, context):
    """Сбор с базовым анализом (без LLM, по словарю)."""
    cid = update.effective_chat.id
    if not _allowed(cid):
        return
    await _collect_active(context, cid, mode="basic")


async def cmd_collect(update, context):
    cid = update.effective_chat.id
    if not _is_admin(cid):
        await context.bot.send_message(cid, "Только для админа (TELEGRAM_ADMIN_CHATS).")
        return
    await context.bot.send_message(cid, "Запускаю сбор отслеживаемого запроса…")
    await _run_collect(context, DEFAULT_QUERY, DEFAULT_AREA, ANALYZE_TOP_N,
                       notify_chat=cid)


async def cmd_dict(update, context):
    cid = update.effective_chat.id
    if not _is_admin(cid):
        return
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
    """Ответ на текст/команду, которые не распознаны (вне диалога).

    Заодно убирает возможную чужую reply-клавиатуру от прежнего бота на этом токене.
    """
    cid = update.effective_chat.id
    if not _allowed(cid):
        return
    await _reset_and_menu(context, cid, "Не понял. Старые кнопки убраны — пользуйтесь меню ниже.")


# --- ядро действий -----------------------------------------------------------
async def _run_collect(context, query, area, top_n, notify_chat=None, summary=True, mode=None):
    """Запустить сбор. progress-сообщение («Догружаю N…») шлётся только при новых
    вакансиях; итог — по возвращённому статусу (summary=False подавляет «Готово»)."""
    conn = _conn(context)
    loop = asyncio.get_running_loop()

    def progress(msg):
        if notify_chat is not None:
            asyncio.run_coroutine_threadsafe(
                context.bot.send_message(notify_chat, msg), loop)

    res = await asyncio.to_thread(collector.collect_once, conn, query, area,
                                  top_n, DETAIL_THROTTLE, progress, mode)
    status = res.get("status")
    if notify_chat is not None:
        if status == "busy":
            await context.bot.send_message(notify_chat, "Сбор уже идёт, подождите…")
        elif status == "error":
            await context.bot.send_message(notify_chat, f"Ошибка сбора: {res['error']}")
        elif status == "ok" and summary:
            await context.bot.send_message(
                notify_chat, f"Готово [{res.get('method', '?')}]. Вакансий: {res['n']} "
                             f"(новых: {res['new']}, из кэша: {res['reused']}). "
                             f"Топ: {', '.join(res['top'])}")
    if status == "error" and ADMINS:
        for a in ADMINS:
            await context.bot.send_message(int(a), f"⚠️ Сбор упал: {res['error']}")
    return res


def _snapshot_age(row):
    """Возраст ok-снимка в секундах (None, если снимка нет)."""
    if not row:
        return None
    return (datetime.now(timezone.utc) - datetime.fromisoformat(row["ts_utc"])).total_seconds()


async def _ensure_custom_snapshot(context, chat_id, st):
    """Для не-дефолтного запроса собрать по требованию — но только если базы нет/устарела.

    Если снимок свежий (моложе CUSTOM_FRESH_SECONDS) — читаем его как есть, без пересбора
    и без обращений к LLM. Итог не шлём (следом идёт таблица топа).
    """
    if st["query_text"] == DEFAULT_QUERY and str(st["area"]) == str(DEFAULT_AREA):
        return  # отслеживаемый запрос — используем данные планового сбора
    conn = _conn(context)
    age = _snapshot_age(storage.latest_ok_snapshot(conn, st["query_text"], st["area"]))
    if age is not None and age < CUSTOM_FRESH_SECONDS:
        return  # база свежая — без сбора и LLM
    if collector.is_busy():
        await context.bot.send_message(
            chat_id, "Сейчас идёт другой сбор — показываю последние данные по запросу "
                     "(если есть). Повторите позже для свежих.")
        return
    await _run_collect(context, st["query_text"], st["area"], ONDEMAND_TOP_N,
                       notify_chat=chat_id, summary=False)


async def _do_top(context, chat_id):
    st = _state(context, chat_id)
    await _ensure_custom_snapshot(context, chat_id, st)
    res = analysis.current_top(_conn(context), st["query_text"], st["area"], st["top_n"])
    await _send_messages(context.bot, chat_id, render_top(res, st), main_kb())


async def _do_vacancies(context, chat_id):
    st = _state(context, chat_id)
    await _ensure_custom_snapshot(context, chat_id, st)
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
    if not _allowed(cid):
        await q.answer("Доступ ограничен.", show_alert=True)
        return
    await q.answer()
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
        await _collect_active(context, cid)
    elif data == "flt":
        st = _state(context, cid)
        await q.edit_message_text(FILTER_TEXT, reply_markup=filter_kb(st))
    elif data == "home":
        await q.edit_message_text(MENU_TEXT, reply_markup=main_kb())
    elif ":" in data:
        key, _, val = data.partition(":")
        field = {"area": "area", "period": "period_days", "topn": "top_n",
                 "text": "query_text"}.get(key)
        if field == "query_text":
            val = "python"  # text:python; text:custom обрабатывает ConversationHandler
            storage.set_chat_state(_conn(context), cid, query_text=val)
        elif field in ("period_days", "top_n"):
            storage.set_chat_state(_conn(context), cid, **{field: int(val)})
        elif field == "area":
            storage.set_chat_state(_conn(context), cid, area=val)
        st = _state(context, cid)
        await q.edit_message_reply_markup(reply_markup=filter_kb(st))


# --- ConversationHandler: свой запрос ---------------------------------------
async def ask_custom(update, context):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text("Отправьте поисковый запрос (например: golang backend). "
                               "/cancel — отмена.")
    return AWAIT_TEXT


async def save_custom(update, context):
    cid = update.effective_chat.id
    text = update.message.text.strip()
    storage.set_chat_state(_conn(context), cid, query_text=text)
    await update.message.reply_text(
        f"Запрос установлен: «{text}». Это разовый анализ без истории трендов.",
        reply_markup=main_kb())
    return ConversationHandler.END


async def cancel_custom(update, context):
    await update.message.reply_text("Отменено.", reply_markup=main_kb())
    return ConversationHandler.END


# --- планировщик / старт -----------------------------------------------------
async def scheduled_collect(context: ContextTypes.DEFAULT_TYPE):
    await _run_collect(context, DEFAULT_QUERY, DEFAULT_AREA, ANALYZE_TOP_N)


def _first_delay(conn):
    """Задержка до первого сбора: пусто/устарело -> сразу, иначе остаток интервала."""
    L = storage.latest_ok_snapshot(conn, DEFAULT_QUERY, DEFAULT_AREA)
    if not L:
        return 5
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(L["ts_utc"])).total_seconds()
    return 5 if age >= COLLECT_INTERVAL else max(5, COLLECT_INTERVAL - age)


async def post_init(app):
    await app.bot.set_my_commands([
        BotCommand("top", "Топ требований"),
        BotCommand("vacancies", "Актуальные вакансии"),
        BotCommand("trend", "Тренд за месяц"),
        BotCommand("diff", "Что изменилось"),
        BotCommand("filters", "Фильтры"),
        BotCommand("refresh", "Обновить данные (сбор сейчас)"),
        BotCommand("collect_llm", "Сбор с анализом через LLM"),
        BotCommand("collect_basic", "Сбор без LLM (по словарю)"),
        BotCommand("status", "Статус сбора"),
        BotCommand("help", "Справка"),
    ])


def main():
    conn = storage.connect(DB_PATH)
    storage.init_schema(conn)

    # concurrent_updates(True): апдейты разных пользователей обрабатываются
    # параллельно — долгий сбор одного не блокирует остальных.
    app = (Application.builder().token(get_token())
           .concurrent_updates(True).post_init(post_init).build())
    app.bot_data["conn"] = conn

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(ask_custom, pattern=r"^text:custom$")],
        states={AWAIT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_custom)]},
        fallbacks=[CommandHandler("cancel", cancel_custom)],
        per_message=False,
    )
    app.add_handler(conv)
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("top", cmd_top))
    app.add_handler(CommandHandler("vacancies", cmd_vacancies))
    app.add_handler(CommandHandler("trend", cmd_trend))
    app.add_handler(CommandHandler("diff", cmd_diff))
    app.add_handler(CommandHandler("filters", cmd_filters))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("collect_llm", cmd_collect_llm))
    app.add_handler(CommandHandler("collect_basic", cmd_collect_basic))
    app.add_handler(CommandHandler("collect", cmd_collect))
    app.add_handler(CommandHandler("dict", cmd_dict))
    app.add_handler(CallbackQueryHandler(on_callback))
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
    app.job_queue.run_repeating(scheduled_collect, interval=COLLECT_INTERVAL,
                                first=delay, name="collect")
    print(f"Бот запущен. Автосбор каждые {COLLECT_INTERVAL/3600:g} ч.", flush=True)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
