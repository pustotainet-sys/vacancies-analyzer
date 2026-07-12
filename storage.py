"""SQLite-хранилище снимков вакансий и агрегатов технологий.

Одно соединение с check_same_thread=False под общим _LOCK — пишут и фоновая
джоба, и обработчики бота. Агрегаты храним бессрочно (нужны для длинных трендов),
сырые вакансии пруним по retention.
"""

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

_LOCK = threading.Lock()
MSK = timezone(timedelta(hours=3))
SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL, ts_msk TEXT NOT NULL,
    query_text TEXT NOT NULL, area TEXT NOT NULL,
    per_page INTEGER, max_items INTEGER,
    total_found INTEGER, vacancy_count INTEGER, enriched_count INTEGER DEFAULT 0,
    status TEXT NOT NULL, error TEXT, duration_sec REAL);
CREATE INDEX IF NOT EXISTS ix_snap_query ON snapshots(query_text, area, ts_utc);

CREATE TABLE IF NOT EXISTS tech_counts (
    run_id INTEGER NOT NULL REFERENCES snapshots(run_id) ON DELETE CASCADE,
    tech TEXT NOT NULL, df INTEGER NOT NULL, share REAL NOT NULL, source TEXT,
    PRIMARY KEY (run_id, tech));
CREATE INDEX IF NOT EXISTS ix_tech ON tech_counts(tech);

CREATE TABLE IF NOT EXISTS cooccurrence (
    run_id INTEGER NOT NULL REFERENCES snapshots(run_id) ON DELETE CASCADE,
    tech_a TEXT NOT NULL, tech_b TEXT NOT NULL, cnt INTEGER NOT NULL,
    PRIMARY KEY (run_id, tech_a, tech_b));

CREATE TABLE IF NOT EXISTS vacancies (
    run_id INTEGER NOT NULL REFERENCES snapshots(run_id) ON DELETE CASCADE,
    vac_id TEXT NOT NULL, name TEXT, company TEXT, published_at TEXT, area TEXT,
    alternate_url TEXT, key_skills_json TEXT, description TEXT,
    snippet_req TEXT, snippet_resp TEXT,
    PRIMARY KEY (run_id, vac_id));

CREATE TABLE IF NOT EXISTS learned_aliases (
    alias_raw TEXT PRIMARY KEY, canonical TEXT,
    occurrences INTEGER NOT NULL DEFAULT 0, first_seen TEXT, last_seen TEXT,
    status TEXT NOT NULL DEFAULT 'pending');

CREATE TABLE IF NOT EXISTS chat_state (
    chat_id INTEGER PRIMARY KEY,
    query_text TEXT DEFAULT 'python', area TEXT DEFAULT '113',
    period_days INTEGER DEFAULT 30, top_n INTEGER DEFAULT 15, updated_at TEXT);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""


def connect(db_path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn):
    with _LOCK:
        conn.executescript(_SCHEMA)
        # миграции для БД, созданных ранней версией схемы
        for col, typ in (("company", "TEXT"), ("alternate_url", "TEXT")):
            try:
                conn.execute(f"ALTER TABLE vacancies ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass  # колонка уже есть
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                     (SCHEMA_VERSION,))
        conn.commit()


def _now():
    utc = datetime.now(timezone.utc)
    return utc.isoformat(), utc.astimezone(MSK).strftime("%Y-%m-%d %H:%M МСК")


# --- запись ------------------------------------------------------------------
def write_snapshot(conn, meta, result, raw_rows):
    """Записать успешный снимок одной транзакцией. Возвращает run_id.

    meta: dict(query_text, area, per_page, max_items, total_found, duration_sec).
    result: вывод extract.aggregate. raw_rows: список dict сырых вакансий.
    """
    ts_utc, ts_msk = _now()
    n = result["n"]
    with _LOCK:
        cur = conn.execute(
            """INSERT INTO snapshots(ts_utc, ts_msk, query_text, area, per_page, max_items,
                   total_found, vacancy_count, enriched_count, status, duration_sec)
               VALUES(?,?,?,?,?,?,?,?,?, 'ok', ?)""",
            (ts_utc, ts_msk, meta["query_text"], str(meta["area"]),
             meta.get("per_page"), meta.get("max_items"), meta.get("total_found"),
             n, meta.get("enriched_count", 0), meta.get("duration_sec")))
        run_id = cur.lastrowid

        conn.executemany(
            "INSERT INTO tech_counts(run_id, tech, df, share, source) VALUES(?,?,?,?,?)",
            [(run_id, tech, df, (df / n if n else 0.0), result["source"].get(tech, "text"))
             for tech, df in result["df"].items()])
        conn.executemany(
            "INSERT INTO cooccurrence(run_id, tech_a, tech_b, cnt) VALUES(?,?,?,?)",
            [(run_id, a, b, c) for (a, b), c in result["cooccur"].items()])
        conn.executemany(
            """INSERT OR IGNORE INTO vacancies(run_id, vac_id, name, company, published_at,
                   area, alternate_url, key_skills_json, description, snippet_req, snippet_resp)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            [(run_id, r["vac_id"], r.get("name"), r.get("company"), r.get("published_at"),
              r.get("area"), r.get("alternate_url"), r.get("key_skills_json"),
              r.get("description"), r.get("snippet_req"), r.get("snippet_resp"))
             for r in raw_rows])
        conn.commit()
    return run_id


def write_error_snapshot(conn, query_text, area, error):
    ts_utc, ts_msk = _now()
    with _LOCK:
        cur = conn.execute(
            """INSERT INTO snapshots(ts_utc, ts_msk, query_text, area,
                   vacancy_count, status, error)
               VALUES(?,?,?,?, 0, 'error', ?)""",
            (ts_utc, ts_msk, query_text, str(area), str(error)[:500]))
        conn.commit()
        return cur.lastrowid


def upsert_learned(conn, raw_skills, known_aliases):
    """Апсертнуть наблюдённые key_skills как кандидаты словаря.

    known_aliases — множество уже известных форм (нижний регистр); их пропускаем.
    """
    ts_utc, _ = _now()
    with _LOCK:
        for name, cnt in (raw_skills or {}).items():
            low = name.strip().lower()
            if not low or low in known_aliases:
                continue
            conn.execute(
                """INSERT INTO learned_aliases(alias_raw, occurrences, first_seen, last_seen)
                   VALUES(?,?,?,?)
                   ON CONFLICT(alias_raw) DO UPDATE SET
                       occurrences = occurrences + excluded.occurrences,
                       last_seen = excluded.last_seen""",
                (low, cnt, ts_utc, ts_utc))
        conn.commit()


def learned_map(conn):
    """Промоутнутые алиасы: alias_raw -> canonical (только status='mapped')."""
    with _LOCK:
        rows = conn.execute(
            "SELECT alias_raw, canonical FROM learned_aliases "
            "WHERE status='mapped' AND canonical IS NOT NULL").fetchall()
    return {r["alias_raw"]: r["canonical"] for r in rows}


def pending_aliases(conn, limit=30):
    with _LOCK:
        rows = conn.execute(
            "SELECT alias_raw, occurrences FROM learned_aliases "
            "WHERE status='pending' ORDER BY occurrences DESC LIMIT ?", (limit,)).fetchall()
    return [(r["alias_raw"], r["occurrences"]) for r in rows]


def prune_raw(conn, older_than_days):
    """Удалить только сырые строки vacancies старше N дней (агрегаты не трогаем)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).isoformat()
    with _LOCK:
        conn.execute(
            "DELETE FROM vacancies WHERE run_id IN "
            "(SELECT run_id FROM snapshots WHERE ts_utc < ?)", (cutoff,))
        conn.commit()


# --- чтение снимков ----------------------------------------------------------
def latest_ok_snapshot(conn, query_text, area):
    with _LOCK:
        return conn.execute(
            "SELECT * FROM snapshots WHERE query_text=? AND area=? AND status='ok' "
            "ORDER BY ts_utc DESC LIMIT 1", (query_text, str(area))).fetchone()


def previous_ok_snapshot(conn, query_text, area, before_run_id):
    with _LOCK:
        return conn.execute(
            "SELECT * FROM snapshots WHERE query_text=? AND area=? AND status='ok' "
            "AND run_id < ? ORDER BY ts_utc DESC LIMIT 1",
            (query_text, str(area), before_run_id)).fetchone()


def last_snapshot(conn, query_text, area):
    """Последний снимок любого статуса (для вывода ошибки в /status)."""
    with _LOCK:
        return conn.execute(
            "SELECT * FROM snapshots WHERE query_text=? AND area=? "
            "ORDER BY ts_utc DESC LIMIT 1", (query_text, str(area))).fetchone()


def nearest_snapshot_to(conn, query_text, area, target_utc_iso):
    """Ok-снимок, ближайший по времени к target (в любую сторону)."""
    with _LOCK:
        return conn.execute(
            """SELECT *, ABS(julianday(ts_utc) - julianday(?)) AS dist
               FROM snapshots WHERE query_text=? AND area=? AND status='ok'
               ORDER BY dist ASC LIMIT 1""",
            (target_utc_iso, query_text, str(area))).fetchone()


def earliest_ok_ts(conn, query_text, area):
    with _LOCK:
        row = conn.execute(
            "SELECT ts_utc FROM snapshots WHERE query_text=? AND area=? AND status='ok' "
            "ORDER BY ts_utc ASC LIMIT 1", (query_text, str(area))).fetchone()
    return row["ts_utc"] if row else None


def tech_counts(conn, run_id):
    with _LOCK:
        rows = conn.execute(
            "SELECT tech, df, share, source FROM tech_counts WHERE run_id=?",
            (run_id,)).fetchall()
    return {r["tech"]: {"df": r["df"], "share": r["share"], "source": r["source"]}
            for r in rows}


def snapshot_vacancies(conn, run_id, limit):
    """Вакансии снимка, свежие сверху."""
    with _LOCK:
        rows = conn.execute(
            "SELECT vac_id, name, company, published_at, alternate_url FROM vacancies "
            "WHERE run_id=? ORDER BY published_at DESC LIMIT ?", (run_id, limit)).fetchall()
    return [dict(r) for r in rows]


def get_vacancy(conn, vac_id):
    """Последняя сохранённая версия вакансии по vac_id (для показа деталей)."""
    with _LOCK:
        row = conn.execute(
            "SELECT * FROM vacancies WHERE vac_id=? ORDER BY run_id DESC LIMIT 1",
            (str(vac_id),)).fetchone()
    return dict(row) if row else None


def cooccurrence(conn, run_id, tech, limit=8):
    with _LOCK:
        rows = conn.execute(
            "SELECT tech_a, tech_b, cnt FROM cooccurrence WHERE run_id=? "
            "AND (tech_a=? OR tech_b=?) ORDER BY cnt DESC LIMIT ?",
            (run_id, tech, tech, limit)).fetchall()
    out = []
    for r in rows:
        other = r["tech_b"] if r["tech_a"] == tech else r["tech_a"]
        out.append((other, r["cnt"]))
    return out


# --- состояние чатов ---------------------------------------------------------
def get_chat_state(conn, chat_id, defaults):
    with _LOCK:
        row = conn.execute("SELECT * FROM chat_state WHERE chat_id=?", (chat_id,)).fetchone()
        if row:
            return dict(row)
        ts_utc, _ = _now()
        conn.execute(
            """INSERT INTO chat_state(chat_id, query_text, area, period_days, top_n, updated_at)
               VALUES(?,?,?,?,?,?)""",
            (chat_id, defaults["query_text"], str(defaults["area"]),
             defaults["period_days"], defaults["top_n"], ts_utc))
        conn.commit()
        row = conn.execute("SELECT * FROM chat_state WHERE chat_id=?", (chat_id,)).fetchone()
    return dict(row)


def set_chat_state(conn, chat_id, **fields):
    if not fields:
        return
    ts_utc, _ = _now()
    fields["updated_at"] = ts_utc
    cols = ", ".join(f"{k}=?" for k in fields)
    with _LOCK:
        conn.execute(f"UPDATE chat_state SET {cols} WHERE chat_id=?",
                     (*fields.values(), chat_id))
        conn.commit()
