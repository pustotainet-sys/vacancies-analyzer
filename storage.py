"""SQLite-хранилище снимков вакансий и агрегатов технологий.

Одно соединение с check_same_thread=False под общим _LOCK — пишут и фоновая
джоба, и обработчики бота. Агрегаты храним бессрочно (нужны для длинных трендов),
сырые вакансии пруним по retention.
"""

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

_LOCK = threading.Lock()
MSK = timezone(timedelta(hours=3))
SCHEMA_VERSION = "3"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL, ts_msk TEXT NOT NULL,
    query_text TEXT NOT NULL, area TEXT NOT NULL,
    per_page INTEGER, max_items INTEGER,
    total_found INTEGER, vacancy_count INTEGER, enriched_count INTEGER DEFAULT 0,
    method TEXT, models_used_json TEXT, status TEXT NOT NULL, error TEXT, duration_sec REAL);
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

-- Детали вакансии хранятся один раз, снимок ссылается на них: одна и та же вакансия
-- попадает в десятки снимков, и копировать её описание в каждый — гигабайты за год.
CREATE TABLE IF NOT EXISTS vacancy_details (
    vac_id TEXT PRIMARY KEY, name TEXT, company TEXT, published_at TEXT, area TEXT,
    alternate_url TEXT, key_skills_json TEXT, description TEXT,
    snippet_req TEXT, snippet_resp TEXT, fetched_at TEXT);

-- published_at продублирован в связке намеренно: hh меняет его при перепубликации,
-- а снимок должен сохранять порядок «свежие сверху» таким, каким он был на момент
-- сбора. Поле короткое — на экономии от выноса описаний это не сказывается.
CREATE TABLE IF NOT EXISTS snapshot_items (
    run_id INTEGER NOT NULL REFERENCES snapshots(run_id) ON DELETE CASCADE,
    vac_id TEXT NOT NULL, published_at TEXT,
    PRIMARY KEY (run_id, vac_id));
CREATE INDEX IF NOT EXISTS ix_items_vac ON snapshot_items(vac_id);

CREATE TABLE IF NOT EXISTS learned_aliases (
    alias_raw TEXT PRIMARY KEY, canonical TEXT,
    occurrences INTEGER NOT NULL DEFAULT 0, first_seen TEXT, last_seen TEXT,
    status TEXT NOT NULL DEFAULT 'pending');

CREATE TABLE IF NOT EXISTS chat_state (
    chat_id INTEGER PRIMARY KEY,
    query_text TEXT DEFAULT 'python', area TEXT DEFAULT '113',
    period_days INTEGER DEFAULT 30, top_n INTEGER DEFAULT 15, updated_at TEXT);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS users (
    chat_id INTEGER PRIMARY KEY, username TEXT,
    first_seen TEXT, last_seen TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    invite_code TEXT,
    llm_jobs_today INTEGER NOT NULL DEFAULT 0, quota_date TEXT);

CREATE TABLE IF NOT EXISTS invites (
    code TEXT PRIMARY KEY, created_by INTEGER, created_at TEXT,
    max_uses INTEGER NOT NULL DEFAULT 1, used_count INTEGER NOT NULL DEFAULT 0,
    note TEXT);

CREATE TABLE IF NOT EXISTS daily_usage (day TEXT PRIMARY KEY, llm_jobs INTEGER NOT NULL DEFAULT 0);

CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    query_text TEXT NOT NULL, area TEXT NOT NULL,
    top_n INTEGER, mode TEXT,
    status TEXT NOT NULL,
    created_at TEXT, started_at TEXT, finished_at TEXT,
    created_by INTEGER, run_id INTEGER, method TEXT, n INTEGER, error TEXT);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs(status, job_id);

CREATE TABLE IF NOT EXISTS job_watchers (
    job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    chat_id INTEGER NOT NULL,
    PRIMARY KEY (job_id, chat_id));

CREATE TABLE IF NOT EXISTS tracked_queries (
    query_text TEXT NOT NULL, area TEXT NOT NULL, chat_id INTEGER NOT NULL,
    created_at TEXT, PRIMARY KEY (query_text, area, chat_id));

CREATE TABLE IF NOT EXISTS saved_filters (
    chat_id INTEGER NOT NULL, query_text TEXT NOT NULL, area TEXT NOT NULL,
    created_at TEXT NOT NULL, PRIMARY KEY (chat_id, query_text, area));
"""


def connect(db_path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # WAL: читатели не блокируются писателем; busy_timeout — страховка от 'database is locked'
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_schema(conn):
    with _LOCK:
        conn.executescript(_SCHEMA)
        try:
            conn.execute("ALTER TABLE snapshots ADD COLUMN method TEXT")
        except sqlite3.OperationalError:
            pass  # колонка уже есть
        if "models_used_json" not in {row["name"] for row in conn.execute("PRAGMA table_info(snapshots)")}:
            conn.execute("ALTER TABLE snapshots ADD COLUMN models_used_json TEXT")
        seeded = conn.execute("SELECT 1 FROM meta WHERE key='saved_filters_seeded'").fetchone()
        if not seeded:
            conn.execute("INSERT OR IGNORE INTO saved_filters(chat_id, query_text, area, created_at) "
                         "SELECT chat_id, query_text, area, COALESCE(created_at, '') "
                         "FROM tracked_queries WHERE chat_id>0")
            conn.execute("INSERT OR IGNORE INTO saved_filters(chat_id, query_text, area, created_at) "
                         "SELECT chat_id, query_text, area, COALESCE(updated_at, '') "
                         "FROM chat_state")
            conn.execute("INSERT INTO meta(key, value) VALUES('saved_filters_seeded', '1')")
        # Очистить подписки, оставшиеся у заблокированных до исправления /ban.
        conn.execute("DELETE FROM tracked_queries WHERE chat_id IN "
                     "(SELECT chat_id FROM users WHERE status='banned')")
        conn.execute("DELETE FROM job_watchers WHERE chat_id IN "
                     "(SELECT chat_id FROM users WHERE status='banned')")
        conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (SCHEMA_VERSION,))
        conn.commit()
    _migrate_vacancies(conn)


def _migrate_vacancies(conn):
    """Разнести старую таблицу vacancies по vacancy_details + snapshot_items.

    Выполняется один раз: после переноса старая таблица переименовывается в
    vacancies_old (удалить вручную, убедившись, что всё на месте).
    """
    with _LOCK:
        has_old = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='vacancies'").fetchone()
        if not has_old:
            return
        # Каноничная деталь собирается по полям: для каждого берём самое свежее
        # НЕПУСТОЕ значение. Строка последнего снимка целиком не годится — company и
        # alternate_url появились поздним ALTER'ом и в части снимков пусты.
        conn.execute("INSERT OR IGNORE INTO vacancy_details(vac_id) "
                     "SELECT DISTINCT vac_id FROM vacancies")
        for col in ("name", "company", "published_at", "area", "alternate_url",
                    "key_skills_json", "description", "snippet_req", "snippet_resp"):
            conn.execute(f"""
                UPDATE vacancy_details SET {col} = (
                    SELECT v.{col} FROM vacancies v
                    WHERE v.vac_id = vacancy_details.vac_id AND v.{col} IS NOT NULL
                    ORDER BY v.run_id DESC LIMIT 1)""")
        conn.execute("INSERT OR IGNORE INTO snapshot_items(run_id, vac_id, published_at) "
                     "SELECT run_id, vac_id, published_at FROM vacancies")
        conn.execute("ALTER TABLE vacancies RENAME TO vacancies_old")
        moved = conn.execute("SELECT COUNT(*) c FROM vacancy_details").fetchone()["c"]
        links = conn.execute("SELECT COUNT(*) c FROM snapshot_items").fetchone()["c"]
        conn.commit()
    print(f"[storage] хранение вакансий нормализовано: деталей {moved}, связок {links}; "
          f"старая таблица сохранена как vacancies_old", flush=True)


def _now():
    utc = datetime.now(timezone.utc)
    return utc.isoformat(), utc.astimezone(MSK).strftime("%Y-%m-%d %H:%M МСК")


def meta_flag(conn, key):
    """Одноразовые отметки миграций: True, если флаг уже был выставлен."""
    with _LOCK:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row:
            return True
        conn.execute("INSERT INTO meta(key, value) VALUES(?, ?)", (key, _now()[0]))
        conn.commit()
    return False


def _today():
    """Текущая дата по МСК — граница суток для квот."""
    return datetime.now(MSK).strftime("%Y-%m-%d")


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
                   total_found, vacancy_count, enriched_count, method, models_used_json,
                   status, duration_sec)
               VALUES(?,?,?,?,?,?,?,?,?,?,?, 'ok', ?)""",
            (ts_utc, ts_msk, meta["query_text"], str(meta["area"]),
             meta.get("per_page"), meta.get("max_items"), meta.get("total_found"),
             n, meta.get("enriched_count", 0), meta.get("method"),
             json.dumps(result.get("models_used", []), ensure_ascii=False),
             meta.get("duration_sec")))
        run_id = cur.lastrowid

        conn.executemany(
            "INSERT INTO tech_counts(run_id, tech, df, share, source) VALUES(?,?,?,?,?)",
            [(run_id, tech, df, (df / n if n else 0.0), result["source"].get(tech, "text"))
             for tech, df in result["df"].items()])
        conn.executemany(
            "INSERT INTO cooccurrence(run_id, tech_a, tech_b, cnt) VALUES(?,?,?,?)",
            [(run_id, a, b, c) for (a, b), c in result["cooccur"].items()])
        # деталь обновляем, но не затираем непустое описание пустым: если hh не отдал
        # description в этот раз, старое всё ещё лучше, чем ничего
        conn.executemany(
            """INSERT INTO vacancy_details(vac_id, name, company, published_at, area,
                   alternate_url, key_skills_json, description, snippet_req, snippet_resp,
                   fetched_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(vac_id) DO UPDATE SET
                   name = COALESCE(excluded.name, vacancy_details.name),
                   company = COALESCE(excluded.company, vacancy_details.company),
                   published_at = COALESCE(excluded.published_at, vacancy_details.published_at),
                   area = COALESCE(excluded.area, vacancy_details.area),
                   alternate_url = COALESCE(excluded.alternate_url,
                                            vacancy_details.alternate_url),
                   key_skills_json = COALESCE(excluded.key_skills_json,
                                              vacancy_details.key_skills_json),
                   description = COALESCE(excluded.description, vacancy_details.description),
                   snippet_req = COALESCE(excluded.snippet_req, vacancy_details.snippet_req),
                   snippet_resp = COALESCE(excluded.snippet_resp, vacancy_details.snippet_resp),
                   fetched_at = excluded.fetched_at""",
            [(r["vac_id"], r.get("name"), r.get("company"), r.get("published_at"),
              r.get("area"), r.get("alternate_url"), r.get("key_skills_json"),
              r.get("description"), r.get("snippet_req"), r.get("snippet_resp"), ts_utc)
             for r in raw_rows])
        conn.executemany(
            "INSERT OR IGNORE INTO snapshot_items(run_id, vac_id, published_at) VALUES(?,?,?)",
            [(run_id, r["vac_id"], r.get("published_at")) for r in raw_rows])
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


def prune_raw(conn, tracked_days, adhoc_days=None):
    """Удалить сырые строки vacancies старше срока (агрегаты не трогаем).

    Для отслеживаемых запросов срок длинный (нужен пересчёт истории), для разовых —
    короткий: именно они и раздувают базу при многих пользователях.
    """
    now = datetime.now(timezone.utc)
    tracked_cut = (now - timedelta(days=tracked_days)).isoformat()
    adhoc_cut = (now - timedelta(days=adhoc_days if adhoc_days is not None
                                 else tracked_days)).isoformat()
    with _LOCK:
        conn.execute(
            """DELETE FROM snapshot_items WHERE run_id IN (
                   SELECT s.run_id FROM snapshots s
                   LEFT JOIN (SELECT DISTINCT query_text, area FROM tracked_queries) t
                     ON t.query_text = s.query_text AND t.area = s.area
                   WHERE s.ts_utc < CASE WHEN t.query_text IS NULL THEN ? ELSE ? END)""",
            (adhoc_cut, tracked_cut))
        # деталь живёт, пока на неё ссылается хоть один снимок
        conn.execute("DELETE FROM vacancy_details WHERE vac_id NOT IN "
                     "(SELECT vac_id FROM snapshot_items)")
        conn.commit()


def prune_history(conn, history_days):
    """Удалить старые снимки с агрегатами, завершённые задачи и суточные счётчики."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=history_days)
    with _LOCK:
        conn.execute("DELETE FROM snapshots WHERE ts_utc < ?", (cutoff.isoformat(),))
        # tech_counts, cooccurrence и snapshot_items удаляются по ON DELETE CASCADE.
        conn.execute("DELETE FROM vacancy_details WHERE vac_id NOT IN "
                     "(SELECT vac_id FROM snapshot_items)")
        conn.execute("DELETE FROM jobs WHERE status IN ('done', 'error') "
                     "AND finished_at < ?", (cutoff.isoformat(),))
        conn.execute("DELETE FROM daily_usage WHERE day < ?", (cutoff.date().isoformat(),))
        conn.commit()


def prune_backups(db_path, history_days):
    """Удалить созданные при деплоях копии SQLite старше срока истории."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=history_days)
    database = Path(db_path)
    for backup in database.parent.glob(f"{database.name}.pre-*"):
        if backup.is_file() and not backup.is_symlink():
            modified = datetime.fromtimestamp(backup.stat().st_mtime, timezone.utc)
            if modified < cutoff:
                backup.unlink()


def vacuum(conn):
    """Сжать файл БД после массового удаления старых данных."""
    with _LOCK:
        conn.execute("VACUUM")


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
            """SELECT d.vac_id, d.name, d.company, i.published_at, d.alternate_url
               FROM snapshot_items i JOIN vacancy_details d USING (vac_id)
               WHERE i.run_id=? ORDER BY i.published_at DESC, d.vac_id DESC LIMIT ?""",
            (run_id, limit)).fetchall()
    return [dict(r) for r in rows]


def raw_vacancies(conn, run_id):
    """Все сохранённые сырые вакансии снимка (для пересчёта без обращения к hh)."""
    with _LOCK:
        rows = conn.execute(
            """SELECT d.vac_id, d.name, d.company, i.published_at, d.area, d.alternate_url,
                      d.key_skills_json, d.description, d.snippet_req, d.snippet_resp
               FROM snapshot_items i JOIN vacancy_details d USING (vac_id)
               WHERE i.run_id=?""", (run_id,)).fetchall()
    return [dict(r) for r in rows]


def replace_analysis(conn, run_id, result, method):
    """Заменить топ (tech_counts + cooccurrence + method) снимка на пересчитанный.

    Сырьё vacancies и метка времени не трогаются — это тот же сбор, просто пере-анализ.
    """
    n = result["n"] or 1
    with _LOCK:
        conn.execute("DELETE FROM tech_counts WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM cooccurrence WHERE run_id=?", (run_id,))
        conn.executemany(
            "INSERT INTO tech_counts(run_id, tech, df, share, source) VALUES(?,?,?,?,?)",
            [(run_id, t, df, df / n, result["source"].get(t, "text"))
             for t, df in result["df"].items()])
        conn.executemany(
            "INSERT INTO cooccurrence(run_id, tech_a, tech_b, cnt) VALUES(?,?,?,?)",
            [(run_id, a, b, c) for (a, b), c in result["cooccur"].items()])
        conn.execute("UPDATE snapshots SET method=?, models_used_json=? WHERE run_id=?",
                     (method, json.dumps(result.get("models_used", []), ensure_ascii=False), run_id))
        conn.commit()


def get_vacancy(conn, vac_id):
    """Сохранённая вакансия по vac_id (для показа деталей)."""
    with _LOCK:
        row = conn.execute("SELECT * FROM vacancy_details WHERE vac_id=?",
                           (str(vac_id),)).fetchone()
    return dict(row) if row else None


def stored_details(conn, vac_ids):
    """Уже сохранённые детали по списку vac_id: {vac_id: {key_skills_json, description}}.

    Деталь хранится в единственном экземпляре — по ней и решаем, качать ли из hh.
    """
    if not vac_ids:
        return {}
    ids = [str(v) for v in vac_ids]
    ph = ",".join("?" * len(ids))
    with _LOCK:
        rows = conn.execute(
            f"SELECT vac_id, key_skills_json, description FROM vacancy_details "
            f"WHERE vac_id IN ({ph}) AND description IS NOT NULL", ids).fetchall()
    return {r["vac_id"]: {"key_skills_json": r["key_skills_json"],
                          "description": r["description"]} for r in rows}


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


def save_filter(conn, chat_id, query_text, area):
    ts_utc, _ = _now()
    with _LOCK:
        conn.execute("INSERT OR IGNORE INTO saved_filters(chat_id, query_text, area, created_at) "
                     "VALUES(?,?,?,?)", (chat_id, query_text, str(area), ts_utc))
        conn.commit()


def user_saved_filters(conn, chat_id):
    with _LOCK:
        rows = conn.execute(
            "SELECT query_text, area FROM saved_filters WHERE chat_id=? "
            "ORDER BY created_at, rowid", (chat_id,)).fetchall()
    return [(row["query_text"], row["area"]) for row in rows]


def delete_filter(conn, chat_id, query_text, area):
    with _LOCK:
        conn.execute("DELETE FROM saved_filters WHERE chat_id=? AND query_text=? AND area=?",
                     (chat_id, query_text, str(area)))
        conn.commit()


def rename_filter(conn, chat_id, old_query, old_area, new_query, new_area):
    """Исправить сохранённую пару и перенести её подписку, если она включена."""
    old_area, new_area = str(old_area), str(new_area)
    with _LOCK:
        source = conn.execute(
            "SELECT 1 FROM saved_filters WHERE chat_id=? AND query_text=? AND area=?",
            (chat_id, old_query, old_area)).fetchone()
        if not source:
            return "missing"
        if (old_query, old_area) == (new_query, new_area):
            return "unchanged"
        target = conn.execute(
            "SELECT 1 FROM saved_filters WHERE chat_id=? AND query_text=? AND area=?",
            (chat_id, new_query, new_area)).fetchone()
        if target:
            return "exists"
        conn.execute("UPDATE saved_filters SET query_text=?, area=? "
                     "WHERE chat_id=? AND query_text=? AND area=?",
                     (new_query, new_area, chat_id, old_query, old_area))
        conn.execute(
            "INSERT OR IGNORE INTO tracked_queries(query_text, area, chat_id, created_at) "
            "SELECT ?, ?, chat_id, created_at FROM tracked_queries "
            "WHERE chat_id=? AND query_text=? AND area=?",
            (new_query, new_area, chat_id, old_query, old_area))
        conn.execute("DELETE FROM tracked_queries WHERE chat_id=? AND query_text=? AND area=?",
                     (chat_id, old_query, old_area))
        conn.execute("UPDATE chat_state SET query_text=?, area=? "
                     "WHERE chat_id=? AND query_text=? AND area=?",
                     (new_query, new_area, chat_id, old_query, old_area))
        conn.commit()
    return "renamed"


# --- пользователи и инвайты --------------------------------------------------
def get_user(conn, chat_id):
    with _LOCK:
        row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
    return dict(row) if row else None


def register_user(conn, chat_id, username=None, invite_code=None, status="active"):
    """Завести пользователя (или вернуть существующего без изменений)."""
    ts_utc, _ = _now()
    with _LOCK:
        conn.execute(
            """INSERT OR IGNORE INTO users(chat_id, username, first_seen, last_seen,
                   status, invite_code) VALUES(?,?,?,?,?,?)""",
            (chat_id, username, ts_utc, ts_utc, status, invite_code))
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
    return dict(row)


def touch_user(conn, chat_id, username=None):
    ts_utc, _ = _now()
    with _LOCK:
        conn.execute("UPDATE users SET last_seen=?, username=COALESCE(?, username) "
                     "WHERE chat_id=?", (ts_utc, username, chat_id))
        conn.commit()


def set_user_status(conn, chat_id, status):
    with _LOCK:
        cur = conn.execute("UPDATE users SET status=? WHERE chat_id=?", (status, chat_id))
        if cur.rowcount and status == "banned":
            conn.execute("DELETE FROM tracked_queries WHERE chat_id=?", (chat_id,))
            conn.execute("DELETE FROM job_watchers WHERE chat_id=?", (chat_id,))
        conn.commit()
    return cur.rowcount > 0


def list_users(conn, limit=50):
    with _LOCK:
        rows = conn.execute(
            "SELECT chat_id, username, status, first_seen, last_seen, llm_jobs_today "
            "FROM users ORDER BY last_seen DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def user_counts(conn, active_days=7):
    """(всего, активных за N дней) — для /stats."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=active_days)).isoformat()
    with _LOCK:
        total = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        active = conn.execute("SELECT COUNT(*) c FROM users WHERE last_seen >= ?",
                              (cutoff,)).fetchone()["c"]
    return total, active


def create_invite(conn, code, created_by, max_uses=1, note=None):
    ts_utc, _ = _now()
    with _LOCK:
        conn.execute(
            "INSERT INTO invites(code, created_by, created_at, max_uses, note) VALUES(?,?,?,?,?)",
            (code, created_by, ts_utc, max_uses, note))
        conn.commit()


def redeem_invite(conn, code, chat_id, username=None):
    """Погасить код и зарегистрировать пользователя одной транзакцией.

    Возвращает 'ok' | 'unknown_code' | 'exhausted' | 'banned'.
    """
    ts_utc, _ = _now()
    code = (code or "").strip()
    with _LOCK:
        existing = conn.execute("SELECT status FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if existing and existing["status"] == "banned":
            return "banned"
        row = conn.execute("SELECT * FROM invites WHERE code=?", (code,)).fetchone()
        if not row:
            return "unknown_code"
        if row["used_count"] >= row["max_uses"]:
            return "exhausted"
        conn.execute("UPDATE invites SET used_count = used_count + 1 WHERE code=?", (code,))
        conn.execute(
            """INSERT INTO users(chat_id, username, first_seen, last_seen, status, invite_code)
               VALUES(?,?,?,?,'active',?)
               ON CONFLICT(chat_id) DO UPDATE SET
                   status='active', last_seen=excluded.last_seen,
                   invite_code=COALESCE(users.invite_code, excluded.invite_code)""",
            (chat_id, username, ts_utc, ts_utc, code))
        conn.commit()
    return "ok"


def list_invites(conn, limit=30):
    with _LOCK:
        rows = conn.execute(
            "SELECT code, max_uses, used_count, note, created_at FROM invites "
            "ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


# --- суточные квоты на LLM ---------------------------------------------------
def try_spend_llm_quota(conn, chat_id, user_limit, global_limit):
    """Списать одну LLM-задачу. Возвращает 'ok' | 'user_exceeded' | 'global_exceeded'.

    Персональный счётчик — в users, общий — в daily_usage; оба сбрасываются сменой
    даты МСК. Глобальный проверяется первым: он предохранитель бюджета, и при его
    исчерпании вызывающий откатывается на basic, а не отказывает пользователю.
    """
    day = _today()
    with _LOCK:
        used = conn.execute("SELECT llm_jobs FROM daily_usage WHERE day=?", (day,)).fetchone()
        if used and used["llm_jobs"] >= global_limit:
            return "global_exceeded"
        row = conn.execute("SELECT llm_jobs_today, quota_date FROM users WHERE chat_id=?",
                           (chat_id,)).fetchone()
        spent = row["llm_jobs_today"] if row and row["quota_date"] == day else 0
        if spent >= user_limit:
            return "user_exceeded"
        conn.execute("UPDATE users SET llm_jobs_today=?, quota_date=? WHERE chat_id=?",
                     (spent + 1, day, chat_id))
        conn.execute("INSERT INTO daily_usage(day, llm_jobs) VALUES(?, 1) "
                     "ON CONFLICT(day) DO UPDATE SET llm_jobs = llm_jobs + 1", (day,))
        conn.commit()
    return "ok"


def llm_quota_left(conn, chat_id, user_limit, global_limit):
    """(остаток пользователя, остаток общий) — для /status и /stats."""
    day = _today()
    with _LOCK:
        row = conn.execute("SELECT llm_jobs_today, quota_date FROM users WHERE chat_id=?",
                           (chat_id,)).fetchone()
        used = conn.execute("SELECT llm_jobs FROM daily_usage WHERE day=?", (day,)).fetchone()
    spent = row["llm_jobs_today"] if row and row["quota_date"] == day else 0
    total = used["llm_jobs"] if used else 0
    return max(0, user_limit - spent), max(0, global_limit - total)
