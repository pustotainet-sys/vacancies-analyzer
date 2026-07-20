"""Очередь задач сбора: постановка с дедупликацией, выдача воркерам, подписчики.

Задача — строка в таблице jobs; заинтересованные чаты — в job_watchers. Дедуп:
одинаковый (kind, query_text, area, mode) в состоянии queued/running не создаёт
вторую задачу, а добавляет подписчика — десять человек, спросивших «golang»,
получают один сбор. Все операции идут под storage._LOCK: он же обеспечивает
атомарность захвата задачи двумя воркерами.
"""

from datetime import datetime, timedelta, timezone

from storage import _LOCK, _now

_ACTIVE_SQL = "('queued','running')"


def enqueue(conn, kind, query_text, area, top_n, mode, chat_id=None):
    """Поставить задачу или подписаться на уже идущую.

    Возвращает (job_id, deduped, position): deduped=True — задача уже была;
    position — сколько задач стоит перед ней в очереди (0 = следующая или уже идёт).
    """
    ts_utc, _ = _now()
    with _LOCK:
        row = conn.execute(
            f"""SELECT job_id, status FROM jobs
                WHERE kind=? AND query_text=? AND area=? AND IFNULL(mode,'')=IFNULL(?,'')
                  AND status IN {_ACTIVE_SQL} ORDER BY job_id LIMIT 1""",
            (kind, query_text, str(area), mode)).fetchone()
        deduped = row is not None
        if deduped:
            job_id = row["job_id"]
        else:
            cur = conn.execute(
                """INSERT INTO jobs(kind, query_text, area, top_n, mode, status,
                       created_at, created_by)
                   VALUES(?,?,?,?,?, 'queued', ?, ?)""",
                (kind, query_text, str(area), top_n, mode, ts_utc, chat_id))
            job_id = cur.lastrowid
        if chat_id is not None:
            conn.execute("INSERT OR IGNORE INTO job_watchers(job_id, chat_id) VALUES(?,?)",
                         (job_id, chat_id))
        pos = conn.execute(
            "SELECT COUNT(*) c FROM jobs WHERE status='queued' AND job_id < ?",
            (job_id,)).fetchone()["c"]
        conn.commit()
    return job_id, deduped, pos


def has_active(conn, kind, query_text, area, mode):
    """Есть ли уже queued/running такая задача (проверка перед списанием квоты)."""
    with _LOCK:
        row = conn.execute(
            f"""SELECT 1 FROM jobs WHERE kind=? AND query_text=? AND area=?
                  AND IFNULL(mode,'')=IFNULL(?,'') AND status IN {_ACTIVE_SQL} LIMIT 1""",
            (kind, query_text, str(area), mode)).fetchone()
    return row is not None


def claim_next(conn):
    """Атомарно взять следующую queued-задачу в работу. None — очередь пуста."""
    ts_utc, _ = _now()
    with _LOCK:
        row = conn.execute(
            """UPDATE jobs SET status='running', started_at=?
               WHERE job_id = (SELECT job_id FROM jobs WHERE status='queued'
                               ORDER BY job_id LIMIT 1)
               RETURNING *""", (ts_utc,)).fetchone()
        conn.commit()
    return dict(row) if row else None


def finish(conn, job_id, status, **fields):
    """Закрыть задачу: status='done'|'error' плюс run_id/method/n/error."""
    ts_utc, _ = _now()
    fields = {k: v for k, v in fields.items() if v is not None}
    cols = "".join(f", {k}=?" for k in fields)
    with _LOCK:
        conn.execute(f"UPDATE jobs SET status=?, finished_at=?{cols} WHERE job_id=?",
                     (status, ts_utc, *fields.values(), job_id))
        conn.commit()


def watchers(conn, job_id):
    with _LOCK:
        rows = conn.execute("SELECT chat_id FROM job_watchers WHERE job_id=?",
                            (job_id,)).fetchall()
    return [r["chat_id"] for r in rows]


def position(conn, job_id):
    with _LOCK:
        row = conn.execute(
            "SELECT COUNT(*) c FROM jobs WHERE status='queued' AND job_id < ?",
            (job_id,)).fetchone()
    return row["c"]


def queue_depth(conn):
    """(в очереди, в работе)."""
    with _LOCK:
        rows = conn.execute(
            f"SELECT status, COUNT(*) c FROM jobs WHERE status IN {_ACTIVE_SQL} "
            "GROUP BY status").fetchall()
    by = {r["status"]: r["c"] for r in rows}
    return by.get("queued", 0), by.get("running", 0)


def avg_duration(conn, last_n=20):
    """Средняя длительность последних успешных задач, сек (None — статистики нет)."""
    with _LOCK:
        rows = conn.execute(
            "SELECT started_at, finished_at FROM jobs WHERE status='done' "
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL "
            "ORDER BY job_id DESC LIMIT ?", (last_n,)).fetchall()
    if not rows:
        return None
    secs = [(datetime.fromisoformat(r["finished_at"])
             - datetime.fromisoformat(r["started_at"])).total_seconds() for r in rows]
    return sum(secs) / len(secs)


def eta_seconds(conn, job_id):
    """Грубая оценка ожидания: (позиция + 1) × средняя длительность / число воркеров."""
    avg = avg_duration(conn)
    if avg is None:
        return None
    return (position(conn, job_id) + 1) * avg


def requeue_stale(conn):
    """Вернуть в очередь задачи, оставшиеся running после падения процесса."""
    with _LOCK:
        cur = conn.execute(
            "UPDATE jobs SET status='queued', started_at=NULL WHERE status='running'")
        conn.commit()
    return cur.rowcount


def stats_24h(conn):
    """(done, error) за последние сутки — для /stats."""
    since = datetime.now(timezone.utc) - timedelta(days=1)
    with _LOCK:
        rows = conn.execute(
            "SELECT status, COUNT(*) c FROM jobs WHERE finished_at >= ? GROUP BY status",
            (since.isoformat(),)).fetchall()
    by = {r["status"]: r["c"] for r in rows}
    return by.get("done", 0), by.get("error", 0)


# --- отслеживаемые запросы (подписки) ----------------------------------------
def track(conn, query_text, area, chat_id):
    ts_utc, _ = _now()
    with _LOCK:
        conn.execute(
            "INSERT OR IGNORE INTO tracked_queries(query_text, area, chat_id, created_at) "
            "VALUES(?,?,?,?)", (query_text, str(area), chat_id, ts_utc))
        conn.commit()


def untrack(conn, query_text, area, chat_id):
    with _LOCK:
        cur = conn.execute(
            "DELETE FROM tracked_queries WHERE query_text=? AND area=? AND chat_id=?",
            (query_text, str(area), chat_id))
        conn.commit()
    return cur.rowcount > 0


def user_tracked(conn, chat_id):
    with _LOCK:
        rows = conn.execute(
            "SELECT query_text, area FROM tracked_queries WHERE chat_id=? ORDER BY created_at",
            (chat_id,)).fetchall()
    return [(r["query_text"], r["area"]) for r in rows]


def is_tracked(conn, query_text, area):
    with _LOCK:
        row = conn.execute(
            "SELECT 1 FROM tracked_queries WHERE query_text=? AND area=? LIMIT 1",
            (query_text, str(area))).fetchone()
    return row is not None


def tracked_pairs(conn, limit=None):
    """Уникальные (query_text, area) по убыванию числа подписчиков."""
    with _LOCK:
        rows = conn.execute(
            "SELECT query_text, area, COUNT(*) subs FROM tracked_queries "
            "GROUP BY query_text, area ORDER BY subs DESC, query_text"
            + (" LIMIT ?" if limit else ""), (limit,) if limit else ()).fetchall()
    return [(r["query_text"], r["area"], r["subs"]) for r in rows]


def tracked_count(conn):
    with _LOCK:
        row = conn.execute(
            "SELECT COUNT(*) c FROM (SELECT 1 FROM tracked_queries GROUP BY query_text, area)"
        ).fetchone()
    return row["c"]
