"""Чтение из хранилища: текущий топ, тренд за период, дифф с прошлого снимка.

Возвращает структурные dict без форматирования. Доли — fraction 0..1; движение
считаем в процентных пунктах (п.п.).
"""

from datetime import datetime, timedelta

import storage

MIN_SHARE = 0.02   # порог, чтобы технология считалась «появившейся»
PP = 0.5           # порог движения доли, процентные пункты


def _shares(conn, run_id):
    return {t: c["share"] for t, c in storage.tech_counts(conn, run_id).items()}


def current_top(conn, query, area, top_n):
    L = storage.latest_ok_snapshot(conn, query, area)
    if not L:
        last = storage.last_snapshot(conn, query, area)
        return {"error": "no_data",
                "last_error": last["error"] if last else None}
    counts = storage.tech_counts(conn, L["run_id"])
    rows = sorted(counts.items(), key=lambda kv: -kv[1]["df"])[:top_n]
    return {
        "snapshot": dict(L),
        "rows": [(tech, c["df"], c["share"], c["source"]) for tech, c in rows],
    }


def _classify(share_now, share_then, top_n):
    techs = set(share_now) | set(share_then)
    rose, fell, new, gone = [], [], [], []
    for t in techs:
        a = share_then.get(t, 0.0)
        b = share_now.get(t, 0.0)
        d = 100 * (b - a)  # процентные пункты
        if t not in share_then and b >= MIN_SHARE:
            new.append((t, b))
        elif t not in share_now and a >= MIN_SHARE:
            gone.append((t, a))
        elif d >= PP:
            rose.append((t, d, a, b))
        elif d <= -PP:
            fell.append((t, d, a, b))
    rose.sort(key=lambda x: -x[1])
    fell.sort(key=lambda x: x[1])
    new.sort(key=lambda x: -x[1])
    gone.sort(key=lambda x: -x[1])
    return {"rose": rose[:top_n], "fell": fell[:top_n],
            "new": new[:top_n], "gone": gone[:top_n]}


def month_trend(conn, query, area, period_days, top_n):
    L = storage.latest_ok_snapshot(conn, query, area)
    if not L:
        return {"error": "no_data"}
    target = (datetime.fromisoformat(L["ts_utc"]) - timedelta(days=period_days)).isoformat()
    A = storage.nearest_snapshot_to(conn, query, area, target)
    if A is None or A["run_id"] == L["run_id"]:
        return {"error": "insufficient_history",
                "have_since": storage.earliest_ok_ts(conn, query, area)}
    gap = datetime.fromisoformat(L["ts_utc"]) - datetime.fromisoformat(A["ts_utc"])
    if gap.total_seconds() < period_days * 86400 * 0.5:
        return {"error": "insufficient_history",
                "have_since": storage.earliest_ok_ts(conn, query, area)}
    return {"from": dict(A), "to": dict(L),
            **_classify(_shares(conn, L["run_id"]), _shares(conn, A["run_id"]), top_n)}


def diff_since_last(conn, query, area, top_n):
    L = storage.latest_ok_snapshot(conn, query, area)
    P = storage.previous_ok_snapshot(conn, query, area, L["run_id"]) if L else None
    if not L or not P:
        return {"error": "need_two_snapshots"}
    return {"from": dict(P), "to": dict(L),
            **_classify(_shares(conn, L["run_id"]), _shares(conn, P["run_id"]), top_n)}


def top_change(conn, query, area, run_id, top_n=15):
    """Изменения мест и частот в топе конкретного нового снимка; None для первого."""
    previous = storage.previous_ok_snapshot(conn, query, area, run_id)
    if previous is None:
        return None
    before = sorted(storage.tech_counts(conn, previous["run_id"]).items(),
                    key=lambda pair: (-pair[1]["df"], pair[0]))[:top_n]
    after = sorted(storage.tech_counts(conn, run_id).items(),
                   key=lambda pair: (-pair[1]["df"], pair[0]))[:top_n]
    old = {name: (rank, stats["df"]) for rank, (name, stats) in enumerate(before, 1)}
    new = {name: (rank, stats["df"]) for rank, (name, stats) in enumerate(after, 1)}
    entered = [(name, *new[name]) for name in new.keys() - old.keys()]
    exited = [(name, *old[name]) for name in old.keys() - new.keys()]
    changed = [(name, *old[name], *new[name]) for name in new.keys() & old.keys()
               if old[name] != new[name]]
    entered.sort(key=lambda entry: entry[1])
    exited.sort(key=lambda entry: entry[1])
    changed.sort(key=lambda entry: entry[3])
    return {"entered": entered, "exited": exited, "changed": changed}
