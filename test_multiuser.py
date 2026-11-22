"""Тесты многопользовательской обвязки: очередь, квоты, инвайты.

Логика конкурентная (несколько воркеров и общий счётчик), поэтому проверяем
именно её, а не рендеринг сообщений.
"""

import sqlite3
import threading

import pytest

import envfile
import jobs
import storage


@pytest.fixture
def conn(tmp_path):
    c = storage.connect(str(tmp_path / "t.db"))
    storage.init_schema(c)
    return c


# --- разбор .env -------------------------------------------------------------
@pytest.mark.parametrize("line, expected", [
    ("K=5235543291          # кому /collect, /dict и алерты", "5235543291"),
    ("K=value", "value"),
    ("K=  value  ", "value"),
    ("K=", ""),
    ("export K=value # хвост", "value"),
    ('K="значение # с решёткой"', "значение # с решёткой"),
    ("K='одинарные'", "одинарные"),
    ("K=sk-or-v1-abc#def", "sk-or-v1-abc#def"),  # # без пробела — часть значения
])
def test_env_parsing(tmp_path, monkeypatch, line, expected):
    path = tmp_path / ".env"
    path.write_text(line + "\n", encoding="utf-8")
    monkeypatch.delenv("K", raising=False)
    assert envfile.load(str(path))["K"] == expected


def test_env_skips_full_line_comments(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("# K=nope\nK2=yes\n", encoding="utf-8")
    monkeypatch.delenv("K2", raising=False)
    loaded = envfile.load(str(path))
    assert loaded == {"K2": "yes"}


# --- нижняя клавиатура -------------------------------------------------------
def test_reply_layout_matches_actions():
    import bot
    flat = [b for row in bot.REPLY_LAYOUT for b in row] + [bot.ADMIN_BUTTON]
    assert sorted(flat) == sorted(bot.REPLY_ACTIONS), "раскладка и действия разошлись"
    assert len(flat) == len(set(flat)), "кнопка продублирована"


def test_admin_button_shown_only_to_admins(monkeypatch):
    import bot
    monkeypatch.setattr(bot, "ADMINS", {"42"})
    plain = [b.text for row in bot.reply_kb(7).keyboard for b in row]
    admin = [b.text for row in bot.reply_kb(42).keyboard for b in row]
    assert bot.ADMIN_BUTTON not in plain
    assert bot.ADMIN_BUTTON in admin
    assert plain == admin[:-1]  # обычные кнопки те же, админская добавлена в конец


def test_every_admin_callback_is_handled():
    """Каждой кнопке панели — ветка в _on_admin_callback, иначе нажатие молчит."""
    import inspect

    import bot
    src = inspect.getsource(bot._on_admin_callback)
    for row in bot.admin_kb().inline_keyboard:
        for btn in row:
            if btn.callback_data.startswith("adm:"):
                action = btn.callback_data[len("adm:"):].split(":")[0]
                assert action in src, f"кнопка {btn.text!r} не обрабатывается"


def test_every_reply_action_is_handled():
    """Каждой кнопке должна отвечать ветка в on_reply_button, иначе нажатие молчит."""
    import inspect

    import bot
    src = inspect.getsource(bot.on_reply_button)
    for action in bot.REPLY_ACTIONS.values():
        assert f'"{action}"' in src, f"действие {action!r} не обрабатывается"


# --- очередь -----------------------------------------------------------------
def test_enqueue_dedupes_same_request(conn):
    first, dup1, _ = jobs.enqueue(conn, "collect", "golang", "113", 100, "llm", 111)
    second, dup2, _ = jobs.enqueue(conn, "collect", "golang", "113", 100, "llm", 222)
    assert (dup1, dup2) == (False, True)
    assert first == second
    assert sorted(jobs.watchers(conn, first)) == [111, 222]


def test_enqueue_separates_different_mode(conn):
    a, _, _ = jobs.enqueue(conn, "collect", "golang", "113", 100, "llm", 1)
    b, dup, _ = jobs.enqueue(conn, "collect", "golang", "113", 100, "basic", 2)
    assert not dup and a != b


def test_position_reflects_queue_order(conn):
    jobs.enqueue(conn, "collect", "a", "113", 100, None, 1)
    _, _, pos = jobs.enqueue(conn, "collect", "b", "113", 100, None, 1)
    assert pos == 1


def test_claim_next_gives_each_job_once(conn):
    for q in ("a", "b", "c"):
        jobs.enqueue(conn, "collect", q, "113", 100, None, 1)
    claimed, lock = [], threading.Lock()

    def worker():
        while True:
            job = jobs.claim_next(conn)
            if job is None:
                return
            with lock:
                claimed.append(job["job_id"])

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(claimed) == [1, 2, 3]  # ни одна задача не выдана дважды


def test_claim_next_returns_none_when_empty(conn):
    assert jobs.claim_next(conn) is None


def test_requeue_stale_restores_running_jobs(conn):
    jobs.enqueue(conn, "collect", "a", "113", 100, None, 1)
    jobs.claim_next(conn)
    assert jobs.queue_depth(conn) == (0, 1)
    assert jobs.requeue_stale(conn) == 1
    assert jobs.queue_depth(conn) == (1, 0)


def test_finish_marks_done_and_clears_queue(conn):
    job_id, _, _ = jobs.enqueue(conn, "collect", "a", "113", 100, None, 1)
    jobs.claim_next(conn)
    jobs.finish(conn, job_id, "done", run_id=7, method="llm", n=100)
    assert jobs.queue_depth(conn) == (0, 0)
    assert jobs.stats_24h(conn) == (1, 0)


def test_has_active_only_while_queued_or_running(conn):
    job_id, _, _ = jobs.enqueue(conn, "collect", "a", "113", 100, "llm", 1)
    assert jobs.has_active(conn, "collect", "a", "113", "llm")
    jobs.finish(conn, job_id, "done")
    assert not jobs.has_active(conn, "collect", "a", "113", "llm")


# --- квоты -------------------------------------------------------------------
def test_user_quota_blocks_after_limit(conn):
    storage.register_user(conn, 1)
    assert [storage.try_spend_llm_quota(conn, 1, 2, 100) for _ in range(3)] == [
        "ok", "ok", "user_exceeded"]


def test_user_quota_is_per_user(conn):
    storage.register_user(conn, 1)
    storage.register_user(conn, 2)
    storage.try_spend_llm_quota(conn, 1, 1, 100)
    assert storage.try_spend_llm_quota(conn, 2, 1, 100) == "ok"


def test_global_quota_wins_over_user_quota(conn):
    storage.register_user(conn, 1)
    storage.try_spend_llm_quota(conn, 1, 99, 1)
    assert storage.try_spend_llm_quota(conn, 1, 99, 1) == "global_exceeded"


def test_quota_resets_on_new_day(conn):
    storage.register_user(conn, 1)
    storage.try_spend_llm_quota(conn, 1, 1, 100)
    assert storage.try_spend_llm_quota(conn, 1, 1, 100) == "user_exceeded"
    conn.execute("UPDATE users SET quota_date='2000-01-01' WHERE chat_id=1")
    conn.execute("DELETE FROM daily_usage")
    conn.commit()
    assert storage.try_spend_llm_quota(conn, 1, 1, 100) == "ok"


def test_quota_left_reports_remaining(conn):
    storage.register_user(conn, 1)
    storage.try_spend_llm_quota(conn, 1, 5, 10)
    assert storage.llm_quota_left(conn, 1, 5, 10) == (4, 9)


# --- инвайты и доступ --------------------------------------------------------
def test_invite_redeems_once(conn):
    storage.create_invite(conn, "CODE", 1, max_uses=1)
    assert storage.redeem_invite(conn, "CODE", 777) == "ok"
    assert storage.redeem_invite(conn, "CODE", 888) == "exhausted"
    assert storage.get_user(conn, 777)["status"] == "active"
    assert storage.get_user(conn, 888) is None


def test_invite_with_multiple_uses(conn):
    storage.create_invite(conn, "TEAM", 1, max_uses=2)
    assert [storage.redeem_invite(conn, "TEAM", cid) for cid in (1, 2, 3)] == [
        "ok", "ok", "exhausted"]


def test_unknown_invite_rejected(conn):
    assert storage.redeem_invite(conn, "NOPE", 1) == "unknown_code"


def test_meta_flag_fires_once(conn):
    """Сидинг env-whitelist должен отработать один раз, иначе правка .env не убирает доступ."""
    assert storage.meta_flag(conn, "users_seeded") is False
    assert storage.meta_flag(conn, "users_seeded") is True
    assert storage.meta_flag(conn, "other") is False


def test_ban_changes_status(conn):
    storage.register_user(conn, 1)
    assert storage.set_user_status(conn, 1, "banned")
    assert storage.get_user(conn, 1)["status"] == "banned"
    assert not storage.set_user_status(conn, 999, "banned")


# --- подписки ----------------------------------------------------------------
def test_track_is_idempotent_and_listed(conn):
    jobs.track(conn, "golang", "113", 1)
    jobs.track(conn, "golang", "113", 1)
    assert jobs.user_tracked(conn, 1) == [("golang", "113")]
    assert jobs.tracked_count(conn) == 1


def test_tracked_pairs_ranked_by_subscribers(conn):
    jobs.track(conn, "rust", "113", 1)
    jobs.track(conn, "golang", "113", 1)
    jobs.track(conn, "golang", "113", 2)
    assert jobs.tracked_pairs(conn)[0][:2] == ("golang", "113")


def test_untrack_removes_only_own_subscription(conn):
    jobs.track(conn, "golang", "113", 1)
    jobs.track(conn, "golang", "113", 2)
    assert jobs.untrack(conn, "golang", "113", 1)
    assert jobs.tracked_count(conn) == 1
    assert jobs.user_tracked(conn, 2) == [("golang", "113")]


# --- нормализованное хранение вакансий ---------------------------------------
def _result(n, techs):
    return {"n": n, "df": dict(techs), "source": {}, "cooccur": {}, "raw_skills": {}}


def test_snapshot_models_survive_reopen_and_reanalysis(tmp_path):
    db_path = tmp_path / "models.db"
    conn = storage.connect(str(db_path))
    storage.init_schema(conn)
    analyzed = _result(1, {"Python": 1})
    analyzed["models_used"] = ["Gemini API: gemini-3.6-flash", "OpenRouter: selected/model"]
    run_id = storage.write_snapshot(
        conn, {"query_text": "python", "area": "113", "method": "llm"}, analyzed, [])
    conn.close()

    conn = storage.connect(str(db_path))
    storage.init_schema(conn)
    snapshot = storage.latest_ok_snapshot(conn, "python", "113")
    assert snapshot["models_used_json"] == ('["Gemini API: gemini-3.6-flash", '
                                            '"OpenRouter: selected/model"]')
    storage.replace_analysis(conn, run_id, _result(1, {"Python": 1}), "basic")
    snapshot = storage.latest_ok_snapshot(conn, "python", "113")
    assert snapshot["models_used_json"] == "[]"
    conn.close()


def test_model_column_migrates_existing_snapshot(tmp_path):
    db_path = tmp_path / "old.db"
    conn = storage.connect(str(db_path))
    storage.init_schema(conn)
    conn.execute("ALTER TABLE snapshots DROP COLUMN models_used_json")
    conn.commit()
    storage.init_schema(conn)
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(snapshots)")}
    assert "models_used_json" in columns
    conn.close()


def _vac(vac_id, published_at, description="desc", **over):
    row = {"vac_id": vac_id, "name": f"Вакансия {vac_id}", "company": "ООО Тест",
           "published_at": published_at, "area": "Москва",
           "alternate_url": f"https://hh.ru/vacancy/{vac_id}", "key_skills_json": None,
           "description": description, "snippet_req": None, "snippet_resp": None}
    row.update(over)
    return row


def _snapshot(conn, query, rows, area="113"):
    return storage.write_snapshot(
        conn, {"query_text": query, "area": area, "total_found": len(rows)},
        _result(len(rows), {"python": len(rows)}), rows)


def test_details_stored_once_across_snapshots(conn):
    rows = [_vac("1", "2026-01-01T00:00:00+0300")]
    _snapshot(conn, "python", rows)
    _snapshot(conn, "python", rows)
    assert conn.execute("SELECT COUNT(*) c FROM vacancy_details").fetchone()["c"] == 1
    assert conn.execute("SELECT COUNT(*) c FROM snapshot_items").fetchone()["c"] == 2


def test_empty_description_does_not_erase_stored_one(conn):
    _snapshot(conn, "python", [_vac("1", "2026-01-01T00:00:00+0300", "полное описание")])
    _snapshot(conn, "python", [_vac("1", "2026-01-02T00:00:00+0300", None)])
    assert storage.get_vacancy(conn, "1")["description"] == "полное описание"


def test_snapshot_keeps_its_own_published_at(conn):
    """Перепубликация вакансии не должна переписывать порядок старого снимка."""
    run1 = _snapshot(conn, "python", [_vac("1", "2026-01-01T00:00:00+0300"),
                                      _vac("2", "2026-01-02T00:00:00+0300")])
    _snapshot(conn, "python", [_vac("1", "2026-06-01T00:00:00+0300"),
                               _vac("2", "2026-01-02T00:00:00+0300")])
    first = storage.snapshot_vacancies(conn, run1, 10)
    assert [r["vac_id"] for r in first] == ["2", "1"]


def test_stored_details_skips_missing_description(conn):
    _snapshot(conn, "python", [_vac("1", "2026-01-01T00:00:00+0300", "есть"),
                               _vac("2", "2026-01-01T00:00:00+0300", None)])
    assert set(storage.stored_details(conn, ["1", "2"])) == {"1"}


def test_prune_drops_adhoc_but_keeps_tracked(conn):
    old = "2000-01-01T00:00:00+00:00"
    tracked_run = _snapshot(conn, "python", [_vac("1", old)])
    adhoc_run = _snapshot(conn, "rust", [_vac("2", old)])
    conn.execute("UPDATE snapshots SET ts_utc=?", (old,))
    conn.commit()
    jobs.track(conn, "python", "113", 1)
    storage.prune_raw(conn, tracked_days=36500, adhoc_days=1)
    assert len(storage.raw_vacancies(conn, tracked_run)) == 1
    assert storage.raw_vacancies(conn, adhoc_run) == []
    assert storage.get_vacancy(conn, "2") is None  # осиротевшая деталь удалена


def test_migration_from_legacy_vacancies_table(tmp_path):
    """Старая БД со схемой v1 переносится без потери данных."""
    path = str(tmp_path / "legacy.db")
    raw = sqlite3.connect(path)
    raw.executescript("""
        CREATE TABLE snapshots (run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT, ts_msk TEXT, query_text TEXT, area TEXT, per_page INTEGER,
            max_items INTEGER, total_found INTEGER, vacancy_count INTEGER,
            enriched_count INTEGER, status TEXT, error TEXT, duration_sec REAL);
        CREATE TABLE vacancies (run_id INTEGER, vac_id TEXT, name TEXT, company TEXT,
            published_at TEXT, area TEXT, alternate_url TEXT, key_skills_json TEXT,
            description TEXT, snippet_req TEXT, snippet_resp TEXT,
            PRIMARY KEY (run_id, vac_id));
        INSERT INTO snapshots(run_id, ts_utc, query_text, area, status)
            VALUES (1,'2026-01-01T00:00:00+00:00','python','113','ok'),
                   (2,'2026-02-01T00:00:00+00:00','python','113','ok');
        -- в старом снимке company пуста (колонку добавили поздним ALTER'ом),
        -- в новом — заполнена, но нет описания
        INSERT INTO vacancies VALUES
            (1,'10','Разработчик',NULL,'2026-01-01',NULL,NULL,NULL,'описание',NULL,NULL),
            (2,'10','Разработчик','ООО Тест','2026-01-05',NULL,'https://hh.ru/vacancy/10',
             NULL,NULL,NULL,NULL);
    """)
    raw.commit()
    raw.close()

    conn = storage.connect(path)
    storage.init_schema(conn)
    detail = storage.get_vacancy(conn, "10")
    assert detail["description"] == "описание"     # взято из старого снимка
    assert detail["company"] == "ООО Тест"          # взято из нового
    assert [r["published_at"] for r in storage.snapshot_vacancies(conn, 1, 10)] == ["2026-01-01"]
    assert [r["published_at"] for r in storage.snapshot_vacancies(conn, 2, 10)] == ["2026-01-05"]
    assert conn.execute(
        "SELECT COUNT(*) c FROM sqlite_master WHERE name='vacancies_old'").fetchone()["c"] == 1
