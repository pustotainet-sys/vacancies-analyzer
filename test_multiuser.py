"""Тесты многопользовательской обвязки: очередь, квоты, инвайты.

Логика конкурентная (несколько воркеров и общий счётчик), поэтому проверяем
именно её, а не рендеринг сообщений.
"""

import asyncio
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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
def test_list_users_includes_saved_filter_count(conn):
    storage.register_user(conn, 1, username="alice")
    storage.register_user(conn, 2, username="bob")
    storage.save_filter(conn, 1, "python", "all")
    storage.save_filter(conn, 1, "python", "113")
    storage.save_filter(conn, 1, "python", "all")

    users = {user["chat_id"]: user for user in storage.list_users(conn)}

    assert users[1]["saved_filter_count"] == 2, "у первого пользователя должно быть два фильтра"
    assert users[2]["saved_filter_count"] == 0, "у второго пользователя фильтров нет"


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


def test_ban_removes_subscriptions_and_pending_job_messages(conn):
    # Arrange
    storage.register_user(conn, 1)
    storage.register_user(conn, 2)
    storage.save_filter(conn, 1, "python", "all")
    jobs.track(conn, "python", "all", 1)
    jobs.track(conn, "python", "all", 2)
    job_id, _, _ = jobs.enqueue(conn, "collect", "python", "all", 200, None, 1)
    jobs.enqueue(conn, "collect", "python", "all", 200, None, 2)
    # Act
    banned = storage.set_user_status(conn, 1, "banned")
    # Assert
    assert banned, "пользователь не заблокирован"
    assert jobs.user_tracked(conn, 1) == [], "подписка заблокированного осталась"
    assert jobs.user_tracked(conn, 2) == [("python", "all")], (
        "подписка другого пользователя затронута")
    assert jobs.watchers(conn, job_id) == [2], "заблокированный ждёт результат задачи"
    assert storage.user_saved_filters(conn, 1) == [("python", "all")], (
        "личные фильтры должны сохраниться для возможной разблокировки")
    storage.set_user_status(conn, 1, "active")
    assert jobs.user_tracked(conn, 1) == [], "разблокировка автоматически вернула подписку"


def test_banned_user_cannot_redeem_another_invite(conn):
    # Arrange
    storage.register_user(conn, 1)
    storage.set_user_status(conn, 1, "banned")
    storage.create_invite(conn, "NEW", 2)
    # Act
    verdict = storage.redeem_invite(conn, "NEW", 1)
    # Assert
    assert verdict == "banned", f"заблокированный восстановил доступ: {verdict}"
    assert storage.get_user(conn, 1)["status"] == "banned", "статус блокировки изменён"
    invite = conn.execute("SELECT used_count FROM invites WHERE code='NEW'").fetchone()
    assert invite["used_count"] == 0, "инвайт потрачен на заблокированного"


def test_ban_suppresses_messages_already_queued_for_delivery(conn):
    # Arrange
    import bot
    sent = []

    async def fake_send_message(chat_id, message, reply_markup=None):
        sent.append(chat_id)

    storage.register_user(conn, 1)
    storage.register_user(conn, 2)
    stale_recipients = [1, 2]
    storage.set_user_status(conn, 1, "banned")
    # Act
    asyncio.run(bot._notify(SimpleNamespace(send_message=fake_send_message), conn,
                            stale_recipients, "progress"))
    # Assert
    assert sent == [2], f"сообщение ушло заблокированному: {sent}"


def test_startup_removes_legacy_banned_subscriptions(conn):
    # Arrange
    storage.register_user(conn, 1)
    storage.set_user_status(conn, 1, "banned")
    jobs.track(conn, "python", "all", 1)
    job_id, _, _ = jobs.enqueue(conn, "collect", "python", "all", 200, None, 1)
    # Act
    storage.init_schema(conn)
    # Assert
    assert jobs.user_tracked(conn, 1) == [], "старое отслеживание пережило перезапуск"
    assert jobs.watchers(conn, job_id) == [], "старое ожидание сообщения пережило перезапуск"


# --- подписки ----------------------------------------------------------------
def test_track_is_idempotent_and_listed(conn):
    jobs.track(conn, "golang", "113", 1)
    jobs.track(conn, "golang", "113", 1)
    assert jobs.user_tracked(conn, 1) == [("golang", "113")]
    assert jobs.tracked_count(conn) == 1


def test_subscribers_exclude_system_and_other_queries(conn):
    # Arrange
    jobs.track(conn, "python", "113", 0)
    jobs.track(conn, "python", "113", 1)
    jobs.track(conn, "python", "113", 2)
    jobs.track(conn, "python", "1", 3)
    # Act
    subscribers = jobs.subscribers(conn, "python", "113")
    # Assert
    assert sorted(subscribers) == [1, 2], f"ожидались чаты 1 и 2, получены {subscribers}"


def test_saved_filters_are_buttons_with_selected_pair(conn):
    # Arrange
    import bot
    storage.save_filter(conn, 1, "python", "113")
    storage.save_filter(conn, 1, "golang", "1")
    jobs.track(conn, "golang", "1", 1)
    state = {"query_text": "golang", "area": "1", "period_days": 30, "top_n": 15}
    # Act
    keyboard = bot.filter_kb(state, True, storage.user_saved_filters(conn, 1),
                             jobs.user_tracked(conn, 1))
    saved = [button for row in keyboard.inline_keyboard for button in row
             if button.callback_data.startswith("saved:")]
    # Assert
    assert len(saved) == 2, f"ожидались две кнопки, получено {len(saved)}"
    assert saved[1].text.startswith("• 📌 golang /"), (
        f"активная пара не отмечена: {saved[1].text}")
    assert saved[1].callback_data.endswith(bot._saved_filter_token("golang", "1")), (
        f"неверная привязка кнопки: {saved[1].callback_data}")
    query_buttons = keyboard.inline_keyboard[3]
    assert query_buttons[0].text == "✏️ golang", query_buttons
    assert query_buttons[0].callback_data == "edit_filter", query_buttons
    assert any(button.callback_data == "text:custom"
               for row in keyboard.inline_keyboard for button in row), (
        "после выбора существующего фильтра пропала возможность создать новый")


def test_new_user_has_no_default_filter():
    # Arrange
    import bot
    state = dict(bot.STATE_DEFAULTS)
    # Act
    keyboard = bot.filter_kb(state)
    # Assert
    buttons = [button for row in keyboard.inline_keyboard for button in row]
    assert state["query_text"] == "", state
    assert not any("python" in button.text.lower() for button in buttons), buttons
    assert not any(button.callback_data in ("track", "delete_filter") for button in buttons), buttons
    assert any(button.callback_data == "text:custom" for button in buttons), buttons


def test_old_unsaved_default_is_cleared_on_startup(conn):
    # Arrange
    import bot
    storage.register_user(conn, 1)
    storage.register_user(conn, 2)
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.get_chat_state(conn, 2, bot.STATE_DEFAULTS)
    storage.set_chat_state(conn, 1, query_text="python")
    storage.set_chat_state(conn, 2, query_text="python")
    storage.save_filter(conn, 2, "python", "113")
    # Act
    storage.init_schema(conn)
    # Assert
    assert storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)["query_text"] == "", (
        "несохранённый начальный запрос должен исчезнуть")
    assert storage.get_chat_state(conn, 2, bot.STATE_DEFAULTS)["query_text"] == "python", (
        "сохранённый фильтр пользователя нельзя удалять")


def test_change_region_replaces_saved_filter_and_preserves_tracking(conn, monkeypatch):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.set_chat_state(conn, 1, query_text="ai ии", area="113")
    storage.save_filter(conn, 1, "ai ии", "113")
    jobs.track(conn, "ai ии", "113", 1)
    monkeypatch.setattr(bot, "_wake_workers", lambda context: None)
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    status = bot._change_selected_area(context, 1, "all")
    # Assert
    assert status == "renamed", f"регион не изменён: {status}"
    assert storage.user_saved_filters(conn, 1) == [("ai ии", "all")], (
        "смена региона создала дубликат")
    assert jobs.user_tracked(conn, 1) == [("ai ии", "all")], (
        "отслеживание осталось на старом регионе")
    queued = conn.execute("SELECT query_text,area FROM jobs WHERE status='queued'").fetchall()
    assert [tuple(row) for row in queued] == [("ai ии", "all")], queued


def test_select_python_uses_existing_saved_pair(conn):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.save_filter(conn, 1, "python", "113")
    storage.save_filter(conn, 1, "ai ии", "all")
    storage.set_chat_state(conn, 1, query_text="ai ии", area="all")
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    bot._select_python_filter(context, 1)
    state = storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    # Assert
    assert (state["query_text"], state["area"]) == ("python", "113"), state
    assert storage.user_saved_filters(conn, 1) == [("python", "113"), ("ai ии", "all")], (
        "выбор python создал лишний фильтр")


def test_delete_selects_saved_python_with_its_region(conn):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.save_filter(conn, 1, "python", "all")
    storage.save_filter(conn, 1, "агент", "113")
    storage.set_chat_state(conn, 1, query_text="агент", area="113")
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    storage.delete_filter(conn, 1, "агент", "113")
    bot._select_remaining_filter(context, 1)
    state = storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    # Assert
    assert (state["query_text"], state["area"]) == ("python", "all"), state


def test_delete_uses_default_only_when_no_saved_filters_remain(conn):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.save_filter(conn, 1, "агент", "all")
    storage.set_chat_state(conn, 1, query_text="агент", area="all")
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    storage.delete_filter(conn, 1, "агент", "all")
    bot._select_remaining_filter(context, 1)
    state = storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    # Assert
    assert (state["query_text"], state["area"]) == ("", bot.DEFAULT_AREA), state


def test_python_filter_can_move_to_world(conn, monkeypatch):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.save_filter(conn, 1, "python", "113")
    storage.set_chat_state(conn, 1, query_text="python")
    jobs.track(conn, "python", "113", 0)
    jobs.track(conn, "python", "113", 1)
    monkeypatch.setattr(bot, "_wake_workers", lambda context: None)
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    status = bot._change_selected_area(context, 1, "all")
    # Assert
    assert status == "renamed", f"python не переключился на весь мир: {status}"
    assert storage.user_saved_filters(conn, 1) == [("python", "all")], (
        "остался лишний фильтр python / Россия")
    assert jobs.user_tracked(conn, 1) == [("python", "all")], (
        "пользовательское отслеживание не перенеслось")
    assert jobs.subscribers(conn, "python", "113") == [], (
        "системный фильтр не должен учитываться как подписчик")


def test_region_button_selects_existing_pair_instead_of_merging(conn):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.save_filter(conn, 1, "ai ии", "113")
    storage.save_filter(conn, 1, "ai ии", "all")
    storage.set_chat_state(conn, 1, query_text="ai ии", area="113")
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    status = bot._change_selected_area(context, 1, "all")
    state = storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    # Assert
    assert status == "selected", f"существующая пара не выбрана: {status}"
    assert state["area"] == "all", state
    assert storage.user_saved_filters(conn, 1) == [("ai ии", "113"), ("ai ии", "all")], (
        "существующие разные пары должны сохраниться")


def test_filter_region_buttons_only_offer_world_and_russia():
    # Arrange
    import bot
    state = {"query_text": "python", "area": "all", "period_days": 30, "top_n": 15}
    # Act
    keyboard = bot.filter_kb(state)
    regions = keyboard.inline_keyboard[0]
    edit_regions = bot._edit_area_kb().inline_keyboard[0]
    # Assert
    assert [(button.text, button.callback_data) for button in regions] == [
        ("• Весь мир", "area:all"), ("Россия", "area:113")], regions
    assert [button.callback_data for button in edit_regions] == ["move:all", "move:113"], (
        edit_regions)


@pytest.mark.parametrize(("area", "expected_area"), [
    pytest.param("all", None, id="world-omits-area"),
    pytest.param("113", "113", id="russia-sets-area"),
])
def test_hh_search_region_parameters(monkeypatch, area, expected_area):
    # Arrange
    import hh_vacancies
    searched = []

    def fake_fetch_page(session, params, page):
        searched.append(dict(params))
        return {"found": 0, "pages": 0, "items": []}

    monkeypatch.setattr(hh_vacancies, "fetch_page", fake_fetch_page)
    # Act
    hh_vacancies.fetch_vacancies(None, "python", area, 100, 200)
    # Assert
    actual_area = searched[0].get("area")
    assert actual_area == expected_area, (
        f"для {area} ожидался area={expected_area}, получено {actual_area}")


def test_rename_filter_keeps_tracking_and_changes_selected_pair(conn):
    # Arrange
    import bot
    storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    storage.set_chat_state(conn, 1, query_text="ai ии", area="113")
    storage.save_filter(conn, 1, "ai ии", "113")
    jobs.track(conn, "ai ии", "113", 1)
    # Act
    status = storage.rename_filter(conn, 1, "ai ии", "113", "ai ml", "1")
    # Assert
    assert status == "renamed", f"фильтр не исправлен: {status}"
    assert storage.user_saved_filters(conn, 1) == [("ai ml", "1")], (
        "список сохранённых фильтров не обновился")
    assert jobs.user_tracked(conn, 1) == [("ai ml", "1")], (
        "отслеживание не перенесено")
    state = storage.get_chat_state(conn, 1, bot.STATE_DEFAULTS)
    assert (state["query_text"], state["area"]) == ("ai ml", "1"), state


def test_rename_filter_rejects_existing_target_without_changes(conn):
    # Arrange
    storage.save_filter(conn, 1, "ai ии", "113")
    storage.save_filter(conn, 1, "python", "113")
    jobs.track(conn, "ai ии", "113", 1)
    # Act
    status = storage.rename_filter(conn, 1, "ai ии", "113", "python", "113")
    # Assert
    assert status == "exists", f"дубликат был принят: {status}"
    assert jobs.user_tracked(conn, 1) == [("ai ии", "113")], (
        "отслеживание изменилось при отказе")


def test_saved_filter_remains_after_untrack(conn):
    # Arrange
    storage.save_filter(conn, 1, "golang", "1")
    jobs.track(conn, "golang", "1", 1)
    # Act
    jobs.untrack(conn, "golang", "1", 1)
    # Assert
    assert storage.user_saved_filters(conn, 1) == [("golang", "1")], (
        "снятие отслеживания удалило сохранённый фильтр")


def test_stale_saved_filter_button_cannot_select_other_pair():
    # Arrange
    import bot
    before = [("python", "113"), ("golang", "1")]
    callback_data = f"saved:1:{bot._saved_filter_token('golang', '1')}"
    # Act
    selected = bot._resolve_saved_filter(before, callback_data)
    stale = bot._resolve_saved_filter([("python", "113"), ("rust", "1")], callback_data)
    # Assert
    assert selected == ("golang", "1"), f"не выбрана нужная пара: {selected}"
    assert stale is None, f"устаревшая кнопка выбрала чужую пару: {stale}"


def test_existing_tracked_filters_migrate_to_saved(tmp_path):
    # Arrange
    db_path = tmp_path / "existing.db"
    conn = storage.connect(str(db_path))
    storage.init_schema(conn)
    jobs.track(conn, "golang", "1", 42)
    conn.execute("DELETE FROM meta WHERE key='saved_filters_seeded'")
    conn.commit()
    # Act
    storage.init_schema(conn)
    saved = storage.user_saved_filters(conn, 42)
    # Assert
    assert saved == [("golang", "1")], f"старое отслеживание не перенесено: {saved}"
    conn.close()


def test_scheduled_filter_uses_llm_mode_and_skips_fresh_snapshot(conn, monkeypatch):
    # Arrange
    import bot
    jobs.track(conn, bot.DEFAULT_QUERY, bot.DEFAULT_AREA, 0)
    jobs.track(conn, "golang", "1", 1)
    jobs.track(conn, "rust", "2", 1)
    storage.write_snapshot(conn, {"query_text": "rust", "area": "2", "method": "llm"},
                           _result(1, {"Rust": 1}), [])
    monkeypatch.setattr(bot, "_wake_workers", lambda context: None)
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    asyncio.run(bot.scheduled_tracked(context))
    queued = conn.execute("SELECT query_text, area, top_n, mode FROM jobs").fetchall()
    # Assert
    assert [tuple(row) for row in queued] == [("golang", "1", bot.ANALYZE_TOP_N, None)], (
        f"неверные фоновые задачи: {[tuple(row) for row in queued]}")


def test_failed_tracked_collect_waits_for_next_interval(conn, monkeypatch):
    # Arrange
    import bot
    jobs.track(conn, "golang", "1", 1)
    job_id, _, _ = jobs.enqueue(conn, "collect", "golang", "1", 200, None)
    jobs.finish(conn, job_id, "error", error="temporary")
    monkeypatch.setattr(bot, "_wake_workers", lambda context: None)
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    # Act
    asyncio.run(bot.scheduled_tracked(context))
    jobs_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    # Assert
    assert jobs_count == 1, f"после недавней ошибки созданы повторные задачи: {jobs_count}"


def test_top_change_tracks_rank_and_frequency(conn):
    # Arrange
    import analysis
    first = _result(2, {"Python": 2, "Docker": 1})
    first_id = storage.write_snapshot(
        conn, {"query_text": "python", "area": "113", "method": "llm"}, first, [])
    baseline = analysis.top_change(conn, "python", "113", first_id, 2)
    second = _result(2, {"Docker": 2, "Kubernetes": 1})
    second_id = storage.write_snapshot(
        conn, {"query_text": "python", "area": "113", "method": "llm"}, second, [])
    # Act
    changes = analysis.top_change(conn, "python", "113", second_id, 2)
    # Assert
    assert baseline is None, f"первый снимок должен быть базой, получено {baseline}"
    assert changes["entered"] == [("Kubernetes", 2, 1)], changes
    assert changes["exited"] == [("Python", 1, 2)], changes
    assert changes["changed"] == [("Docker", 2, 1, 1, 2)], changes


def test_unchanged_top_has_no_notification(conn):
    # Arrange
    import analysis
    import bot
    first = _result(2, {"Python": 2})
    storage.write_snapshot(conn, {"query_text": "python", "area": "113"}, first, [])
    second_id = storage.write_snapshot(conn, {"query_text": "python", "area": "113"},
                                       first, [])
    # Act
    changes = analysis.top_change(conn, "python", "113", second_id)
    message = bot._top_change_text("python", "113", changes)
    # Assert
    assert message is None, f"без изменений уведомление не нужно: {message}"


def test_top_change_notifies_only_subscribers_after_second_collect(conn):
    # Arrange
    import bot
    sent = []

    async def fake_send_message(chat_id, message, reply_markup=None):
        sent.append((chat_id, message))

    for chat_id in (1, 2, 3):
        storage.register_user(conn, chat_id)
    jobs.track(conn, "python", "113", 1)
    jobs.track(conn, "python", "113", 2)
    jobs.track(conn, "rust", "113", 3)
    app = SimpleNamespace(bot_data={"conn": conn},
                          bot=SimpleNamespace(send_message=fake_send_message))
    job = {"kind": "collect", "query_text": "python", "area": "113"}
    first_id = storage.write_snapshot(conn, {"query_text": "python", "area": "113",
                                             "method": "llm"},
                                      _result(1, {"Python": 1}), [])
    asyncio.run(bot._notify_top_change(app, job, {"status": "ok", "run_id": first_id,
                                                  "method": "llm"}))
    second_id = storage.write_snapshot(conn, {"query_text": "python", "area": "113",
                                              "method": "llm"},
                                       _result(1, {"Docker": 1}), [])
    # Act
    asyncio.run(bot._notify_top_change(app, job, {"status": "ok", "run_id": second_id,
                                                  "method": "llm"}))
    # Assert
    assert [chat_id for chat_id, _ in sent] == [1, 2], f"неверные получатели: {sent}"
    assert all("Docker" in text and "Python" in text for _, text in sent), sent


def test_basic_fallback_does_not_send_llm_top_alert(conn):
    # Arrange
    import bot
    sent = []

    async def fake_send_message(chat_id, message, reply_markup=None):
        sent.append((chat_id, message))

    jobs.track(conn, "python", "113", 1)
    app = SimpleNamespace(bot_data={"conn": conn},
                          bot=SimpleNamespace(send_message=fake_send_message))
    job = {"kind": "collect", "query_text": "python", "area": "113"}
    storage.write_snapshot(conn, {"query_text": "python", "area": "113", "method": "llm"},
                           _result(1, {"Python": 1}), [])
    fallback_id = storage.write_snapshot(
        conn, {"query_text": "python", "area": "113", "method": "basic (откат с LLM)"},
        _result(1, {"Docker": 1}), [])
    # Act
    asyncio.run(bot._notify_top_change(app, job, {"status": "ok", "run_id": fallback_id,
                                                  "method": "basic (откат с LLM)"}))
    # Assert
    assert not sent, f"разные методики анализа нельзя сравнивать: {sent}"


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


def test_tracked_raw_retention_is_30_days(conn):
    # Arrange
    import collector
    recent = (datetime.now(timezone.utc) - timedelta(days=29)).isoformat()
    expired = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
    old_id = _snapshot(conn, "python", [_vac("old", expired)])
    new_id = _snapshot(conn, "python", [_vac("new", recent)])
    conn.execute("UPDATE snapshots SET ts_utc=? WHERE run_id=?", (expired, old_id))
    conn.execute("UPDATE snapshots SET ts_utc=? WHERE run_id=?", (recent, new_id))
    conn.commit()
    jobs.track(conn, "python", "113", 1)
    # Act
    collector.prune_expired(conn)
    # Assert
    assert storage.raw_vacancies(conn, old_id) == [], "сырьё старше 30 дней осталось"
    assert len(storage.raw_vacancies(conn, new_id)) == 1, "свежее сырьё удалено"
    assert storage.get_vacancy(conn, "old") is None, "старое описание осталось"
    assert storage.latest_ok_snapshot(conn, "python", "113") is not None, (
        "снимки до 180 дней должны сохраняться")


def test_history_retention_removes_old_snapshot_relations_and_jobs(conn):
    # Arrange
    old_time = (datetime.now(timezone.utc) - timedelta(days=181)).isoformat()
    current_day = datetime.now(timezone.utc).date().isoformat()
    old_day = (datetime.now(timezone.utc) - timedelta(days=181)).date().isoformat()
    old_result = _result(1, {"Python": 1, "Docker": 1})
    old_result["cooccur"] = {("Python", "Docker"): 1}
    old_id = storage.write_snapshot(
        conn, {"query_text": "old", "area": "113", "method": "llm"}, old_result,
        [_vac("shared", old_time), _vac("old_only", old_time)])
    new_id = _snapshot(conn, "new", [_vac("shared", current_day)])
    conn.execute("UPDATE snapshots SET ts_utc=? WHERE run_id=?", (old_time, old_id))
    old_job, _, _ = jobs.enqueue(conn, "collect", "old", "113", 200, None, 1)
    jobs.finish(conn, old_job, "done", run_id=old_id)
    conn.execute("UPDATE jobs SET finished_at=? WHERE job_id=?", (old_time, old_job))
    active_job, _, _ = jobs.enqueue(conn, "collect", "new", "113", 200, None, 1)
    conn.execute("INSERT INTO daily_usage(day,llm_jobs) VALUES(?,1)", (old_day,))
    conn.execute("INSERT INTO daily_usage(day,llm_jobs) VALUES(?,1)", (current_day,))
    storage.save_filter(conn, 1, "old", "113")
    conn.commit()
    # Act
    storage.prune_history(conn, 180)
    # Assert
    assert conn.execute("SELECT 1 FROM snapshots WHERE run_id=?", (old_id,)).fetchone() is None
    assert conn.execute("SELECT 1 FROM snapshots WHERE run_id=?", (new_id,)).fetchone(), (
        "новый снимок удалён")
    for table in ("tech_counts", "cooccurrence", "snapshot_items"):
        count = conn.execute(f"SELECT count(*) FROM {table} WHERE run_id=?", (old_id,)).fetchone()[0]
        assert count == 0, f"связанные данные остались в {table}"
    assert storage.get_vacancy(conn, "old_only") is None, "осиротевшая вакансия осталась"
    assert storage.get_vacancy(conn, "shared") is not None, "общая вакансия удалена"
    assert conn.execute("SELECT 1 FROM jobs WHERE job_id=?", (old_job,)).fetchone() is None
    assert conn.execute("SELECT 1 FROM job_watchers WHERE job_id=?", (old_job,)).fetchone() is None
    assert conn.execute("SELECT 1 FROM jobs WHERE job_id=?", (active_job,)).fetchone()
    days = [row["day"] for row in conn.execute("SELECT day FROM daily_usage")]
    assert days == [current_day], f"неверные суточные счётчики: {days}"
    assert storage.user_saved_filters(conn, 1) == [("old", "113")], (
        "настройки пользователя не должны удаляться")


def test_daily_maintenance_prunes_history_without_new_collect(conn, monkeypatch):
    # Arrange
    import bot
    old_time = (datetime.now(timezone.utc) - timedelta(days=181)).isoformat()
    old_id = _snapshot(conn, "old", [_vac("old_vac", old_time)])
    conn.execute("UPDATE snapshots SET ts_utc=? WHERE run_id=?", (old_time, old_id))
    conn.commit()
    context = SimpleNamespace(application=SimpleNamespace(bot_data={"conn": conn}))
    monkeypatch.setenv("RETENTION_HISTORY_DAYS", "180")
    # Act
    asyncio.run(bot.scheduled_vacuum(context))
    # Assert
    assert storage.last_snapshot(conn, "old", "113") is None, (
        "ежедневная очистка не удалила старый снимок")
    assert storage.get_vacancy(conn, "old_vac") is None, (
        "ежедневная очистка не удалила осиротевшую вакансию")


def test_backup_retention_only_removes_expired_deployment_copies(tmp_path):
    # Arrange
    db_path = tmp_path / "hh_stats.db"
    old_backup = tmp_path / "hh_stats.db.pre-old"
    recent_backup = tmp_path / "hh_stats.db.pre-recent"
    unrelated = tmp_path / "notes.txt"
    for path in (old_backup, recent_backup, unrelated):
        path.write_text("test")
    old_timestamp = (datetime.now(timezone.utc) - timedelta(days=181)).timestamp()
    os.utime(old_backup, (old_timestamp, old_timestamp))
    os.utime(unrelated, (old_timestamp, old_timestamp))
    # Act
    storage.prune_backups(str(db_path), 180)
    # Assert
    assert not old_backup.exists(), "старая копия БД не удалена"
    assert recent_backup.exists(), "свежая копия БД удалена"
    assert unrelated.exists(), "удалён посторонний файл"


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
