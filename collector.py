"""Один прогон сбора: N свежих вакансий -> детали -> извлечение -> запись.

Деталь /vacancies/{id} — основной сигнал (description + key_skills). Объём
ограничен analyze_top_n. Guard не даёт прогонам пересекаться (ручной + фоновый).
"""

import json
import os
import threading
import time
from datetime import datetime, timezone

import requests

import extract
import llm
import skills_dict
import storage
from hh_vacancies import API_URL, build_session, fetch_vacancies

_guard = threading.Lock()          # неблокирующий захват -> отсечь параллельный прогон
_busy = threading.Event()


def fetch_vacancy_detail(session, vac_id, timeout=30):
    resp = session.get(f"{API_URL}/{vac_id}", timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _known_aliases(skill_map):
    known = {a.lower() for aliases in skill_map.values() for a in aliases}
    known |= {c.lower() for c in skill_map}
    return known


def _raw_row(v):
    ks = v.get("key_skills")
    sn = v.get("snippet") or {}
    vac_id = str(v.get("id"))
    return {
        "vac_id": vac_id,
        "name": v.get("name"),
        "company": (v.get("employer") or {}).get("name"),
        "published_at": v.get("published_at"),
        "area": (v.get("area") or {}).get("name"),
        "alternate_url": v.get("alternate_url") or f"https://hh.ru/vacancy/{vac_id}",
        "key_skills_json": json.dumps(ks, ensure_ascii=False) if ks else None,
        "description": extract.strip_html(v.get("description")) or None,
        "snippet_req": extract.strip_html(sn.get("requirement")) or None,
        "snippet_resp": extract.strip_html(sn.get("responsibility")) or None,
    }


def _gazetteer(conn, items):
    learned = storage.learned_map(conn)
    skill_map = skills_dict.merged_with_learned(learned)
    matcher = extract.build_matcher(skill_map)
    alias_map = skills_dict.alias_to_canonical(skill_map)
    return extract.aggregate(items, matcher, alias_map)


def _analyze(conn, items, mode, progress):
    """Выбор режима извлечения. Возвращает (result, method).

    mode: 'llm' | 'basic' | 'auto'. LLM — основной; при сбое или отсутствии ключа —
    откат на базовый (gazetteer).
    """
    mode = (mode or os.environ.get("LLM_MODE", "auto")).lower()
    want_llm = mode == "llm" or (mode == "auto" and llm.available())
    if want_llm:
        if not llm.available():
            if progress:
                progress("OPENROUTER_API_KEY не задан — использую базовый режим…")
            return _gazetteer(conn, items), "basic (нет ключа LLM)"
        try:
            return llm.aggregate_llm(items, progress=progress), "llm"
        except Exception as e:
            if progress:
                progress(f"LLM недоступен ({type(e).__name__}) — базовый режим…")
            print(f"[collect] LLM ошибка: {e}", flush=True)
            return _gazetteer(conn, items), "basic (откат с LLM)"
    return _gazetteer(conn, items), "basic"


def collect_once(conn, query, area, analyze_top_n, throttle=0.2, progress=None, mode=None):
    """Собрать и записать один снимок. Возвращает dict со статусом.

    progress(msg) — необязательный колбэк, вызывается только когда реально начинается
    догрузка новых вакансий (не при работе из кэша). Итог/ошибку сообщает вызывающий
    по возвращённому dict. Не пересекается с другим прогоном (status='busy').
    """
    if not _guard.acquire(blocking=False):
        return {"status": "busy"}
    _busy.set()
    t0 = time.monotonic()
    session = None
    try:
        session = build_session()
        # search_field пуст -> hh ищет по всем полям (как на сайте); можно сузить
        # через HH_SEARCH_FIELD=name (только по названию).
        search_field = os.environ.get("HH_SEARCH_FIELD") or None
        items, total_found = fetch_vacancies(
            session, query, area, per_page=100, max_items=analyze_top_n,
            extra_params={"order_by": "publication_time"}, search_field=search_field)

        total = len(items)
        # детали уже виденных вакансий берём из БД — по сети тянем только новые
        cached = storage.stored_details(conn, [str(v["id"]) for v in items])
        new_items = [v for v in items if str(v["id"]) not in cached]
        print(f"[collect] вакансий: {total}; из БД: {len(cached)}; новых к загрузке: "
              f"{len(new_items)} (~{len(new_items) * (throttle + 0.15):.0f}с)…", flush=True)
        # сообщаем пользователю только если реально идёт догрузка новых вакансий
        if new_items and progress:
            progress(f"Догружаю {len(new_items)} новых вакансий…")

        enriched, reused, done = 0, 0, 0
        for v in items:
            vid = str(v["id"])
            if vid in cached:
                c = cached[vid]
                v["description"] = c["description"]
                v["key_skills"] = json.loads(c["key_skills_json"]) if c["key_skills_json"] else None
                reused += 1
                enriched += 1
                continue
            try:
                detail = fetch_vacancy_detail(session, vid)
                v["description"] = detail.get("description")
                v["key_skills"] = detail.get("key_skills")
                enriched += 1
                if throttle:
                    time.sleep(throttle)
            except requests.RequestException:
                pass  # деталь не загрузилась — останется сниппет как fallback
            done += 1
            if new_items and (done % 25 == 0 or done == len(new_items)):
                print(f"[collect]   новых деталей {done}/{len(new_items)}", flush=True)

        result, method = _analyze(conn, items, mode, progress)

        meta = {
            "query_text": query, "area": area, "per_page": 100,
            "max_items": analyze_top_n, "total_found": total_found, "method": method,
            "enriched_count": enriched, "duration_sec": round(time.monotonic() - t0, 1),
        }
        raw_rows = [_raw_row(v) for v in items]
        run_id = storage.write_snapshot(conn, meta, result, raw_rows)
        skill_map = skills_dict.merged_with_learned(storage.learned_map(conn))
        storage.upsert_learned(conn, result["raw_skills"], _known_aliases(skill_map))
        storage.prune_raw(conn, _retention_days())

        top = sorted(result["df"].items(), key=lambda kv: -kv[1])[:10]
        return {"status": "ok", "run_id": run_id, "n": result["n"], "method": method,
                "enriched": enriched, "reused": reused, "new": len(new_items),
                "top": [t for t, _ in top[:5]]}

    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        body = e.response.text[:200] if e.response is not None else ""
        has_token = bool(session and "Authorization" in session.headers)
        if code == 403 and not has_token:
            err = ("hh 403 без токена: не удалось получить/обновить токен приложения "
                   "(возможно 'refresh too early') И запрос с облачного IP. С домашнего IP "
                   "поиск обычно работает без токена; проверьте HH_CLIENT_ID/SECRET, если нужен токен")
        elif code == 403:
            err = (f"hh 403 несмотря на токен — вероятно лимит запросов или блок IP. "
                   f"Ответ hh: {body}")
        else:
            err = f"hh HTTP {code}: {body or str(e)[:200]}"
        storage.write_error_snapshot(conn, query, area, err)
        return {"status": "error", "error": err}
    except Exception as e:  # сеть/парсинг — не роняем бота
        err = f"{type(e).__name__}: {str(e)[:200]}"
        storage.write_error_snapshot(conn, query, area, err)
        return {"status": "error", "error": err}
    finally:
        _busy.clear()
        _guard.release()


def is_busy():
    return _busy.is_set()


def _retention_days():
    return int(os.environ.get("RETENTION_RAW_DAYS", "90"))
