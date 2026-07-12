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


def collect_once(conn, query, area, analyze_top_n, throttle=0.2, notify=None):
    """Собрать и записать один снимок. Возвращает dict со статусом.

    Не пересекается с другим прогоном: если занято — сразу выходит со status='busy'.
    """
    if not _guard.acquire(blocking=False):
        if notify:
            notify("Сбор уже идёт, подождите…")
        return {"status": "busy"}
    _busy.set()
    t0 = time.monotonic()
    try:
        session = build_session()
        # search_field пуст -> hh ищет по всем полям (как на сайте); можно сузить
        # через HH_SEARCH_FIELD=name (только по названию).
        search_field = os.environ.get("HH_SEARCH_FIELD") or None
        items, total_found = fetch_vacancies(
            session, query, area, per_page=100, max_items=analyze_top_n,
            extra_params={"order_by": "publication_time"}, search_field=search_field)

        total = len(items)
        print(f"[collect] обогащение деталями: {total} вакансий "
              f"(~{total * (throttle + 0.15):.0f}с)…", flush=True)
        enriched = 0
        for i, v in enumerate(items, 1):
            try:
                detail = fetch_vacancy_detail(session, v["id"])
                v["description"] = detail.get("description")
                v["key_skills"] = detail.get("key_skills")
                enriched += 1
                if throttle:
                    time.sleep(throttle)
            except requests.RequestException:
                continue  # деталь не загрузилась — останется сниппет как fallback
            if i % 25 == 0 or i == total:
                print(f"[collect]   деталей {i}/{total}", flush=True)

        learned = storage.learned_map(conn)
        skill_map = skills_dict.merged_with_learned(learned)
        matcher = extract.build_matcher(skill_map)
        alias_map = skills_dict.alias_to_canonical(skill_map)
        result = extract.aggregate(items, matcher, alias_map)

        meta = {
            "query_text": query, "area": area, "per_page": 100,
            "max_items": analyze_top_n, "total_found": total_found,
            "enriched_count": enriched, "duration_sec": round(time.monotonic() - t0, 1),
        }
        raw_rows = [_raw_row(v) for v in items]
        run_id = storage.write_snapshot(conn, meta, result, raw_rows)
        storage.upsert_learned(conn, result["raw_skills"], _known_aliases(skill_map))
        storage.prune_raw(conn, _retention_days())

        top = sorted(result["df"].items(), key=lambda kv: -kv[1])[:10]
        if notify:
            notify(f"Готово. Вакансий: {result['n']} (деталей: {enriched}). "
                   f"Топ: {', '.join(t for t, _ in top[:5])}")
        return {"status": "ok", "run_id": run_id, "n": result["n"],
                "enriched": enriched, "top": top}

    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        if code == 403:
            err = ("hh 403: нет валидного токена приложения "
                   "(HH_CLIENT_ID/HH_CLIENT_SECRET) или запрос с облачного IP")
        else:
            err = f"hh HTTP {code}: {str(e)[:200]}"
        storage.write_error_snapshot(conn, query, area, err)
        if notify:
            notify(f"Ошибка сбора: {err}")
        return {"status": "error", "error": err}
    except Exception as e:  # сеть/парсинг — не роняем бота
        err = f"{type(e).__name__}: {str(e)[:200]}"
        storage.write_error_snapshot(conn, query, area, err)
        if notify:
            notify(f"Ошибка сбора: {err}")
        return {"status": "error", "error": err}
    finally:
        _busy.clear()
        _guard.release()


def is_busy():
    return _busy.is_set()


def _retention_days():
    return int(os.environ.get("RETENTION_RAW_DAYS", "90"))
