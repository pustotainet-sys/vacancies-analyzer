#!/usr/bin/env python3
"""Сбор вакансий с hh.ru через API поиска.

С хостинговых/облачных IP анти-бот hh отдаёт 403 на /vacancies без авторизации.
Решается токеном приложения: скрипт сам получает его через grant_type=client_credentials
из HH_CLIENT_ID / HH_CLIENT_SECRET (данные твоего зарегистрированного приложения).
Готовый токен можно передать напрямую через HH_ACCESS_TOKEN.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

import envfile
envfile.load()  # .env -> os.environ до чтения констант ниже

API_URL = "https://api.hh.ru/vacancies"
TOKEN_URL = "https://api.hh.ru/token"
# hh просит указывать User-Agent, идентифицирующий приложение (см. док API).
USER_AGENT = os.getenv("HH_USER_AGENT", "hh-vacancies-analyzer/1.0")
MAX_PER_PAGE = 100
# hh отдаёт максимум 2000 результатов на один поисковый запрос.
API_RESULT_LIMIT = 2000


# Кэш токена приложения. hh отдаёт долгоживущий токен (~14 дней) и запрещает
# частые запросы нового ("app token refresh too early"). Поэтому храним токен на диске
# и переиспользуем между перезапусками; время — wall-clock (time.time), чтобы пережить рестарт.
_HERE = os.path.dirname(os.path.abspath(__file__))
_TOKEN_FILE = os.path.join(_HERE, ".hh_token.json")
_token_cache = {"token": None, "exp": 0.0}


def _load_token_file():
    try:
        with open(_TOKEN_FILE, encoding="utf-8") as f:
            d = json.load(f)
        return d.get("token"), float(d.get("exp", 0))
    except Exception:
        return None, 0.0


def _save_token_file(token, exp):
    try:
        with open(_TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump({"token": token, "exp": exp}, f)
    except Exception as exc:
        print(f"ВНИМАНИЕ: не удалось сохранить токен: {exc}", file=sys.stderr)


def get_app_token(client_id, client_secret):
    now = time.time()
    if _token_cache["token"] and now < _token_cache["exp"]:
        return _token_cache["token"]
    ftoken, fexp = _load_token_file()  # переживает перезапуск -> без лишних /token
    if ftoken and now < fexp:
        _token_cache["token"], _token_cache["exp"] = ftoken, fexp
        return ftoken

    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data["access_token"]
    ttl = int(data.get("expires_in", 1209600))  # hh: обычно ~2 недели
    exp = now + max(60, ttl - 300)  # обновить заранее, с запасом 5 мин
    _token_cache["token"], _token_cache["exp"] = token, exp
    _save_token_file(token, exp)
    print("Получен токен приложения (client_credentials).", file=sys.stderr)
    return token


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    token = os.getenv("HH_ACCESS_TOKEN")
    if not token:
        client_id = os.getenv("HH_CLIENT_ID")
        client_secret = os.getenv("HH_CLIENT_SECRET")
        if client_id and client_secret:
            # неудача получения токена НЕ фатальна: с домашнего IP поиск работает и без него
            try:
                token = get_app_token(client_id, client_secret)
            except Exception as exc:
                print(f"ВНИМАНИЕ: не удалось получить токен приложения: {exc}. "
                      "Продолжаю без токена (ок для домашнего IP).", file=sys.stderr)

    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    else:
        print(
            "ВНИМАНИЕ: работаю без токена. С облачного/хостингового IP hh вернёт 403 на поиск.",
            file=sys.stderr,
        )
    return session


def fetch_page(session, params, page):
    resp = session.get(API_URL, params={**params, "page": page}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_vacancies(session, text, area, per_page, max_items, extra_params=None,
                    search_field=None):
    params = {
        "text": text,
        "per_page": min(per_page, MAX_PER_PAGE),
    }
    if area not in (None, "", "all"):
        params["area"] = area
    # Без search_field hh ищет по всем полям. Список значений requests передаёт
    # повторяющимися search_field; строка "name,description" у hh не работает.
    if search_field:
        params["search_field"] = search_field
    if extra_params:
        params.update(extra_params)

    items = []
    first = fetch_page(session, params, 0)
    total_found = first["found"]
    total_pages = first["pages"]
    items.extend(first["items"])
    print(f"Найдено вакансий: {total_found} (страниц: {total_pages})", file=sys.stderr)

    for page in range(1, total_pages):
        if max_items and len(items) >= max_items:
            break
        if page * params["per_page"] >= API_RESULT_LIMIT:
            print(f"Достигнут лимит API в {API_RESULT_LIMIT} результатов на запрос.", file=sys.stderr)
            break
        data = fetch_page(session, params, page)
        items.extend(data["items"])
        print(f"  страница {page + 1}/{total_pages}, собрано {len(items)}", file=sys.stderr)
        time.sleep(0.3)  # мягкий троттлинг, чтобы не упереться в rate limit

    if max_items:
        items = items[:max_items]
    return items, total_found


def fetch_relevant_vacancies(session, text, area, per_page, max_items):
    """Первые 80% — совпадения в названии, остальные — в описании без дублей."""
    title_limit = max(1, max_items * 4 // 5)
    search_options = {"order_by": "relevance"}
    title_items, title_found = fetch_vacancies(
        session, text, area, per_page, title_limit,
        extra_params=search_options, search_field="name")
    description_query = f'"{text}"' if len(text.split()) > 1 else text
    description_items, description_found = fetch_vacancies(
        session, description_query, area, per_page, max_items,
        extra_params=search_options, search_field="description")
    selected = list(title_items)
    seen_ids = {str(vacancy["id"]) for vacancy in selected}
    for vacancy in description_items:
        vacancy_id = str(vacancy["id"])
        if vacancy_id not in seen_ids:
            selected.append(vacancy)
            seen_ids.add(vacancy_id)
        if len(selected) >= max_items:
            break
    return selected, max(title_found, description_found)


def flatten(v):
    salary = v.get("salary") or {}
    return {
        "id": v.get("id"),
        "name": v.get("name"),
        "company": (v.get("employer") or {}).get("name"),
        "area": (v.get("area") or {}).get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "currency": salary.get("currency"),
        "experience": (v.get("experience") or {}).get("name"),
        "employment": (v.get("employment") or {}).get("name"),
        "schedule": (v.get("schedule") or {}).get("name"),
        "published_at": v.get("published_at"),
        "url": v.get("alternate_url"),
    }


def save_json(items, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def save_csv(rows, path):
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_args():
    p = argparse.ArgumentParser(description="Сбор вакансий с hh.ru")
    p.add_argument("--text", default="python", help="поисковый запрос (по умолчанию: python)")
    p.add_argument("--area", default="113", help="регион: all=без ограничения, 113=Россия (по умолчанию: 113)")
    p.add_argument("--per-page", type=int, default=100, help="вакансий на страницу, макс 100")
    p.add_argument("--max-items", type=int, default=500, help="сколько всего вакансий собрать (0 = все доступные)")
    p.add_argument("--out-dir", default=".", help="каталог для выходных файлов")
    return p.parse_args()


def main():
    args = parse_args()
    session = build_session()

    try:
        raw, total_found = fetch_vacancies(
            session, args.text, args.area, args.per_page, args.max_items or None
        )
    except requests.HTTPError as e:
        print(f"Ошибка API: {e} — {e.response.text[:300]}", file=sys.stderr)
        sys.exit(1)

    rows = [flatten(v) for v in raw]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.out_dir, exist_ok=True)
    json_path = os.path.join(args.out_dir, f"vacancies_{args.text}_{stamp}.json")
    csv_path = os.path.join(args.out_dir, f"vacancies_{args.text}_{stamp}.csv")

    save_json(raw, json_path)
    save_csv(rows, csv_path)

    print(f"\nВсего по запросу '{args.text}': {total_found}")
    print(f"Собрано и сохранено: {len(rows)}")
    print(f"  JSON: {json_path}")
    print(f"  CSV:  {csv_path}")


if __name__ == "__main__":
    main()
