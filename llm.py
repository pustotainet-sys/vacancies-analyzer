"""Извлечение технологий через LLM (OpenRouter), иерархически.

Основной режим: вакансии обрабатываются батчами по N штук (чтобы влезть в контекст
модели) — для каждой вакансии модель возвращает список требуемых технологий. Затем
второй проход LLM нормализует/сливает синонимы по всему набору. Результат имеет тот
же формат, что и extract.aggregate: {n, df, source, cooccur, raw_skills}.

Ключ берётся из OPENROUTER_API_KEY, модель — из OPENROUTER_MODEL.
"""

import json
import math
import os
import re
from collections import Counter

import requests

from extract import strip_html

API_URL = "https://openrouter.ai/api/v1/chat/completions"

# перечень моделей по приоритету: при ошибке пробуем следующую
DEFAULT_MODELS = [
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-nano-30b-a3b:free",
    "cohere/north-mini-code:free",
]


def api_key():
    return os.environ.get("OPENROUTER_API_KEY", "").strip()


def available():
    return bool(api_key())


def models():
    """Список моделей по приоритету. env OPENROUTER_MODELS (через запятую) переопределяет."""
    raw = os.environ.get("OPENROUTER_MODELS", "").strip()
    if raw:
        return [m.strip() for m in raw.split(",") if m.strip()]
    single = os.environ.get("OPENROUTER_MODEL", "").strip()  # обратная совместимость
    if single:
        return [single]
    return list(DEFAULT_MODELS)


def _log(msg):
    print(f"[llm] {msg}", flush=True)


def _batch_size():
    # модели с контекстом 120k+; ~1k токенов на вакансию -> ~50 вакансий в батч
    return int(os.environ.get("LLM_BATCH_SIZE", "50"))


def _max_desc():
    return int(os.environ.get("LLM_MAX_DESC", "1500"))


def _max_tokens():
    # лимит вывода. Для 50 вакансий JSON-ответ крупнее — берём с запасом.
    # (у платных моделей слишком большой max_tokens может дать 402 по кредитам.)
    return int(os.environ.get("LLM_MAX_TOKENS", "8000"))


def _chat(messages, temperature=0.0, timeout=180):
    """Запрос к LLM с перебором моделей по приоритету.

    При ошибке модели логируем и переходим к следующей. Если упали все — RuntimeError
    (выше по стеку это приведёт к откату на базовый режим без LLM).
    """
    key = api_key()
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY не задан")
    ms = models()
    last_err = None
    for idx, m in enumerate(ms):
        try:
            _log(f"запрос к модели {m} (приоритет {idx + 1}/{len(ms)})")
            resp = requests.post(
                API_URL,
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json"},
                json={"model": m, "messages": messages, "temperature": temperature,
                      "max_tokens": _max_tokens()},
                timeout=timeout,
            )
            resp.raise_for_status()
            if idx > 0:
                _log(f"успешно на резервной модели {m}")
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            detail = ""
            if isinstance(e, requests.HTTPError) and e.response is not None:
                detail = f" HTTP {e.response.status_code}: {e.response.text[:150]}"
            last_err = e
            _log(f"ошибка модели {m}: {type(e).__name__}{detail}")
            if idx + 1 < len(ms):
                _log(f"переключаюсь на следующую модель: {ms[idx + 1]}")
    raise RuntimeError(f"все {len(ms)} моделей не сработали; последняя ошибка: {last_err}")


def _parse_json(text):
    """Достать JSON-объект из ответа модели (учитывая ```-ограждения и лишний текст)."""
    if not text:
        return {}
    t = text.strip()
    t = re.sub(r"^```(?:json)?|```$", "", t.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{.*\}", t, flags=re.DOTALL)  # первый {...}
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return {}
    return {}


# --- этап 1: извлечение по батчу --------------------------------------------
def _vacancy_block(i, v, max_desc):
    parts = [f"[{i}] Название: {v.get('name') or ''}"]
    ks = v.get("key_skills")
    if ks:
        names = [k.get("name") if isinstance(k, dict) else k for k in ks]
        names = [n for n in names if n]
        if names:
            parts.append("Ключевые навыки: " + ", ".join(names))
    desc = strip_html(v.get("description") or "")
    if not desc:
        sn = v.get("snippet") or {}
        desc = strip_html(" ".join(x for x in (sn.get("requirement"),
                                               sn.get("responsibility")) if x))
    if desc:
        parts.append("Описание: " + desc[:max_desc])
    return "\n".join(parts)


_BATCH_SYS = (
    "Ты эксперт по IT-вакансиям. Извлекаешь из вакансий требования ДВУХ видов:\n"
    "1) Технологии — языки, фреймворки, библиотеки, БД, инструменты, платформы "
    "(Python, PostgreSQL, Docker, FastAPI, Kubernetes).\n"
    "2) Навыки и рабочие процессы — конкретные профессиональные компетенции и задачи, "
    "например: обучение моделей, инференс LLM, формирование датасетов, построение ETL, "
    "проектирование архитектуры, code review, A/B-тесты, MLOps, дообучение (fine-tuning), RAG.\n"
    "Для каждой вакансии верни ОБЪЕДИНЁННЫЙ список этих требований. НЕ включай общие soft "
    "skills (коммуникабельность, ответственность), названия компаний и должностей, фразы "
    "вроде «будет плюсом». Формулируй кратко и канонично: технологии — общепринятым "
    "названием, навыки/процессы — коротким термином по-русски (существительное/герундий). "
    "Ответь СТРОГО JSON без пояснений: "
    '{"vacancies":[{"i":<индекс>,"items":["...","..."]}]}'
)


def _extract_batch(vacs, max_desc):
    prompt = "\n\n".join(_vacancy_block(i, v, max_desc) for i, v in enumerate(vacs))
    content = _chat([{"role": "system", "content": _BATCH_SYS},
                     {"role": "user", "content": prompt}])
    data = _parse_json(content)
    out = {}
    for item in data.get("vacancies", []) if isinstance(data, dict) else []:
        idx = item.get("i")
        vals = item.get("items") or item.get("tech") or []  # "tech" — совместимость
        if isinstance(idx, int):
            out[idx] = [t.strip() for t in vals if isinstance(t, str) and t.strip()]
    return out


# --- этап 2: финальное слияние перефразировок и ранжирование самой моделью ----
# Не считаем частоту по точным строкам (перефразировки разошлись бы). Отдаём модели
# все извлечённые пункты, а она сама сливает синонимы/перефразировки и сортирует по частоте.
_RANK_SYS = (
    "Ты эксперт по IT-вакансиям. Тебе дан список требований (технологии, навыки, рабочие "
    "процессы), извлечённых из вакансий, с числом вакансий, где встретился каждый пункт. "
    "Объедини синонимы, разные написания и ПЕРЕФРАЗИРОВКИ одного и того же смысла в один "
    "пункт, СУММИРУЯ их числа (например 'Postgres'+'постгрес' -> 'PostgreSQL'; "
    "'обучение моделей'+'тренировка ML-моделей' -> 'Обучение моделей'; "
    "'инференс'+'inference LLM' -> 'Инференс LLM'). Сохраняй и технологии, и навыки/процессы; "
    "убери общие soft skills и мусор. Верни ИТОГОВЫЙ список, ОТСОРТИРОВАННЫЙ по убыванию "
    "частоты. Канон — кратко: технологии общепринято, навыки/процессы по-русски. "
    "Ответь СТРОГО JSON без пояснений: "
    '{"ranked":[{"name":"канон","count":<целое>}, ...]}'
)


def _rank(item_counts):
    """item_counts: {строка: сколько вакансий содержат}. Возвращает [(канон, count)] по убыванию."""
    if not item_counts:
        return []
    payload = [{"item": k, "count": v}
               for k, v in sorted(item_counts.items(), key=lambda kv: -kv[1])]
    content = _chat([{"role": "system", "content": _RANK_SYS},
                     {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}])
    data = _parse_json(content)
    ranked = data.get("ranked", []) if isinstance(data, dict) else []
    out = []
    for r in ranked:
        if not isinstance(r, dict):
            continue
        name = (r.get("name") or "").strip()
        cnt = r.get("count")
        if name and isinstance(cnt, (int, float)) and cnt > 0:
            out.append((name, int(cnt)))
    return out


# --- сборка результата (тот же формат, что extract.aggregate) ----------------
def aggregate_llm(vacancies, progress=None):
    n = len(vacancies)
    batch = _batch_size()
    max_desc = _max_desc()
    total_batches = math.ceil(n / batch) if n else 0

    # этап 1: по каждой вакансии — множество требований
    seen_counts = Counter()  # сколько вакансий содержат каждую сырую формулировку
    for b in range(total_batches):
        chunk = vacancies[b * batch:(b + 1) * batch]
        if progress:
            progress(f"LLM: разбор батча {b + 1}/{total_batches}…")
        res = _extract_batch(chunk, max_desc)
        for idx in range(len(chunk)):
            seen_counts.update(set(res.get(idx, [])))  # set -> одна вакансия = один голос

    # этап 2: модель сама сливает перефразировки и сортирует по частоте
    if progress and seen_counts:
        progress("LLM: итоговое слияние и ранжирование…")
    ranked = _rank(dict(seen_counts))
    if not ranked:  # модель не вернула — падать не будем, возьмём сырые частоты
        ranked = sorted(seen_counts.items(), key=lambda kv: -kv[1])

    df = {}
    for name, cnt in ranked:
        df[name] = min(cnt, n)  # не больше числа вакансий

    return {
        "n": n,
        "df": df,
        "source": {t: "llm" for t in df},
        "cooccur": {},  # в LLM-режиме co-occurrence не считаем
        "raw_skills": {},
    }
