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
from itertools import combinations

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


def _cooccur_top_k():
    return int(os.environ.get("COOCCUR_TOP_K", "40"))


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
    "Ты эксперт по технологиям и языкам программирования. "
    "Ты извлекаешь требуемые навыки, технологии, технический стек из вакансий. Для каждой вакансии верни список "
    "технологий, языков, фреймворков, библиотек, БД, инструментов и платформ, которые "
    "реально требуются. НЕ включай soft skills, общие слова, названия компаний, "
    "должности. Используй канонические англоязычные названия там, где это принято "
    "(Python, PostgreSQL, Docker, FastAPI, Kubernetes). Ответь СТРОГО JSON без пояснений: "
    '{"vacancies":[{"i":<индекс>,"tech":["...","..."]}]}'
)


def _extract_batch(vacs, max_desc):
    prompt = "\n\n".join(_vacancy_block(i, v, max_desc) for i, v in enumerate(vacs))
    content = _chat([{"role": "system", "content": _BATCH_SYS},
                     {"role": "user", "content": prompt}])
    data = _parse_json(content)
    out = {}
    for item in data.get("vacancies", []) if isinstance(data, dict) else []:
        idx = item.get("i")
        techs = item.get("tech") or []
        if isinstance(idx, int):
            out[idx] = [t.strip() for t in techs if isinstance(t, str) and t.strip()]
    return out


# --- этап 2: нормализация/слияние синонимов по всему набору -------------------
_NORM_SYS = (
    "Ты эксперт по технологиям и языкам программирования. "
    "Тебе дан список названий технологий, извлечённых из вакансий. Объедини синонимы и "
    "разные написания к единому каноническому виду (например 'Postgres','постгрес' -> "
    "'PostgreSQL'; 'js' -> 'JavaScript'; 'k8s' -> 'Kubernetes'). Убери то, что не является "
    "технологией/инструментом (map в пустую строку). Ответь СТРОГО JSON без пояснений: "
    '{"map":{"исходное":"канон", ...}} для ВСЕХ входных названий.'
)


def _normalize(names):
    if not names:
        return {}
    content = _chat([{"role": "system", "content": _NORM_SYS},
                     {"role": "user", "content": json.dumps(names, ensure_ascii=False)}])
    data = _parse_json(content)
    mapping = data.get("map", {}) if isinstance(data, dict) else {}
    return {k: v for k, v in mapping.items() if isinstance(v, str)}


# --- сборка результата (тот же формат, что extract.aggregate) ----------------
def aggregate_llm(vacancies, progress=None):
    n = len(vacancies)
    batch = _batch_size()
    max_desc = _max_desc()
    total_batches = math.ceil(n / batch) if n else 0

    per_vac = []  # множество технологий на каждую вакансию (этап 1)
    for b in range(total_batches):
        chunk = vacancies[b * batch:(b + 1) * batch]
        if progress:
            progress(f"LLM: разбор батча {b + 1}/{total_batches}…")
        res = _extract_batch(chunk, max_desc)
        for idx in range(len(chunk)):
            per_vac.append(set(res.get(idx, [])))

    # этап 2: нормализация синонимов по всем найденным названиям
    distinct = sorted({t for s in per_vac for t in s})
    if progress and distinct:
        progress("LLM: нормализация результатов…")
    merge = _normalize(distinct) if distinct else {}

    def canon(t):
        return merge.get(t, t)  # нет в карте -> оставляем как есть

    per_vac = [{c for c in (canon(t) for t in s) if c} for s in per_vac]

    df = Counter()
    for s in per_vac:
        df.update(s)
    top = {t for t, _ in df.most_common(_cooccur_top_k())}
    cooccur = Counter()
    for s in per_vac:
        for a, b in combinations(sorted(s & top), 2):
            cooccur[(a, b)] += 1

    return {
        "n": n,
        "df": dict(df),
        "source": {t: "llm" for t in df},
        "cooccur": dict(cooccur),
        "raw_skills": {},
    }
