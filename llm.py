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
import time
from collections import Counter
from contextlib import ExitStack
from datetime import datetime

import requests

from extract import strip_html

API_URL = "https://openrouter.ai/api/v1/chat/completions"
MAX_MODEL_ATTEMPTS = 3

# перечень моделей по приоритету: при ошибке пробуем следующую
DEFAULT_MODELS = [
    "poolside/laguna-xs-2.1:free",
    "nvidia/nemotron-3-nano-30b-a3b:free",
    "cohere/north-mini-code:free",
]


def api_key():
    return os.environ.get("OPENROUTER_API_KEY", "").strip()


# модели прямого Gemini API по приоритету: при ошибке (например 503) пробуем следующую
DEFAULT_GEMINI_MODELS = [
    "gemini-3.5-flash",
    "gemini-3.1-flash-lite",
    "gemini-2.5-flash-lite",
]


def gemini_key():
    return os.environ.get("GEMINI_API_KEY", "").strip()


def gemini_models():
    """Список моделей Gemini по приоритету. env GEMINI_MODELS (через запятую) переопределяет."""
    raw = os.environ.get("GEMINI_MODELS", "").strip()
    if raw:
        return [m.strip() for m in raw.split(",") if m.strip()]
    single = os.environ.get("GEMINI_MODEL", "").strip()  # обратная совместимость
    if single:
        return [single]
    return list(DEFAULT_GEMINI_MODELS)


def available():
    # LLM доступен, если есть хотя бы один провайдер: прямой Gemini или OpenRouter
    return bool(gemini_key() or api_key())


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
    # Вакансий в батч. Ограничение — лимит ВЫВОДА модели (LLM_MAX_TOKENS), не вход:
    # каждая вакансия даёт ~15-25 пунктов. У Gemini flash выход до 64K, так что батч
    # 50-60 помещается, если LLM_MAX_TOKENS достаточно большой.
    return int(os.environ.get("LLM_BATCH_SIZE", "50"))


def _max_desc():
    return int(os.environ.get("LLM_MAX_DESC", "1500"))


def _max_tokens():
    # Лимит вывода модели. Раньше стоял 8000 и РЕЗАЛ ответ на больших батчах.
    # У Gemini flash выход до 64K, поэтому ставим с запасом под батч 50-60.
    # (Осторожно с платными моделями OpenRouter: слишком большой -> 402 по кредитам.
    #  У :free-моделей и прямого Gemini этой проблемы нет.)
    return int(os.environ.get("LLM_MAX_TOKENS", "32000"))


def _dump_dir():
    # Куда складывать сырой результат этапа 1. По умолчанию выключено: при многих
    # пользователях дамп пишется на каждую задачу и каталог растёт неограниченно.
    if os.environ.get("LLM_DUMP", "0").strip() not in ("1", "true", "yes"):
        return None
    return os.environ.get("LLM_DUMP_DIR", "llm_dumps").strip() or None


def _open_dump(n, total_batches):
    """Открыть JSONL-файл под сырые результаты первой фазы. None — дамп выключен/не открылся."""
    d = _dump_dir()
    if not d:
        return None
    try:
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, f"phase1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")
        f = open(path, "w", encoding="utf-8")
    except OSError as e:
        _log(f"не удалось открыть файл дампа: {e}")
        return None
    _dump_write(f, {"type": "meta", "vacancies": n, "batches": total_batches,
                    "batch_size": _batch_size(), "max_desc": _max_desc(),
                    "started": datetime.now().isoformat(timespec="seconds")})
    _log(f"сырые результаты первой фазы: {path}")
    return f


def _dump_write(f, obj):
    if not f:
        return
    try:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        f.flush()  # чтобы файл был пригоден для анализа даже при падении на середине
    except OSError as e:
        _log(f"ошибка записи дампа: {e}")


def _retry_model(error, model, attempt):
    """Повторить временный сбой, но не постоянный HTTP 4xx, кроме 429."""
    if attempt >= MAX_MODEL_ATTEMPTS:
        return False
    if isinstance(error, requests.HTTPError):
        status = error.response.status_code if error.response is not None else None
    else:
        status = getattr(error, "code", None)
    if isinstance(status, int) and status < 500 and status != 429:
        return False
    _log(f"повторяю {model}: попытка {attempt + 1}/{MAX_MODEL_ATTEMPTS}")
    time.sleep(attempt)
    return True


def _chat_gemini(messages, temperature):
    """Прямой Gemini: до трёх попыток каждой модели, затем следующая."""
    from google import genai
    from google.genai import types
    system = "\n".join(m["content"] for m in messages if m.get("role") == "system")
    user = "\n\n".join(m["content"] for m in messages if m.get("role") != "system")
    with ExitStack() as stack:
        if os.environ.get("GEMINI_FORCE_IPV4") == "1":
            import httpx2
            transport = httpx2.HTTPTransport(local_address="0.0.0.0")
            http_client = stack.enter_context(httpx2.Client(transport=transport))
            client = stack.enter_context(genai.Client(
                http_options=types.HttpOptions(httpx_client=http_client)))
        else:
            client = stack.enter_context(genai.Client())
        cfg = types.GenerateContentConfig(temperature=temperature,
                                          max_output_tokens=_max_tokens(),
                                          system_instruction=system or None)
        ms = gemini_models()
        last_err = None
        for idx, m in enumerate(ms):
            for attempt in range(1, MAX_MODEL_ATTEMPTS + 1):
                try:
                    _log(f"Gemini {m} (приоритет {idx + 1}/{len(ms)}, "
                         f"попытка {attempt}/{MAX_MODEL_ATTEMPTS})")
                    resp = client.models.generate_content(model=m, contents=user, config=cfg)
                    text = resp.text
                    if not isinstance(text, str) or not text.strip():
                        raise RuntimeError("пустой ответ")
                    if idx > 0:
                        _log(f"успешно на резервной Gemini {m}")
                    return text
                except Exception as e:
                    last_err = e
                    _log(f"ошибка Gemini {m}: {type(e).__name__}: {str(e)[:150]}")
                    if _retry_model(e, m, attempt):
                        continue
                    break
            if idx + 1 < len(ms):
                _log(f"переключаюсь на следующую Gemini: {ms[idx + 1]}")
        raise RuntimeError(f"все {len(ms)} Gemini-моделей не сработали; последняя: {last_err}")


def _chat_openrouter(messages, temperature, timeout):
    key = api_key()
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY не задан")
    ms = models()
    last_err = None
    for idx, m in enumerate(ms):
        for attempt in range(1, MAX_MODEL_ATTEMPTS + 1):
            try:
                _log(f"запрос к OpenRouter {m} (приоритет {idx + 1}/{len(ms)}, "
                     f"попытка {attempt}/{MAX_MODEL_ATTEMPTS})")
                resp = requests.post(
                    API_URL,
                    headers={"Authorization": f"Bearer {key}",
                             "Content-Type": "application/json"},
                    json={"model": m, "messages": messages, "temperature": temperature,
                          "max_tokens": _max_tokens()},
                    timeout=timeout,
                )
                resp.raise_for_status()
                body = resp.json()
                choice = body["choices"][0]
                content = choice["message"].get("content")
                selected_model = body.get("model") or m
                finish_reason = choice.get("finish_reason")
                _log(f"OpenRouter выбрал {selected_model}; finish_reason={finish_reason}")
                if not isinstance(content, str) or not content.strip():
                    raise RuntimeError(f"пустой текст от {selected_model}; "
                                       f"finish_reason={finish_reason}")
                if idx > 0:
                    _log(f"успешно на резервной модели {m}")
                return content
            except Exception as e:
                detail = ""
                if isinstance(e, requests.HTTPError) and e.response is not None:
                    detail = f" HTTP {e.response.status_code}: {e.response.text[:150]}"
                last_err = e
                _log(f"ошибка модели {m}: {type(e).__name__}{detail}")
                if _retry_model(e, m, attempt):
                    continue
                break
        if idx + 1 < len(ms):
            _log(f"переключаюсь на следующую модель: {ms[idx + 1]}")
    raise RuntimeError(f"OpenRouter: все {len(ms)} моделей не сработали; последняя: {last_err}")


def _chat(messages, temperature=0.0, timeout=180):
    """Запрос к LLM с приоритетом провайдеров: прямой Gemini -> OpenRouter.

    Всё упало — RuntimeError (выше по стеку это приведёт к откату на базовый режим).
    """
    errors = []
    # 1) прямой Gemini API — высший приоритет (со своим перебором моделей)
    if gemini_key():
        try:
            return _chat_gemini(messages, temperature)
        except Exception as e:
            errors.append(f"gemini: {type(e).__name__}: {str(e)[:150]}")
            _log("все Gemini-модели не сработали — переключаюсь на OpenRouter")
    # 2) OpenRouter (цепочка моделей по приоритету)
    if api_key():
        try:
            return _chat_openrouter(messages, temperature, timeout)
        except Exception as e:
            errors.append(str(e)[:200])
    raise RuntimeError("все провайдеры LLM не сработали: " +
                       (" | ".join(errors) if errors else "нет ключей (GEMINI_API_KEY/OPENROUTER_API_KEY)"))


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
    out = {}
    data = _parse_json(content)
    for item in data.get("vacancies", []) if isinstance(data, dict) else []:
        idx = item.get("i")
        vals = item.get("items") or item.get("tech") or []  # "tech" — совместимость
        if isinstance(idx, int):
            out[idx] = [t.strip() for t in vals if isinstance(t, str) and t.strip()]
    if not out:
        # JSON мог быть обрезан по лимиту вывода — спасаем целые объекты вакансий
        for m in re.finditer(r'"i"\s*:\s*(\d+)\s*,\s*"(?:items|tech)"\s*:\s*\[([^\]]*)\]',
                             content):
            idx = int(m.group(1))
            vals = re.findall(r'"([^"]+)"', m.group(2))
            out[idx] = [t.strip() for t in vals if t.strip()]
        if out:
            _log(f"ответ был обрезан — спасено вакансий: {len(out)} "
                 "(уменьшите LLM_BATCH_SIZE)")
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
    dump = _open_dump(n, total_batches)
    try:
        for b in range(total_batches):
            chunk = vacancies[b * batch:(b + 1) * batch]
            if progress:
                progress(f"LLM: разбор батча {b + 1}/{total_batches}…")
            res = _extract_batch(chunk, max_desc)
            for idx in range(len(chunk)):
                items = res.get(idx, [])
                seen_counts.update(set(items))  # set -> одна вакансия = один голос
                v = chunk[idx]
                _dump_write(dump, {
                    "type": "vacancy", "batch": b + 1, "i": b * batch + idx,
                    "id": v.get("id"), "name": v.get("name"),
                    "url": v.get("alternate_url"),
                    "employer": (v.get("employer") or {}).get("name"),
                    "items": items,
                })
        _dump_write(dump, {"type": "raw_counts",
                           "counts": dict(seen_counts.most_common())})
    finally:
        if dump:
            dump.close()

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
