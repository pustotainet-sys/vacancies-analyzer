"""Загрузка параметров из .env рядом со скриптом (без внешних зависимостей).

Приоритет: значения из .env перекрывают переменные окружения (override=True),
переменные окружения остаются фолбэком для ключей, которых в .env нет.
Формат строк: KEY=VALUE, поддержаны пустые строки, комментарии (#), префикс
`export ` и обрамляющие кавычки.
"""

import os

HERE = os.path.dirname(os.path.abspath(__file__))


def load(path=None, override=True):
    path = path or os.path.join(HERE, ".env")
    if not os.path.exists(path):
        return {}
    loaded = {}
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if not key:
                continue
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if override or key not in os.environ:
                os.environ[key] = val
                loaded[key] = val
    return loaded
