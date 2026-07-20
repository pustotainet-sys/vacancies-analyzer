"""Загрузка параметров из .env рядом со скриптом (без внешних зависимостей).

Приоритет: значения из .env перекрывают переменные окружения (override=True),
переменные окружения остаются фолбэком для ключей, которых в .env нет.
Формат строк: KEY=VALUE, поддержаны пустые строки, комментарии (#) — в том числе
хвостовые (`KEY=value  # пояснение`, как в .env.example), префикс `export ` и
обрамляющие кавычки. Значение с символом # сохраняется, если взято в кавычки.
"""

import os

HERE = os.path.dirname(os.path.abspath(__file__))


def _strip_comment(val):
    """Снять обрамляющие кавычки или отрезать хвостовой комментарий.

    Кавычки важнее: в них # — часть значения. Без кавычек комментарием считается
    только `#` после пробела, иначе пострадали бы ключи и пароли с решёткой.
    """
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        return val[1:-1]
    for sep in (" #", "\t#"):
        pos = val.find(sep)
        if pos != -1:
            val = val[:pos]
    return val.strip()


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
            val = _strip_comment(val)
            if override or key not in os.environ:
                os.environ[key] = val
                loaded[key] = val
    return loaded
