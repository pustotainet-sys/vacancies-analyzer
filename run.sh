#!/usr/bin/env bash
# Запуск бота строго интерпретатором из .venv: системный python3 не видит зависимостей.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

# Ищем процесс по АБСОЛЮТНОМУ пути: в системе есть другие проекты с файлом bot.py,
# и поиск по одному имени глушил бы запуск из-за чужого процесса.
PATTERN="$HERE/bot.py"

if [ ! -x .venv/bin/python ]; then
    echo "Нет .venv — создаю и ставлю зависимости…"
    uv venv .venv
    uv pip install --python .venv -r requirements.txt
fi

if [ "${1:-}" = "restart" ]; then
    pkill -f "$PATTERN" 2>/dev/null || true
    sleep 1
elif [ "${1:-}" = "stop" ]; then
    pkill -f "$PATTERN" 2>/dev/null && echo "Остановлен." || echo "Не был запущен."
    exit 0
fi

# один экземпляр на токен: два процесса дерутся за getUpdates и апдейты теряются
if pgrep -f "$PATTERN" >/dev/null; then
    echo "Бот уже запущен:" >&2
    pgrep -af "$PATTERN" >&2
    echo "Перезапуск: ./run.sh restart   Остановка: ./run.sh stop" >&2
    exit 1
fi

exec .venv/bin/python "$PATTERN"
