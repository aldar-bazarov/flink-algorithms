#!/usr/bin/env bash
set -uo pipefail

project_dir="$(cd "$(dirname "$0")" && pwd)"
cd "$project_dir"

if [[ ! -x .venv/bin/python ]]; then
    echo "Не найдено виртуальное окружение .venv. Сначала выполните ./setup_local.sh" >&2
    exit 1
fi

status=0

echo "[1/4] Подготовка локальных копий шаблонов"
.venv/bin/python tools/render_local_templates.py || status=1

echo
echo "[2/4] Проверка синтаксиса через py_compile"
if .venv/bin/python -m compileall -q .local_build; then
    echo "Синтаксис: OK"
else
    status=1
fi

echo
echo "[3/4] Статический анализ Ruff"
.venv/bin/ruff check .local_build tools tests --config pyproject.toml || status=1

echo
echo "[4/4] Тесты Pytest"
.venv/bin/python -m pytest || status=1

echo
if [[ "$status" -eq 0 ]]; then
    echo "Все локальные проверки успешно пройдены."
else
    echo "Есть ошибки. Исправьте сообщения выше и снова выполните ./check.sh" >&2
fi

exit "$status"
