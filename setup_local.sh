#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")" && pwd)"
cd "$project_dir"

python_bin="/opt/homebrew/bin/python3.11"
if [[ ! -x "$python_bin" ]]; then
    if command -v python3.11 >/dev/null 2>&1; then
        python_bin="$(command -v python3.11)"
    else
        echo "Python 3.11 не найден. На macOS установите: brew install python@3.11" >&2
        exit 1
    fi
fi

if [[ ! -x .venv/bin/python ]]; then
    "$python_bin" -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip wheel "setuptools<81" cython "numpy==2.1.3"

# На Apple Silicon для Apache Beam нет готового wheel этой версии,
# поэтому он собирается локально до установки PyFlink.
.venv/bin/python -m pip install --no-build-isolation "apache-beam==2.61.0"
.venv/bin/python -m pip install -r requirements-dev.txt

echo
echo "Окружение готово. Активировать:"
echo "  source .venv/bin/activate"
echo "Запустить все проверки:"
echo "  ./check.sh"

