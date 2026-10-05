# Локальная проверка Python-алгоритмов

Kafka, PostgreSQL и запущенный Flink для этих проверок не нужны.

## Первый запуск

Откройте Terminal, перейдите в папку проекта и выполните:

```bash
cd /Users/aldarbazarov/Downloads/2_work/danon/flink_algorithms
./setup_local.sh
```

Скрипт создаст локальную папку `.venv` и установит зависимости из
`requirements-dev.txt`.

## Ежедневная работа

После изменения алгоритма запустите:

```bash
./check.sh
```

Команда последовательно выполняет:

1. создание временных копий шаблонов с заменой `<<TAGS>>`;
2. проверку синтаксиса через `py_compile`;
3. поиск очевидных ошибок через Ruff;
4. локальные тесты через Pytest.

Временные копии находятся в `.local_build`. Редактировать их не нужно — при
следующей проверке они создаются заново.

## Активация окружения вручную

Если хотите запускать отдельные команды:

```bash
source .venv/bin/activate
python --version
python -m py_compile algorithms/utils/algorithm_skeleton_new.py
ruff check tests tools examples
pytest
```

Для проверки файлов алгоритмов с шаблонами используйте `./check.sh`: обычный
`ruff check .` и Pylance видят `<<TAGS>>` до подстановки и помечают такую строку
как синтаксическую ошибку.

Выйти из окружения:

```bash
deactivate
```

## Visual Studio Code

Открывайте именно папку проекта, а не отдельный Python-файл:

```bash
code /Users/aldarbazarov/Downloads/2_work/danon/flink_algorithms
```

Проект уже настроен на интерпретатор `.venv/bin/python`, Pylance, Ruff и Pytest.
Если в правом нижнем углу указан другой Python, нажмите на его название и
выберите `.venv/bin/python` через команду `Python: Select Interpreter`.

Полную проверку можно запустить сочетанием `Cmd+Shift+B`. Тесты также доступны
на вкладке Testing (значок колбы на левой панели).

Файлы в `algorithms/` с отдельной строкой `<<TAGS>>` являются шаблонами, а не валидными
Python-файлами. Для командной проверки `./check.sh` создаёт их корректные копии
в `.local_build`. В самом исходном шаблоне Pylance будет считать `<<TAGS>>`
синтаксической ошибкой до подстановки тегов менеджером.
