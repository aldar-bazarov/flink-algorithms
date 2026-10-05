# Как писать потоковые алгоритмы на основе `algorithm_skeleton_new.py`

Этот документ описывает практический контракт [`algorithm_skeleton_new.py`](../algorithms/utils/algorithm_skeleton_new.py): что нужно настроить, какую функцию написать и какие данные она получает. Знать внутреннее устройство Apache Flink для создания простого алгоритма не обязательно.

## Что делает каркас

Каркас постоянно читает JSON-сообщения из Kafka, выбирает из них нужные теги и запоминает последнее корректное значение каждого тега. Когда приходит тег-триггер и все входные значения уже известны, каркас вызывает функцию вашего алгоритма. Полученный результат он собирает в пакет и отправляет обратно в Kafka.

В упрощенном виде поток выглядит так:

```text
Kafka RawTopic
    -> разбор JSON
    -> фильтрация InputTags
    -> кэш последних значений
    -> calculate_result(...)
    -> группировка результата по sink
    -> Kafka ResultTopic / дополнительные топики
```

Каркас также:

- хранит состояние алгоритма в checkpoint Flink;
- восстанавливает состояние после перезапуска задачи;
- может читать дополнительные Kafka-топики, например справочники или мастер-данные;
- может отправлять результаты в несколько Kafka-топиков;
- запускает обработку последовательно: `parallelism = 1`.

## Самый короткий рабочий алгоритм

Ниже алгоритм складывает два входных тега и публикует один выходной тег.

```python
from utils.algorithm_skeleton_new import (
    AlgorithmStateStatus,
    KafkaSinkMessage,
    SingleOutputAlgorithmConfig,
    build_raw_result,
    extract_int_value,
    format_timestamp_millis,
    get_max_timestamp_from_details,
    run_single_output_algorithm,
)


TEMPERATURE_TAG = "Plant.Line1.Temperature"
CORRECTION_TAG = "Plant.Line1.Correction"
RESULT_TAG = "Plant.Line1.AdjustedTemperature"


CONFIG = SingleOutputAlgorithmConfig(
    GroupId="adjusted-temperature-v1",
    JobName="adjusted-temperature",
    AlgorithmID="algorithm-instance-id",
    RawTopic="lab.tag.processing",
    ResultTopic="lab.tag.store",
    BootstrapServer="kafka:9092",
    StartFrom="latest",
    ObjectId="line-1",
    InputTags=(TEMPERATURE_TAG, CORRECTION_TAG),
    TriggerTags=(TEMPERATURE_TAG, CORRECTION_TAG),
)


def calculate_result(details_by_tag, source_states, main_state):
    # Эти аргументы в простом алгоритме не нужны.
    del source_states
    del main_state

    temperature = extract_int_value(details_by_tag[TEMPERATURE_TAG])
    correction = extract_int_value(details_by_tag[CORRECTION_TAG])
    timestamp = format_timestamp_millis(
        get_max_timestamp_from_details([
            details_by_tag[TEMPERATURE_TAG],
            details_by_tag[CORRECTION_TAG],
        ])
    )

    result = build_raw_result(
        timestamp=timestamp,
        tag=RESULT_TAG,
        value=temperature + correction,
        source_type=details_by_tag["source_type"],
        plant_id=details_by_tag["plant_id"],
        object_id=details_by_tag["object_id"],
    )

    return KafkaSinkMessage(SinkName="default", Value=result), AlgorithmStateStatus.Done


if __name__ == "__main__":
    run_single_output_algorithm(CONFIG, calculate_result)
```

Для нового скрипта обычно достаточно:

1. Скопировать этот пример в отдельный `.py`-файл.
2. Задать Kafka-параметры, идентификаторы и имена тегов.
3. Реализовать `calculate_result`.
4. Вызвать `run_single_output_algorithm` под условием `if __name__ == "__main__"`.

Рекомендуется передавать поля конфигурации по именам, а не позиционно: так изменение порядка полей не приведет к незаметной подмене значений.

## Формат входного сообщения

Основной топик должен содержать JSON-объекты следующего вида:

```json
{
  "SourceType": "PLC",
  "PlantId": "plant-1",
  "ObjectId": "line-1",
  "Timestamp": "2026-09-22T08:30:00.000+00:00",
  "Tags": [
    {
      "FullName": "Plant.Line1.Temperature",
      "Value": 42,
      "Quality": "Good",
      "Timestamp": "2026-09-22T08:30:00.000+00:00"
    }
  ]
}
```

Для обработки тега обязательны:

- `FullName`, совпадающий с одним из `InputTags`;
- непустой `Value` (значение `0`, `False` и пустая строка допустимы; `null` — нет);
- `Timestamp`, который можно разобрать как ISO 8601 либо как Unix timestamp.

Некорректный JSON, пакет без списка `Tags`, неизвестный тег или тег без корректных `Value`/`Timestamp` пропускается. Для последнего случая пишется предупреждение в лог.

## Конфигурация алгоритма

`SingleOutputAlgorithmConfig` принимает следующие поля:

| Поле | Назначение |
| --- | --- |
| `GroupId` | Consumer group основного Kafka-источника. Обязательное непустое значение. |
| `JobName` | Имя задачи Flink; также входит в имена операторов и состояния. |
| `AlgorithmID` | Идентификатор алгоритма. В текущем каркасе хранится в конфигурации, но при обработке не используется. |
| `RawTopic` | Основной входной Kafka-топик. |
| `ResultTopic` | Выходной топик стандартного sink с именем `default`. |
| `BootstrapServer` | Адрес Kafka, например `kafka:9092`. |
| `CheckpointIntervalMs` | Интервал checkpoint в миллисекундах, по умолчанию 30 секунд. |
| `CheckpointDir` | Каталог или URI checkpoint. Пустое значение отключает явную настройку каталога. |
| `SavepointDir` | Каталог или URI savepoint. Пустое значение отключает явную настройку каталога. |
| `StartFrom` | `earliest` начинает с ранних доступных сообщений, любое другое значение трактуется как `latest`. Применяется, когда для consumer group нет подходящей сохраненной позиции. |
| `ObjectId` | Идентификатор объекта в выходном пакете. Обязательное непустое значение. |
| `InputTags` | Все теги, необходимые функции расчета. Должен быть задан хотя бы один тег. |
| `TriggerTags` | Теги, при поступлении которых запускается расчет. Каждый из них должен входить в `InputTags`. |
| `StateKeyFields` | Поля входного пакета, разделяющие независимые состояния. По умолчанию: `SourceType`, `PlantId`, `ObjectId`. |

### Как работают `InputTags` и `TriggerTags`

Пусть заданы:

```python
InputTags=("temperature", "setpoint")
TriggerTags=("temperature",)
```

Тогда каркас сначала дождется хотя бы одного корректного значения обоих тегов. После этого расчет будет выполняться только при каждом новом `temperature`, используя последнее известное значение `setpoint`.

Если `TriggerTags=()`, триггерами становятся все `InputTags`. После первоначального заполнения кэша расчет запускается при поступлении каждого входного тега.

Важно: каркас не сравнивает время нового значения со временем сохраненного. Поздно пришедшее старое сообщение тоже заменит значение в кэше. Если порядок по времени важен, проверяйте его в `calculate_result` и храните нужную отметку времени в `main_state`.

### Как разделяется состояние

По умолчанию набор последних тегов и `main_state` разделяются по комбинации:

```text
[SourceType, PlantId, ObjectId]
```

Сообщения от разных линий или объектов поэтому не смешиваются. Поле `ObjectId` входного сообщения участвует в выборе состояния, а `CONFIG.ObjectId` записывается в выходной пакет.

Не удаляйте нужное поле из `StateKeyFields`, если его отсутствие может объединить данные разных объектов. Если указанного поля нет во входном JSON, его значением в ключе будет `null`.

## Контракт `calculate_result`

Функция должна принимать ровно три аргумента:

```python
def calculate_result(
    details_by_tag: dict,
    source_states: dict[str, dict],
    main_state: dict,
):
    ...
```

### `details_by_tag`

Содержит копии последних значений всех `InputTags`:

```python
temperature_detail = details_by_tag["temperature"]
value = temperature_detail["Value"]
timestamp = temperature_detail["Timestamp"]
```

Дополнительно каркас добавляет служебные значения:

```python
details_by_tag["source_type"]  # SourceType входного пакета
details_by_tag["plant_id"]    # PlantId входного пакета
details_by_tag["object_id"]   # CONFIG.ObjectId
```

Названия входных тегов `source_type`, `plant_id` и `object_id` использовать нельзя: служебные значения перезапишут их в словаре.

### `source_states`

Словарь состояний дополнительных Kafka-источников. Ключ — `Name` источника, значение — словарь, который ранее заполнял его callback. Если дополнительных источников нет, передается `{}`. В `calculate_result` считайте эти данные доступными только для чтения: изменения `source_states`, сделанные самой функцией расчета, каркас обратно в состояние Flink не сохраняет.

### `main_state`

Изменяемый словарь состояния бизнес-логики для текущей комбинации `StateKeyFields`. В нем можно хранить предыдущий результат, счетчики, историю и другие данные между вызовами:

```python
previous = main_state.get("previous_value")
main_state["previous_value"] = current_value
```

Все, что записывается в `main_state`, а также в состояние внутри callback дополнительного источника, должно сериализоваться стандартной функцией `json.dumps`. Используйте строки, числа, `bool`, `None`, списки и словари. Объекты `datetime`, `set`, пользовательские классы и другие несериализуемые значения сначала преобразуйте в строки или простые структуры.

Не храните бизнес-состояние в глобальных переменных: оно не входит в checkpoint и потеряется после перезапуска.

### Возвращаемое значение

Функция всегда возвращает пару:

```python
(результат, статус)
```

Поддерживаются два статуса:

- `AlgorithmStateStatus.Done` — расчет завершен;
- `AlgorithmStateStatus.NotAllNecessary` — данных пока недостаточно, выходное сообщение создавать не нужно.

Варианты возврата:

```python
# Один результат
return KafkaSinkMessage("default", result), AlgorithmStateStatus.Done

# Несколько результатов, возможно в разные sink
return [
    KafkaSinkMessage("default", first_result),
    KafkaSinkMessage("diagnostics", second_result),
], AlgorithmStateStatus.Done

# Корректно пропустить расчет
return None, AlgorithmStateStatus.NotAllNecessary

# Расчет успешен, но публиковать нечего
return None, AlgorithmStateStatus.Done
```

Не возвращайте «голый» словарь результата. Текущая реализация ожидает `KafkaSinkMessage` либо итерируемую коллекцию таких объектов. Неизвестный статус приводит к `RuntimeError`, а неизвестное имя sink — к ошибке задачи.

## Формат результата

Значение внутри `KafkaSinkMessage` должно иметь как минимум такой вид:

```python
result = {
    "SourceType": "PLC",
    "PlantId": "plant-1",
    "ObjectId": "line-1",
    "Timestamp": "2026-09-22T08:30:00.000+00:00",
    "Tags": [
        {
            "FullName": "Plant.Line1.Result",
            "Value": 45,
            "Quality": "Good",
            "Timestamp": "2026-09-22T08:30:00.000+00:00",
        }
    ],
}
```

Проще и безопаснее создавать его через `build_raw_result`.

Каркас использует `SourceType`, `Timestamp` и `Tags` результата. В финальном пакете он:

- берет `PlantId` из текущего входного сообщения;
- берет `ObjectId` из `CONFIG.ObjectId`;
- объединяет теги всех результатов, созданных при обработке одного входного Kafka-сообщения;
- заменяет верхнеуровневый `Timestamp` максимальным `Timestamp` среди выходных тегов;
- копирует `WindowStart` и `WindowFinish` из входного пакета, если они присутствуют.

Поэтому у каждого выходного тега должен быть корректный `Timestamp`. Если callback сработает несколько раз для одного входного пакета, результаты одного sink будут объединены в один Kafka-пакет.

## Полезные функции каркаса

### Числа

```python
integer_value = extract_int_value(detail)
float_value = to_float(detail.get("Value"))
```

`extract_int_value` возвращает `int` или выбрасывает `ValueError`. `to_float` понимает и точку, и запятую как десятичный разделитель; при ошибке возвращает `None`.

### Время

```python
parsed = parse_timestamp(detail["Timestamp"])
latest = get_max_timestamp_from_details([detail_a, detail_b])
iso_value = format_timestamp_millis(latest)
```

`parse_timestamp` возвращает timezone-aware `datetime` или `None`. ISO-строка без часового пояса считается временем UTC. `format_timestamp_millis` сохраняет часовой пояс и выводит миллисекунды.

### Результат

```python
result = build_raw_result(
    timestamp=iso_value,
    tag="Plant.Line1.Result",
    value=123.4,
    source_type=details_by_tag["source_type"],
    plant_id=details_by_tag["plant_id"],
    object_id=details_by_tag["object_id"],
    quality="Good",
)
```

## Дополнительный Kafka-источник

Дополнительный источник нужен, если расчет зависит от справочника, настроек или мастер-данных из другого топика. Его callback получает:

1. исходную строку Kafka-сообщения;
2. отдельное состояние этого источника;
3. общее внутреннее состояние основного потока.

Пример:

```python
import json

from utils.algorithm_skeleton_new import AdditionalKafkaSourceConfig


MASTER_SOURCE = "master_data"


def process_master_message(payload: str, source_state: dict, main_state: dict):
    del main_state
    message = json.loads(payload)
    source_state["coefficient"] = float(message["coefficient"])


additional_sources = [
    AdditionalKafkaSourceConfig(
        Name=MASTER_SOURCE,
        Topic="lab.algorithm.master-data",
        CallbackFunction=process_master_message,
        GroupId="adjusted-temperature-master-v1",  # необязательно
        StartFrom="earliest",
    )
]


def calculate_result(details_by_tag, source_states, main_state):
    coefficient = source_states.get(MASTER_SOURCE, {}).get("coefficient")
    if coefficient is None:
        return None, AlgorithmStateStatus.NotAllNecessary
    # Продолжение расчета...
```

Запуск:

```python
run_single_output_algorithm(
    CONFIG,
    calculate_result,
    additional_sources=additional_sources,
)
```

Если `GroupId` дополнительного источника не задан, используется `CONFIG.GroupId`. Имя источника должно быть непустым и уникальным; имя `__main_state__` зарезервировано.

Callback дополнительного источника сам не создает выходных сообщений. Он только обновляет состояние, которое будет доступно при следующем расчете основного потока. Исключения JSON-разбора и валидации в нем следует обрабатывать осознанно: необработанное исключение перезапустит задачу.

## Дополнительный Kafka-sink

Стандартный sink всегда называется `default` и пишет в `CONFIG.ResultTopic`. Другие топики объявляются явно:

```python
from utils.algorithm_skeleton_new import AdditionalKafkaSinkConfig


additional_sinks = [
    AdditionalKafkaSinkConfig(
        Name="diagnostics",
        Topic="lab.algorithm.diagnostics",
    )
]

run_single_output_algorithm(
    CONFIG,
    calculate_result,
    additional_sinks=additional_sinks,
)
```

Чтобы отправить сообщение туда:

```python
return [
    KafkaSinkMessage("default", normal_result),
    KafkaSinkMessage("diagnostics", diagnostic_result),
], AlgorithmStateStatus.Done
```

Имена sink должны быть непустыми и уникальными. Имя `default` уже занято стандартным выходом.

## Checkpoint, доставка и перезапуск

Каркас включает checkpoint Flink в режиме `EXACTLY_ONCE`, но Kafka sink настроен с гарантией `AT_LEAST_ONCE`. Практический вывод: при сбое и восстановлении одинаковый выходной пакет может быть опубликован повторно. Получатель должен уметь безопасно обрабатывать дубликаты либо результат должен быть идемпотентным.

Checkpoint сохраняет:

- последние значения `InputTags` для каждого ключа состояния;
- `main_state` каждого ключа;
- состояния дополнительных источников.

Внешние checkpoint сохраняются при отмене задачи (`RETAIN_ON_CANCELLATION`). Savepoint — отдельный управляемый снимок; указание `SavepointDir` само по себе не создает savepoint.

Не меняйте без необходимости `JobName` и автоматически назначенные `uid` операторов при обновлении существующей задачи: это может помешать восстановлению совместимого состояния.

## Частые ошибки

- `InputTags` пуст — задача завершится с `ValueError` при построении процессора.
- `TriggerTags` содержит тег не из `InputTags` — задача завершится с `ValueError`.
- `ObjectId` пуст — задача не запустится.
- В `main_state` записан `datetime`, `set` или другой несериализуемый объект — сохранение состояния завершится ошибкой.
- Функция вернула словарь вместо `KafkaSinkMessage` — каркас начнет перебирать ключи словаря и завершится ошибкой.
- У выходного тега нет корректного `Timestamp` — каркас не сможет вычислить время итогового пакета.
- Использовано имя несуществующего sink — будет выброшен `RuntimeError`.
- В callback выполняется долгий сетевой запрос — вся обработка останавливается на время запроса, потому что параллелизм равен единице.
- Входные события приходят не по времени — старое событие может заменить более новое; каркас сам это не предотвращает.
- Ожидается строгий `exactly once` на выходе — Kafka sink фактически настроен как `AT_LEAST_ONCE`, поэтому возможны повторы.

## Чек-лист перед публикацией

- Все `InputTags` указаны точно, с учетом регистра.
- Все `TriggerTags` входят в `InputTags`.
- Заданы `GroupId`, `JobName`, `ObjectId`, Kafka broker и топики.
- `calculate_result` всегда возвращает пару `(result, status)`.
- Каждый результат обернут в `KafkaSinkMessage`.
- Каждый выходной тег содержит корректный `Timestamp`.
- Состояние содержит только JSON-сериализуемые данные.
- Логика проверена на первом сообщении, неполных данных, неверном значении и сообщениях не по порядку.
- Получатель результата готов к возможным повторным сообщениям.
- Для обновления уже работающей задачи продумана совместимость checkpoint/savepoint.
