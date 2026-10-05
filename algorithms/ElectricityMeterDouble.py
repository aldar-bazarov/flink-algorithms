"""lab.tag.processing -> удвоение Tags[*].Value -> lab.tag.store.

Обрабатываются только события HISTORIAN_POWER_ELECTRICITY_METER.
Фреймворк используется для создания Kafka source/sink, без изменения его файла.
Его run_single_output_algorithm рассчитан на другой формат (algorithm_state).
Kafka key и headers не переносятся используемыми source/sink фреймворка.
"""

import json
import math
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


class JobConfig:
    GroupId = os.getenv("ELECTRICITY_GROUP_ID", "electricity-meter-double")
    JobName = "ElectricityMeterDouble"
    RawTopic = "lab.tag.processing"
    ResultTopic = "lab.tag.store"
    BootstrapServer = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.178.9.232:9092")
    SourceType = "HISTORIAN_POWER_ELECTRICITY_METER"
    StartFrom = "latest"
    CheckpointIntervalMs = 30_000
    CheckpointDir = "/usr/share/flink/flink-checkpoints"
    SavepointDir = "/usr/share/flink/flink-savepoints"


def calculate_result(event: dict) -> dict | None:
    """Изменяет все значения подходящего события, сохраняя остальные поля.

    Другие SourceType пропускаются. Некорректное подходящее событие вызывает
    ValueError: частично преобразованный пакет не отправляется в Kafka.
    Исходный словарь и его теги не изменяются.
    """
    if not isinstance(event, dict):
        raise ValueError("Kafka value должен быть JSON-объектом")
    if event.get("SourceType") != JobConfig.SourceType:
        return None

    tags = event.get("Tags")
    if not isinstance(tags, list):
        raise ValueError("Tags должен быть массивом")

    result_tags = []
    for index, tag in enumerate(tags):
        if not isinstance(tag, dict):
            raise ValueError(f"Tags[{index}] должен быть объектом")
        value = tag.get("Value")
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise ValueError(f"Tags[{index}].Value должен быть числом")
        try:
            number = Decimal(str(value))
            if not number.is_finite():
                raise ValueError(f"Tags[{index}].Value должен быть конечным числом")
            # Округление половины от нуля: 1.005 -> 1.01, -1.005 -> -1.01.
            doubled = float((number * 2).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            if not math.isfinite(doubled):
                raise ValueError(f"Tags[{index}].Value выходит за числовой диапазон")
        except (InvalidOperation, OverflowError) as exc:
            raise ValueError(f"Невозможно преобразовать Tags[{index}].Value") from exc
        result_tags.append({**tag, "Value": doubled})

    return {**event, "Tags": result_tags}


def process_event(raw_value: str):
    # Десятичный разбор исключает двоичную погрешность до округления Value.
    event = json.loads(raw_value, parse_float=Decimal)
    result = calculate_result(event)
    if result is not None:
        # Прочие числовые поля остаются JSON-числами.
        yield json.dumps(result, ensure_ascii=False, allow_nan=False, default=float)


def main():
    from pyflink.common import Configuration, Types, WatermarkStrategy
    from pyflink.datastream import (
        CheckpointingMode,
        ExternalizedCheckpointRetention,
        StreamExecutionEnvironment,
    )
    from utils.algorithm_skeleton_new import (
        build_kafka_sink,
        build_kafka_source,
        normalize_fs_uri,
    )

    config = Configuration()
    config.set_string("flink.partition-discovery.interval-millis", "30000")
    config.set_string("state.checkpoints.dir", normalize_fs_uri(JobConfig.CheckpointDir))
    config.set_string("state.savepoints.dir", normalize_fs_uri(JobConfig.SavepointDir))
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.enable_checkpointing(JobConfig.CheckpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    checkpoint = env.get_checkpoint_config()
    checkpoint.set_min_pause_between_checkpoints(10_000)
    checkpoint.set_checkpoint_timeout(120_000)
    checkpoint.set_max_concurrent_checkpoints(1)
    checkpoint.set_tolerable_checkpoint_failure_number(3)
    checkpoint.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )

    source = build_kafka_source(
        bootstrap_servers=JobConfig.BootstrapServer,
        topic=JobConfig.RawTopic,
        group_id=JobConfig.GroupId,
        start_from=JobConfig.StartFrom,
    )
    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "electricity-meter-source"
    ).uid("electricity-meter-source-v1")
    processed = stream.flat_map(process_event, output_type=Types.STRING()).uid(
        "electricity-meter-double-v1"
    )
    sink = build_kafka_sink(
        bootstrap_servers=JobConfig.BootstrapServer,
        topic=JobConfig.ResultTopic,
    )
    processed.sink_to(sink).uid("electricity-meter-sink-v1")
    env.execute(JobConfig.JobName)


if __name__ == "__main__":
    main()
