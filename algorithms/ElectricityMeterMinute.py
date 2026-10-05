"""Шаблон для UI: минутная дельта накопленного показания энергосчётчика.

Перед запуском платформа заменяет <<...>>, включая блок <<TAGS>>.
В настройках тегов нужны InputTag (показание) и OutputTag (минутный расход).
"""

from datetime import timedelta
from decimal import Decimal

from utils.algorithm_skeleton_new import (
    AlgorithmStateStatus,
    DEFAULT_KAFKA_SINK_NAME,
    KafkaSinkMessage,
    SingleOutputAlgorithmConfig,
    build_raw_result,
    format_timestamp_millis,
    parse_timestamp,
    run_single_output_algorithm,
)


class JobConfig:
    GroupId = "<<StreamAlgorithm.Code>>_<<StreamAlgorithmInstance.Name>>"
    JobName = "<<StreamAlgorithm.Code>>_<<StreamAlgorithmInstance.Name>>"
    AlgorithmID = "<<StreamAlgorithmInstance.ID>>"
    ProcessingTopic = "<<SystemNode.KafkaPrefix>>.tag.processing"
    ResultTopic = "<<SystemNode.KafkaPrefix>>.tag.store"
    BootstrapServer = "<<SystemNode.KafkaServer>>"
    ObjectId = "<<StreamAlgorithmInstance.ObjectID>>"

    # Платформа подставляет определения InputTag и OutputTag.
    <<TAGS>>


CONFIG = SingleOutputAlgorithmConfig(
    GroupId=JobConfig.GroupId,
    JobName=JobConfig.JobName,
    AlgorithmID=JobConfig.AlgorithmID,
    RawTopic=JobConfig.ProcessingTopic,
    ResultTopic=JobConfig.ResultTopic,
    BootstrapServer=JobConfig.BootstrapServer,
    ObjectId=JobConfig.ObjectId,
    InputTags=(JobConfig.InputTag,),
    TriggerTags=(JobConfig.InputTag,),
    StartFrom="latest",
)


def calculate_result(details_by_tag, source_states, main_state):
    """Каркас вызывает эту функцию при поступлении показания счётчика."""
    reading = details_by_tag[JobConfig.InputTag]
    timestamp = parse_timestamp(reading["Timestamp"])
    if timestamp is None:
        return None, AlgorithmStateStatus.Done

    # 12:00:30 пропускаем: базой для 12:01:00 остаётся 12:00:00.
    if timestamp.second != 0 or timestamp.microsecond != 0:
        return None, AlgorithmStateStatus.Done

    value = Decimal(str(reading["Value"]))
    if not value.is_finite() or value < 0:
        raise ValueError("Показание должно быть конечным неотрицательным числом")

    previous = main_state.get("minute_boundary")
    previous_time = parse_timestamp(previous["Timestamp"]) if previous else None

    # Повторы и старые показания не меняют базу и не создают новый результат.
    if previous_time is not None and timestamp <= previous_time:
        return None, AlgorithmStateStatus.Done

    # Каркас сохранит этот словарь в состоянии Flink, даже без результата.
    # Decimal записываем строкой для совместимости с JSON-сериализацией.
    main_state["minute_boundary"] = {
        "Timestamp": format_timestamp_millis(timestamp),
        "Value": str(value),
    }

    # Первое показание только запоминаем. Пропущенные минуты не оцениваем.
    if previous_time is None or timestamp - previous_time != timedelta(minutes=1):
        return None, AlgorithmStateStatus.Done

    delta = value - Decimal(previous["Value"])
    if delta < 0:
        # Счётчик сбросился: новая база уже сохранена, расход не выдаём.
        return None, AlgorithmStateStatus.Done

    result_value = int(delta) if delta == delta.to_integral_value() else float(delta)
    result = build_raw_result(
        timestamp=format_timestamp_millis(timestamp),
        tag=JobConfig.OutputTag,
        value=result_value,
        source_type=details_by_tag["source_type"],
        plant_id=details_by_tag["plant_id"],
        object_id=details_by_tag["object_id"],
        quality=reading.get("Quality", ""),
    )
    return KafkaSinkMessage(DEFAULT_KAFKA_SINK_NAME, result), AlgorithmStateStatus.Done


if __name__ == "__main__":
    # Запуск обработки опубликованной платформой джобы; это не публикация.
    run_single_output_algorithm(CONFIG, calculate_result)
