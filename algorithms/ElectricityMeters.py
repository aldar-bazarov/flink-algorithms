import math

from utils.algorithm_skeleton_new import (
    AlgorithmStateStatus,
    KafkaSinkMessage,
    SingleOutputAlgorithmConfig,
    build_raw_result,
    format_timestamp_millis,
    get_max_timestamp_from_details,
    run_single_output_algorithm,
    to_float,
)


class JobConfig:
    GroupId = "ElectricityMeters_Электросчётчики"
    JobName = "ElectricityMeters_Электросчётчики"
    AlgorithmID = "688f2143-1e97-4f5c-9449-7ac6f1fc7484"

    # Отдельное имя не дает старой настройке RawTopic менеджера заменить этот топик.
    ProcessingTopic = "lab.tag.processing"
    ResultTopic = "lab.tag.store"
    BootstrapServer = "10.178.9.232:9092,10.178.9.235:9092"
    MasterTopic = "lab.md.processing"

    # Теги в соответствии с настройками алгоритма
    OutputTagElectricity = "5109.UTIL.ELEC.RU04.TP03.8.A+Eres_NORM"
    InputTagElectricity = "5109.UTIL.ELEC.RU04.TP03.8.A+Eres"

    # Эти значения задаются для конкретной линии
    ObjectId = "e090beca-388b-49e8-96d4-96f98e2122a6"

    CheckpointIntervalMs = 30_000
    CheckpointDir = "/usr/share/flink/flink-checkpoints"
    SavepointDir = "/usr/share/flink/flink-savepoints"

    ConstPostfixNorm = "_NORM"


CONFIG = SingleOutputAlgorithmConfig(
    JobConfig.GroupId,
    JobConfig.JobName,
    JobConfig.AlgorithmID,
    JobConfig.ProcessingTopic,
    JobConfig.ResultTopic,
    JobConfig.BootstrapServer,
    JobConfig.CheckpointIntervalMs,
    JobConfig.CheckpointDir,
    JobConfig.SavepointDir,
    StartFrom="earliest",
    ObjectId=JobConfig.ObjectId,
    SourceType="HISTORIAN_POWER_ELECTRICITY_METER",
    # Каждое новое показание сравнивается с предыдущим для этого потока.
    InputTags=(JobConfig.InputTagElectricity,),
)


def calculate_result(
    details_by_tag: dict[str, dict | str],
    source_states: dict[str, dict],
    main_state: dict,
):
    detail = details_by_tag.get(JobConfig.InputTagElectricity)
    source_type = details_by_tag.get("source_type")
    plant_id = details_by_tag.get("plant_id")

    if not isinstance(detail, dict) or not isinstance(source_type, str) or not isinstance(plant_id, str):
        return None, AlgorithmStateStatus.NotAllNecessary

    electricity = to_float(detail.get("Value"))

    if electricity is None or not math.isfinite(electricity):
        return None, AlgorithmStateStatus.NotAllNecessary

    previous_value = to_float(main_state.get("previous_value"))
    main_state["previous_value"] = electricity
    if previous_value is None or not math.isfinite(previous_value):
        return None, AlgorithmStateStatus.Done

    timestamp = format_timestamp_millis(get_max_timestamp_from_details([detail]))

    result = build_raw_result(
        timestamp=timestamp,
        tag=JobConfig.OutputTagElectricity,
        value=electricity - previous_value,
        source_type=source_type + JobConfig.ConstPostfixNorm,
        plant_id=plant_id,
        object_id=JobConfig.ObjectId,
        quality=detail.get("Quality", ""),
    )

    return (
        KafkaSinkMessage(SinkName="default", Value=result),
        AlgorithmStateStatus.Done,
    )


if __name__ == "__main__":
    run_single_output_algorithm(CONFIG, calculate_result)
