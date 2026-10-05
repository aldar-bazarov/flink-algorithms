import logging
from pathlib import Path

from utils.algorithm_skeleton import build_raw_result, extract_int_value, get_max_timestamp_from_details, \
    format_timestamp_millis, OpenSearchConfig
from utils.algorithm_skeleton import (
    AdditionalKafkaSinkConfig,
    KafkaSinkMessage,
    SingleOutputAlgorithmConfig,
    run_single_output_algorithm,
    AlgorithmStateStatus,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class JobConfig:
    GroupId = "<<StreamAlgorithm.Code>>_<<StreamAlgorithmInstance.Name>>"
    JobName = "<<StreamAlgorithm.Code>>_<<StreamAlgorithmInstance.Name>>"
    AlgorithmID = "<<StreamAlgorithmInstance.ID>>"

    # FlinkScriptManager подставляет эти значения через PythonConfigInjector
    RawTopic = "lab.tag.preprocessed"
    ResultTopic = "lab.tag.store"
    BootstrapServer = "10.178.9.232:9092"
    MasterTopic = "lab.md.processing"

    # Теги в соответствии с настройками алгоритма
    <<TAGS>>

    CheckpointIntervalMs = 30_000
    CheckpointDir = "/usr/share/flink/flink-checkpoints"
    SavepointDir = "/usr/share/flink/flink-savepoints"

CONFIG = SingleOutputAlgorithmConfig(
    JobConfig.GroupId,
    JobConfig.JobName,
    JobConfig.AlgorithmID,
    JobConfig.RawTopic,
    JobConfig.ResultTopic,
    JobConfig.BootstrapServer,
    JobConfig.CheckpointIntervalMs,
    JobConfig.CheckpointDir,
    JobConfig.SavepointDir,
    StartFrom='latest'
)

# Пустые поля заполняются из менеджера
opensearch_config = OpenSearchConfig(
    BaseUrl="",
    DefaultIndex="flink-diagnostics",
    Username="",
    Password="",
)

def calculate_result(
    details_by_tag: dict[str, dict | str],
    source_states: dict[str, dict],
    main_state: dict,
):
    del source_states
    del main_state

    cip_active_detail = details_by_tag[JobConfig.CIPIN]

    if cip_active_detail is None:
        return None, AlgorithmStateStatus.NotAllNecessary

    try:
        cip_active_value = extract_int_value(cip_active_detail)
        current_timestamp = get_max_timestamp_from_details([cip_active_detail])
    except ValueError:
        return None, AlgorithmStateStatus.NotAllNecessary

    current_timestamp_iso = format_timestamp_millis(current_timestamp)

    mode = 0
    if cip_active_value > 0:
        mode = 1

    res_raw = build_raw_result(current_timestamp_iso, JobConfig.CIPOUT, mode, details_by_tag["source_type"], details_by_tag["plant_id"])

    return [
        KafkaSinkMessage(SinkName="RawTopic", Value=res_raw),
    ], AlgorithmStateStatus.Done


if __name__ == "__main__":
    additional_sources = None

    additional_sinks = [
        AdditionalKafkaSinkConfig(Name="RawTopic", Topic="lab.tag.processing"),
    ]

    script_dir = Path(__file__).resolve().parent
    default_driver_jars = None


    run_single_output_algorithm(
        CONFIG,
        calculate_result,
        additional_sources=additional_sources,
        additional_sinks=additional_sinks,
        script_dir=script_dir,
        default_driver_jars=default_driver_jars,
        opensearch_config=opensearch_config,
    )
