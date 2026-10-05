from __future__ import annotations

import json
import logging
import sys
from math import fabs
from datetime import datetime, timezone

from utils.algorithm_skeleton_new import (
    AdditionalKafkaSourceConfig,
    AlgorithmStateStatus,
    DEFAULT_KAFKA_SINK_NAME,
    KafkaSinkMessage,
    SingleOutputAlgorithmConfig,
    build_raw_result,
    extract_int_value,
    format_timestamp_millis,
    get_max_timestamp_from_details,
    parse_timestamp,
    run_single_output_algorithm,
    to_float,
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


    # Отдельное имя не дает старой настройке RawTopic менеджера заменить этот топик.
    ProcessingTopic = "<<SystemNode.KafkaPrefix>>.tag.processing"
    ResultTopic = "<<SystemNode.KafkaPrefix>>.tag.store"
    BootstrapServer = "<<SystemNode.KafkaServer>>"
    MasterTopic = "<<SystemNode.KafkaPrefix>>.md.processing"

    
    # Теги в соответствии с настройками алгоритма
    <<TAGS>>
 

   # Эти значения задаются для конкретной линии
    LineID = "<<StreamAlgorithmInstance.ObjectID>>"
    LineObject = "1"
    ObjectId = "<<StreamAlgorithmInstance.ObjectID>>"
  

    CheckpointIntervalMs = 30_000
    CheckpointDir = "/usr/share/flink/flink-checkpoints"
    SavepointDir = "/usr/share/flink/flink-savepoints"

    MasterAlgorithmIDs = "AlgorithmID"
    MasterTimestamp = "Timestamp"
    MasterObjectID = "ObjectID"
    MasterParams = "Params"
    MasterParamName = "Name"
    MasterParamValue = "Value"
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
    StartFrom='latest',
    ObjectId=JobConfig.ObjectId,
    # Каждый входной тег запускает расчет, когда известны оба значения.
    InputTags=(JobConfig.InputTag,),
)

def calculate_result(
    details_by_tag: dict[str, dict | str],
    source_states: dict[str, dict],
    main_state: dict,
):
    aggregat_details = details_by_tag[JobConfig.InputTag]

    current_timestamp = datetime.now(timezone.utc)
    current_timestamp_iso = format_timestamp_millis(current_timestamp)
    
    mode = aggregat_details.get("Value", "")
    res_raw = build_raw_result(
        current_timestamp_iso,
        JobConfig.OutputTag,
        mode,
        details_by_tag["source_type"] + JobConfig.ConstPostfixNorm,
        details_by_tag["plant_id"],
        object_id=JobConfig.ObjectId,
        quality=aggregat_details.get("Quality", ""),
    )

    return [
        KafkaSinkMessage(SinkName=DEFAULT_KAFKA_SINK_NAME, Value=res_raw),
    ], AlgorithmStateStatus.Done


def main() -> None:
    additional_sources = None
    run_single_output_algorithm(
        CONFIG,
        calculate_result,
        additional_sources=additional_sources,
    )


if __name__ == "__main__":
    main()
