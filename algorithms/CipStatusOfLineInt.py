import json
import logging
import sys
from math import fabs
from pathlib import Path

from utils.algorithm_skeleton import (
    AdditionalKafkaSinkConfig,
    KafkaSinkMessage,
    AdditionalKafkaSourceConfig,
    SingleOutputAlgorithmConfig,
    run_single_output_algorithm, to_float, extract_float_value, extract_raw_value, DEFAULT_KAFKA_SINK_NAME,
    OpenSearchConfig,
    AlgorithmStateStatus, build_raw_result, parse_timestamp, get_max_timestamp_from_details,
    format_timestamp_millis, extract_int_value,
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

    # Эти значения задаются для конкретной линии
    LineID = "<<StreamAlgorithmInstance.ObjectID>>"
    LineObject = "1"

    CheckpointIntervalMs = 30_000
    CheckpointDir = "/usr/share/flink/flink-checkpoints"
    SavepointDir = "/usr/share/flink/flink-savepoints"

    MasterAlgorithmIDs = "AlgorithmID"
    MasterTimestamp = "Timestamp"
    MasterObjectID = "ObjectID"
    MasterParams = "Params"
    MasterParamName = "Name"
    MasterParamValue = "Value"
    Hysteresis = "Hysteresis"
    SpeedOfProduction = "SpeedOfProduction"
    ConstPostfixNorm = "_NORM"


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
    StartFrom='earliest'
)

# Пустые поля заполняются из менеджера
opensearch_config = OpenSearchConfig(
    BaseUrl="",
    DefaultIndex="flink-diagnostics",
    Username="",
    Password="",
)

MASTER_SOURCE_NAME = "master_data"

MODE_STOP = 0
MODE_WORK = 1
MODE_CIP = 2
MODE_LOW_SPEED = 4

MAIN_HISTORY_WINDOW_SECONDS = 4 * 60 * 60
PREVIOUS_VALUE_MIN_AGE_SECONDS = 30
SPEED_OF_PRODUCTION_DELIMITER = 120


def normalize_identifier(value) -> str:
    return str(value or "").strip().casefold()


def get_line_id_key() -> str:
    return normalize_identifier(JobConfig.LineID or JobConfig.LineObject)


def get_line_object_key() -> str:
    return normalize_identifier(JobConfig.LineObject or JobConfig.LineID)


def is_master_message_for_current_algorithm(payload: dict) -> bool:
    algorithm_id = normalize_identifier(JobConfig.AlgorithmID)
    algorithm_ids = payload.get(JobConfig.MasterAlgorithmIDs)

    if not algorithm_id or not isinstance(algorithm_ids, list):
        return False

    return any(normalize_identifier(item) == algorithm_id for item in algorithm_ids)


def extract_master_params(payload: dict) -> dict[str, object]:
    raw_params = payload.get(JobConfig.MasterParams)
    if not isinstance(raw_params, list):
        return {}

    params = {}
    for item in raw_params:
        if not isinstance(item, dict):
            continue

        name = str(item.get(JobConfig.MasterParamName) or "").strip().casefold()
        if name:
            params[name] = item.get(JobConfig.MasterParamValue)

    return params


def get_history_bucket(state: dict, key: str) -> list[dict]:
    bucket = state.get(key)
    if isinstance(bucket, list):
        return bucket
    bucket = []
    state[key] = bucket
    return bucket


def prune_history(history: list[dict], cutoff_ts: float) -> None:
    first_valid_index = 0
    while first_valid_index < len(history):
        item_ts = float(history[first_valid_index].get("ts", 0.0))
        if item_ts >= cutoff_ts:
            break
        first_valid_index += 1

    if first_valid_index > 0:
        del history[:first_valid_index]


def append_history_record(history: list[dict], record: dict, max_age_seconds: int) -> None:
    record_ts = float(record["ts"])
    if not history or float(history[-1].get("ts", 0.0)) <= record_ts:
        history.append(record)
    else:
        history.append(record)
        history.sort(key=lambda item: float(item.get("ts", 0.0)))

    prune_history(history, record_ts - max_age_seconds)


def find_previous_counter_record(history: list[dict], current_ts: float) -> dict | None:
    max_allowed_ts = current_ts - PREVIOUS_VALUE_MIN_AGE_SECONDS
    best_record = None
    best_ts = float("-inf")

    for item in history:
        logger.info("ITEM: %s", item)
        item_ts = float(item.get("ts", 0.0))

        if item_ts > max_allowed_ts:
            continue

        if item_ts > best_ts:
            best_record = item
            best_ts = item_ts

    return best_record


def get_master_lines_state(source_state: dict) -> dict[str, list[dict]]:
    lines = source_state.get("lines")
    if isinstance(lines, dict):
        return lines
    lines = {}
    source_state["lines"] = lines
    return lines


def find_master_record(source_states: dict[str, dict], current_ts: float) -> dict | None:
    line_id = get_line_id_key()
    if not line_id:
        return None

    source_state = source_states.get(MASTER_SOURCE_NAME)
    if not isinstance(source_state, dict):
        return None

    line_history = get_master_lines_state(source_state).get(line_id)
    if not isinstance(line_history, list):
        return None

    best_record = None
    best_ts = float("-inf")

    for item in line_history:
        item_ts = float(item.get("ts", 0.0))
        if item_ts > current_ts:
            continue
        if best_record is None or item_ts > best_ts:
            best_record = item
            best_ts = item_ts

    return best_record


def is_low_speed(delta: float, master_record: dict | None) -> bool:
    if not master_record:
        return False

    speed_of_production = to_float(master_record.get("speed_of_production"))
    hysteresis = to_float(master_record.get("hysteresis"))

    # При отсутствии обоих параметров линия считается работающей.
    if speed_of_production is None and hysteresis is None:
        return False

    if speed_of_production is None:
        return False

    hysteresis = hysteresis or 0.0
    min_threshold = max(0.0, 1.0 - (hysteresis / 100.0))
    speed_per_minute = speed_of_production / SPEED_OF_PRODUCTION_DELIMITER
    expected_delta = speed_per_minute * min_threshold

    return fabs(delta) < expected_delta


def process_master_message(value: str, source_state: dict, main_state: dict) -> None:
    """
Обработка элементов в топике с мастер-данными и фиксация элемента в стейте
    :param value: Значение из топика
    :param source_state: Стейт для хранения элементов
    :param main_state:
    :return:
    """
    del main_state

    try:
        payload = json.loads(value)
    except Exception:
        return

    if not isinstance(payload, dict):
        return

    if not is_master_message_for_current_algorithm(payload):
        return

    # В контракте мастер-данных ObjectID содержит Lines.ID.
    object_id = normalize_identifier(payload.get(JobConfig.MasterObjectID))
    timestamp = parse_timestamp(payload.get(JobConfig.MasterTimestamp))
    params = extract_master_params(payload)
    speed_of_production = to_float(params.get(JobConfig.SpeedOfProduction.casefold()))
    hysteresis = to_float(params.get(JobConfig.Hysteresis.casefold()))

    if not object_id or timestamp is None:
        return

    timestamp_iso = format_timestamp_millis(timestamp)

    lines = get_master_lines_state(source_state)
    line_history = lines.get(object_id)
    if not isinstance(line_history, list):
        line_history = []
        lines[object_id] = line_history

    append_history_record(
        line_history,
        {
            "ts": timestamp.timestamp(),
            "timestamp": timestamp_iso,
            "speed_of_production": speed_of_production,
            "hysteresis": hysteresis,
        },
        MAIN_HISTORY_WINDOW_SECONDS,
    )


def calculate_result(
    details_by_tag: dict[str, dict | str],
    source_states: dict[str, dict],
    main_state: dict,
):
    counter_detail = details_by_tag[JobConfig.CounterTag]
    cip_active_detail = details_by_tag[JobConfig.CIPActiveTagIn]

    try:
        current_timestamp = get_max_timestamp_from_details([counter_detail, cip_active_detail])
        counter_value = extract_int_value(counter_detail)
        cip_active_value = extract_int_value(cip_active_detail)
    except ValueError:
        return None, AlgorithmStateStatus.NotAllNecessary

    current_ts = current_timestamp.timestamp()
    current_timestamp_iso = format_timestamp_millis(current_timestamp)

    history = get_history_bucket(main_state, "history")
    prune_history(history, current_ts - MAIN_HISTORY_WINDOW_SECONDS)

    previous_record = find_previous_counter_record(history, current_ts)
    if previous_record is None:
        delta = counter_value
    else:
        delta = counter_value - float(previous_record.get("counter", 0.0))

    logger.info("DELTA: %f", abs(delta))
    if cip_active_value == 1:
        mode = MODE_CIP
    elif abs(delta) <= sys.float_info.epsilon:
        logger.info("CIP-ACTIVE: %f", abs(delta))
        mode = MODE_STOP
    elif is_low_speed(delta, find_master_record(source_states, current_ts)):
        mode = MODE_LOW_SPEED
    else:
        mode = MODE_WORK

    append_history_record(
        history,
        {
            "ts": current_ts,
            "timestamp": current_timestamp_iso,
            "counter": counter_value,
            "cip_active": cip_active_value,
            "mode": mode,
        },
        MAIN_HISTORY_WINDOW_SECONDS,
    )

    res_raw = build_raw_result(current_timestamp_iso, JobConfig.OutputTag, mode, details_by_tag["source_type"] + JobConfig.ConstPostfixNorm, details_by_tag["plant_id"])

    return [
        # KafkaSinkMessage(SinkName="RawTopic", Value=res_raw),
        KafkaSinkMessage(SinkName=DEFAULT_KAFKA_SINK_NAME, Value=res_raw),
    ], AlgorithmStateStatus.Done



if __name__ == "__main__":
    additional_sources = None
    if JobConfig.MasterTopic:
        additional_sources = [
            AdditionalKafkaSourceConfig(
                Name=MASTER_SOURCE_NAME,
                Topic=JobConfig.MasterTopic,
                CallbackFunction=process_master_message,
                StartFrom='earliest',
            )
        ]

    script_dir = Path(__file__).resolve().parent
    default_driver_jars = None

    additional_sinks = None

    run_single_output_algorithm(
        CONFIG,
        calculate_result,
        additional_sources=additional_sources,
        additional_sinks=additional_sinks,
        script_dir=script_dir,
        default_driver_jars=default_driver_jars,
        opensearch_config=opensearch_config,
    )
