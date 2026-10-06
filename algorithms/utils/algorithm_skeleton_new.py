"""Последовательная обработка пакетов Tags из Kafka без оркестратора и БД."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from pyflink.common import Configuration, SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, ExternalizedCheckpointRetention, StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedCoProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor


def normalize_fs_uri(path_or_uri: str) -> str:
    """
    Нормализация пути к файлу
    :param path_or_uri:
    :return:
    """
    raw = (path_or_uri or "").strip()
    if not raw:
        return raw
    if "://" in raw:
        return raw
    return Path(raw).expanduser().resolve().as_uri()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

DEFAULT_KAFKA_SINK_NAME = "default"
MAIN_STATE_NAME = "__main_state__"
STATE_STREAM_KEY = "__singleton__"


@dataclass(frozen=True)
class AdditionalKafkaSourceConfig:
    Name: str
    Topic: str
    CallbackFunction: Callable[[str, dict, dict], None]
    GroupId: Optional[str] = None
    StartFrom: str = "latest"


@dataclass(frozen=True)
class AdditionalKafkaSinkConfig:
    Name: str
    Topic: str


@dataclass(frozen=True)
class KafkaSinkMessage:
    SinkName: str
    Value: Any


@dataclass
class SingleOutputAlgorithmConfig:
    GroupId: str
    JobName: str
    AlgorithmID: str
    RawTopic: str = "lab.tag.processing"
    ResultTopic: str = "lab.tag.store"
    BootstrapServer: str = "172.23.201.125:9092"
    CheckpointIntervalMs: int = 30_000
    CheckpointDir: str = "/usr/share/flink/flink-checkpoints"
    SavepointDir: str = "/usr/share/flink/flink-savepoints"
    StartFrom: str = "latest"
    SourceType: str = ""
    ObjectId: str = ""
    InputTags: tuple[str, ...] = ()
    # Пустой список означает расчет при каждом входном теге.
    TriggerTags: tuple[str, ...] = ()
    StateKeyFields: tuple[str, ...] = ("SourceType", "PlantId", "ObjectId")


class AlgorithmStateStatus:
    Done = 2
    NotAllNecessary = 4


def build_raw_result(
    timestamp: str,
    tag: str,
    value: Any,
    source_type: str = "",
    plant_id: str = "",
    object_id: str = "",
    quality: Any = "",
) -> dict:
    return {
        "SourceType": source_type,
        "PlantId": plant_id,
        "ObjectId": object_id,
        "Timestamp": timestamp,
        "Tags": [
            {
                "FullName": tag,
                "Value": value,
                "Quality": quality,
                "Timestamp": timestamp,
            }
        ],
    }


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def extract_int_value(detail: dict) -> int:
    value = detail.get("Value")
    try:
        if value is not None:
            return int(value)
    except (TypeError, ValueError, OverflowError):
        pass
    raise ValueError("У тега нет целочисленного значения")


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def format_timestamp_millis(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat(timespec="milliseconds")


def parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            return None

    raw_value = str(value or "").strip()
    if not raw_value:
        return None

    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def extract_detail_timestamp(detail: dict) -> datetime:
    parsed = parse_timestamp(detail.get("Timestamp"))
    if parsed is None:
        raise ValueError("У тега нет метки времени")
    return parsed


def get_max_timestamp_from_details(details: list[dict]) -> datetime:
    return max(extract_detail_timestamp(detail) for detail in details)


def build_kafka_source(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    start_from: str = "latest",
) -> KafkaSource:
    mode = (start_from or "latest").strip().lower()
    offsets = KafkaOffsetsInitializer.earliest() if mode == "earliest" else KafkaOffsetsInitializer.latest()

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .set_properties(
            {
                "api.version.request": "true",
                "security.protocol": "PLAINTEXT",
            }
        )
        .build()
    )


def build_state_descriptor(job_name: str) -> MapStateDescriptor:
    return MapStateDescriptor(
        f"{job_name}-source-states",
        Types.STRING(),
        Types.STRING(),
    )


def build_kafka_sink(
    bootstrap_servers: str,
    topic: str,
) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def load_dict_state(state, key: str) -> dict:
    raw_value = state.get(key)
    if raw_value is None:
        return {}
    try:
        loaded = json.loads(raw_value)
    except Exception:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def save_dict_state(state, key: str, value: dict):
    state.put(key, json_dumps(value))


class BaseAlgorithmProcessor:
    """Хранит последние входы и вызывает расчет сразу после каждого триггера.

    Порядок Tags не меняется; одинаковые имена и времена не удаляются.
    Кэш и история разделены по StateKeyFields входного пакета.
    """

    def __init__(
        self,
        algorithm_config: SingleOutputAlgorithmConfig,
        callback_function: Callable,
        allowed_sink_names: set[str],
    ):
        self._algorithm_config = algorithm_config
        self._callback_function = callback_function
        self._allowed_sink_names = set(allowed_sink_names)
        self._input_tags = set(algorithm_config.InputTags)
        self._trigger_tags = set(algorithm_config.TriggerTags) or self._input_tags
        self._source_type = str(algorithm_config.SourceType or "").strip()
        self._object_id = str(algorithm_config.ObjectId or "").strip()
        if not self._input_tags:
            raise ValueError("Не заданы входные теги алгоритма (InputTags)")
        if not self._trigger_tags <= self._input_tags:
            raise ValueError("TriggerTags должны входить в InputTags")
        if not self._source_type:
            raise ValueError("Не задан SourceType входных сообщений алгоритма")
        if not self._object_id:
            raise ValueError("Не задан ObjectId входных сообщений и результата алгоритма")

    def process_main(self, value: str, source_states: dict, main_state: dict):
        try:
            event = json.loads(value)
        except (TypeError, ValueError):
            return
        if not isinstance(event, dict):
            return

        event_source_type = str(event.get("SourceType") or "").strip()
        event_object_id = str(event.get("ObjectId") or "").strip()
        if event_source_type != self._source_type or event_object_id != self._object_id:
            return

        tags = event.get("Tags")
        if not isinstance(tags, list):
            return

        stream_key = json_dumps([event.get(field) for field in self._algorithm_config.StateKeyFields])
        # Не создаем состояние для пакетов, в которых нет нужных тегов.
        stream_state = None
        batches = {}
        for tag in tags:
            if not isinstance(tag, dict):
                continue
            tag_name = tag.get("FullName")
            if not isinstance(tag_name, str) or tag_name not in self._input_tags:
                continue
            if parse_timestamp(tag.get("Timestamp")) is None or tag.get("Value") is None:
                logger.warning("Пропущен тег %s без Value или корректного Timestamp", tag_name)
                continue

            if stream_state is None:
                stream_state = main_state.setdefault("streams", {}).setdefault(stream_key, {})
            latest_tags = stream_state.setdefault("latest_tags", {})
            latest_tags[tag_name] = dict(tag)
            if tag_name not in self._trigger_tags or not self._input_tags <= latest_tags.keys():
                continue

            details = {name: dict(latest_tags[name]) for name in self._input_tags}
            details.update(
                source_type=str(event.get("SourceType") or ""),
                plant_id=event.get("PlantId", ""),
                object_id=self._algorithm_config.ObjectId,
            )
            result, status = self._callback_function(
                details,
                source_states,
                stream_state.setdefault("algorithm", {}),
            )
            if status == AlgorithmStateStatus.NotAllNecessary:
                continue
            if status != AlgorithmStateStatus.Done:
                raise RuntimeError(f"Неожиданный статус алгоритма: {status}")
            if result is None:
                continue
            messages = [result] if isinstance(result, KafkaSinkMessage) else result
            for message in messages:
                sink_name = message.SinkName or DEFAULT_KAFKA_SINK_NAME
                if sink_name not in self._allowed_sink_names:
                    raise RuntimeError(f"Kafka sink '{sink_name}' не задан в алгоритме")
                output = message.Value
                batch = batches.get(sink_name)
                if batch is None:
                    batch = {
                        "SourceType": output["SourceType"],
                        "PlantId": event.get("PlantId", ""),
                        "ObjectId": self._algorithm_config.ObjectId,
                        "Timestamp": output["Timestamp"],
                        "Tags": [],
                    }
                    for field in ("WindowStart", "WindowFinish"):
                        if field in event:
                            batch[field] = event[field]
                    batches[sink_name] = batch
                batch["Tags"].extend(output["Tags"])

        for sink_name, batch in batches.items():
            if batch["Tags"]:
                batch["Timestamp"] = format_timestamp_millis(
                    get_max_timestamp_from_details(batch["Tags"]),
                )
                yield sink_name, json_dumps(batch)


class MainProcessAlgorithm(KeyedProcessFunction, BaseAlgorithmProcessor):
    """Обработка без мастер-топика с сохранением состояния в checkpoint."""

    def __init__(self, algorithm_config, callback_function, allowed_sink_names, state_descriptor):
        BaseAlgorithmProcessor.__init__(self, algorithm_config, callback_function, allowed_sink_names)
        self._state_descriptor = state_descriptor
        self._state = None

    def open(self, runtime_context):
        self._state = runtime_context.get_map_state(self._state_descriptor)

    def process_element(self, value: str, ctx):
        main_state = load_dict_state(self._state, MAIN_STATE_NAME)
        yield from self.process_main(value, {}, main_state)
        save_dict_state(self._state, MAIN_STATE_NAME, main_state)


class ProcessAlgorithm(KeyedCoProcessFunction, BaseAlgorithmProcessor):
    """Главный поток считает результат, дополнительные Kafka-потоки обновляют параметры."""

    def __init__(
        self,
        algorithm_config,
        callback_function,
        allowed_sink_names,
        state_descriptor,
        source_callbacks,
    ):
        BaseAlgorithmProcessor.__init__(self, algorithm_config, callback_function, allowed_sink_names)
        self._state_descriptor = state_descriptor
        self._source_callbacks = source_callbacks
        self._state = None

    def open(self, runtime_context):
        self._state = runtime_context.get_map_state(self._state_descriptor)

    def process_element1(self, value: str, ctx):
        source_states = {name: load_dict_state(self._state, name) for name in self._source_callbacks}
        main_state = load_dict_state(self._state, MAIN_STATE_NAME)
        yield from self.process_main(value, source_states, main_state)
        save_dict_state(self._state, MAIN_STATE_NAME, main_state)

    def process_element2(self, value, ctx):
        source_name, source_payload = value
        source_state = load_dict_state(self._state, source_name)
        main_state = load_dict_state(self._state, MAIN_STATE_NAME)
        self._source_callbacks[source_name](source_payload, source_state, main_state)
        save_dict_state(self._state, source_name, source_state)
        save_dict_state(self._state, MAIN_STATE_NAME, main_state)
        # PyFlink ожидает iterable и от обработчика без выходных значений.
        return ()


def run_single_output_algorithm(
    config: SingleOutputAlgorithmConfig,
    calculate_result: Callable,
    additional_sources: Optional[list[AdditionalKafkaSourceConfig]] = None,
    additional_sinks: Optional[list[AdditionalKafkaSinkConfig]] = None,
) -> None:
    additional_sources = list(additional_sources or [])
    source_callbacks = {}
    for source in additional_sources:
        name = source.Name.strip()
        if not name or name == MAIN_STATE_NAME or name in source_callbacks:
            raise ValueError(f"Недопустимое или повторное имя Kafka-источника: {name!r}")
        source_callbacks[name] = source.CallbackFunction

    sink_topics = {DEFAULT_KAFKA_SINK_NAME: config.ResultTopic}
    for sink in additional_sinks or []:
        name = sink.Name.strip()
        if not name or name in sink_topics:
            raise ValueError(f"Недопустимое или повторное имя Kafka-sink: {name!r}")
        sink_topics[name] = sink.Topic
    if not all(
        str(value or "").strip()
        for value in [
            config.RawTopic,
            config.BootstrapServer,
            config.GroupId,
            *sink_topics.values(),
            *(source.Topic for source in additional_sources),
        ]
    ):
        raise ValueError("Не заданы Kafka BootstrapServer, GroupId или топики")

    state_descriptor = build_state_descriptor(config.JobName)
    processor_args = dict(
        algorithm_config=config,
        callback_function=calculate_result,
        allowed_sink_names=set(sink_topics),
        state_descriptor=state_descriptor,
    )
    processor = (
        ProcessAlgorithm(**processor_args, source_callbacks=source_callbacks)
        if additional_sources
        else MainProcessAlgorithm(**processor_args)
    )

    runtime_config = Configuration()
    if config.CheckpointDir:
        runtime_config.set_string("state.checkpoints.dir", normalize_fs_uri(config.CheckpointDir))
    if config.SavepointDir:
        runtime_config.set_string("state.savepoints.dir", normalize_fs_uri(config.SavepointDir))
    env = StreamExecutionEnvironment.get_execution_environment(runtime_config)
    env.set_parallelism(1)
    env.enable_checkpointing(config.CheckpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(10_000)
    checkpoint_config.set_checkpoint_timeout(120_000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)
    checkpoint_config.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
    )

    raw_stream = env.from_source(
        build_kafka_source(config.BootstrapServer, config.RawTopic, config.GroupId, config.StartFrom),
        WatermarkStrategy.no_watermarks(),
        "processing-source",
    ).uid(f"{config.JobName}-source-v2")
    keyed_raw_stream = raw_stream.key_by(lambda _: STATE_STREAM_KEY, key_type=Types.STRING())
    output_type = Types.TUPLE([Types.STRING(), Types.STRING()])

    if additional_sources:
        additional_stream = None
        for index, source in enumerate(additional_sources, start=1):
            extra_stream = env.from_source(
                build_kafka_source(
                    config.BootstrapServer,
                    source.Topic,
                    source.GroupId or config.GroupId,
                    source.StartFrom,
                ),
                WatermarkStrategy.no_watermarks(),
                source.Name.strip(),
            ).uid(f"{config.JobName}-extra-source-{index}-v2")
            tagged_stream = extra_stream.map(
                lambda value, name=source.Name.strip(): (name, value),
                output_type=output_type,
            ).uid(f"{config.JobName}-extra-source-tagged-{index}-v2")
            additional_stream = tagged_stream if additional_stream is None else additional_stream.union(tagged_stream)
        processed = keyed_raw_stream.connect(
            additional_stream.key_by(lambda _: STATE_STREAM_KEY, key_type=Types.STRING()),
        ).process(processor, output_type=output_type)
    else:
        processed = keyed_raw_stream.process(processor, output_type=output_type)
    processed = processed.uid(f"{config.JobName}-process-v2")

    for index, (name, topic) in enumerate(sink_topics.items(), start=1):
        routed = processed.filter(
            lambda item, sink_name=name: item[0] == sink_name,
        ).uid(f"{config.JobName}-sink-route-{index}-v2")
        payloads = routed.map(lambda item: item[1], output_type=Types.STRING()).uid(
            f"{config.JobName}-sink-payload-{index}-v2",
        )
        payloads.sink_to(build_kafka_sink(config.BootstrapServer, topic)).uid(
            f"{config.JobName}-sink-{index}-v2",
        )
    env.execute(config.JobName)
