import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

# import psycopg2
# from psycopg2.extras import RealDictCursor, execute_batch

import pyodbc

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedBroadcastProcessFunction, KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from utils.flink_runtime import env_flag, env_int, normalize_fs_uri, setup_debugpy_if_enabled

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)
# PyFlink может установить root logger до импорта job, из-за чего basicConfig
# уже не меняет effective level и в TaskManager остаются видны только WARNING.
logger.setLevel(logging.INFO)

LOG_PAYLOAD_MAX_CHARS = max(0, env_int("ORCHESTRATOR_LOG_PAYLOAD_MAX_CHARS", 4000, logger=logger))


def log_step(stage: str, message: str, *args: Any) -> None:
    """Пишет единообразную диагностическую запись этапа оркестратора."""
    logger.info("[ORCH][%s] " + message, stage, *args)


def payload_preview(value: Any) -> str:
    """Готовит ограниченное по длине представление Kafka payload для диагностики."""
    text = str(value or "").replace("\r", "\\r").replace("\n", "\\n")
    if LOG_PAYLOAD_MAX_CHARS <= 0 or len(text) <= LOG_PAYLOAD_MAX_CHARS:
        return text
    return f"{text[:LOG_PAYLOAD_MAX_CHARS]}...<truncated total_chars={len(text)}>"

MASTER_DB_BACKEND = "mssql"
MASTER_DB_SCHEMA = "dbo"
MASTER_DB_DSN = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=wlbn0b010;"
    "DATABASE=FlinkMasterData;"
    "UID=irs_sql_user;"
    "PWD=Hgn465193bn4@!;"
    "MARS_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes"
)


class JobConfig:
    # FlinkScriptManager подставляет эти значения через PythonConfigInjector
    RawTopic = "lab.tag.processing"
    MasterTopic = "lab.md.processing"
    ResultTopic = "lab.tag.preprocessed"
    SnapshotTopic = "lab.tag.preprocessed"
    BootstrapServer = "10.178.9.232:9092"

    CheckpointIntervalMs = 30_000
    CheckpointDir = "/usr/share/flink/flink-checkpoints"
    SavepointDir = "/usr/share/flink/flink-savepoints"


class AlgorithmStateStatus:
    Pending = 0
    Processing = 1
    Done = 2
    Failed = 3


@dataclass
class AlgorithmRow:
    algorithm_name: str
    mapping_algorithm_id: str
    row_id: str
    tag_name: str
    is_input: bool


@dataclass
class MappingAlgorithm:
    algorithm_name: str
    mapping_algorithm_id: str
    rows_by_tag: Dict[str, AlgorithmRow]
    required_input_row_ids: set[str]
    output_rows: list[AlgorithmRow]


def log_mapping_snapshot(
    stage: str,
    version: str,
    templates: Dict[str, MappingAlgorithm],
    routing_index: Dict[str, list[AlgorithmRow]],
) -> None:
    """Логирует загруженный маппинг без значений тегов и реквизитов подключения."""
    log_step(
        stage,
        "mapping ready version=%s algorithms=%d routing_tags=%d",
        version,
        len(templates),
        len(routing_index),
    )
    for template in sorted(templates.values(), key=lambda item: item.algorithm_name):
        input_tags = sorted(row.tag_name for row in template.rows_by_tag.values())
        output_tags = sorted(row.tag_name for row in template.output_rows)
        log_step(
            stage,
            "mapping algorithm=%s id=%s input_tags=%s output_tags=%s",
            template.algorithm_name,
            template.mapping_algorithm_id,
            input_tags,
            output_tags,
        )
    log_step(stage, "routing index tags=%s", sorted(routing_index))

ALGORITHMS_TEMPLATES_STATE_KEY = "templates"
ALGORITHMS_ROUTING_STATE_KEY = "routing_index"
ALGORITHMS_DEPENDENCY_STATE_KEY = "dependency_index"
ALGORITHMS_VERSION_STATE_KEY = "version"

ALGORITHMS_STATE_DESCRIPTOR = MapStateDescriptor(
    "algorithms_state",
    Types.STRING(),
    Types.STRING(),
)

PENDING_DERIVED_STATE_DESCRIPTOR = ValueStateDescriptor(
    "pending_derived_outputs",
    Types.STRING(),
)

BUFFERED_TRIGGERS_STATE_DESCRIPTOR = ValueStateDescriptor(
    "buffered_algorithm_triggers",
    Types.STRING(),
)

def now_utc_iso() -> str:
    """Возвращает текущее время в UTC в формате ISO"""
    return datetime.now(timezone.utc).isoformat()


def normalize_name(value: str) -> str:
    """Нормализует имя тега для сопоставления"""
    return str(value or "").strip().lower()


def deterministic_id(prefix: str, *parts: Any) -> str:
    """Строит детерминированный UUID по набору значений"""
    payload = "|".join("" if part is None else str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{prefix}|{payload}"))


def parse_timestamp_ms(value: Any) -> Optional[int]:
    """Converts ISO timestamp to epoch milliseconds for event-time comparisons."""
    text = str(value or "").strip()
    if not text:
        return None

    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def timestamp_lte(left: Any, right: Any) -> bool:
    """Returns True when left event-time is before or equal to right event-time."""
    left_ms = parse_timestamp_ms(left)
    right_ms = parse_timestamp_ms(right)
    if left_ms is not None and right_ms is not None:
        return left_ms <= right_ms
    return str(left or "") <= str(right or "")


def same_timestamp(left: Any, right: Any) -> bool:
    """Returns True when two timestamp values represent the same event-time."""
    left_ms = parse_timestamp_ms(left)
    right_ms = parse_timestamp_ms(right)
    if left_ms is not None and right_ms is not None:
        return left_ms == right_ms
    return str(left or "").strip() == str(right or "").strip()


def serialize_db_value(value: Any) -> Any:
    """Преобразует значения из реляционной БД в JSON-совместимый вид."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return value


def parse_bool(value: Any) -> Optional[bool]:
    """Преобразует значение из Kafka в bool, если формат явно распознан."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "yes", "y", "да", "д", "input", "in"):
            return True
        if normalized in ("false", "0", "no", "n", "нет", "н", "output", "out"):
            return False
    return None


def build_algorithm_row_id(algorithm_id: str, tag_name: str, is_input: bool) -> str:
    """Строит стабильный ID строки алгоритма из данных Kafka-пакета."""
    return deterministic_id("algorithm-row", algorithm_id, normalize_name(tag_name), is_input)


def is_algorithm_name_fallback(algorithm_name: str, algorithm_id: str) -> bool:
    """Проверяет, что имя алгоритма было заменено на AlgorithmID из-за отсутствия имени в пакете."""
    return str(algorithm_name or "").strip() == str(algorithm_id or "").strip()


def algorithm_row_to_dict(row: AlgorithmRow) -> dict:
    """Преобразует строку алгоритма в JSON-совместимый словарь."""
    return {
        "algorithm_name": row.algorithm_name,
        "mapping_algorithm_id": row.mapping_algorithm_id,
        "row_id": row.row_id,
        "tag_name": row.tag_name,
        "is_input": row.is_input,
    }


def algorithm_row_from_dict(data: dict) -> Optional[AlgorithmRow]:
    """Восстанавливает строку алгоритма из словаря, полученного из broadcast state."""
    if not isinstance(data, dict):
        return None

    mapping_algorithm_id = str(data.get("mapping_algorithm_id", "")).strip()
    algorithm_name = str(data.get("algorithm_name", "")).strip() or mapping_algorithm_id
    row_id = str(data.get("row_id", "")).strip()
    tag_name = str(data.get("tag_name", "")).strip()
    is_input = parse_bool(data.get("is_input"))

    if not algorithm_name or not mapping_algorithm_id or not row_id or not tag_name or is_input is None:
        return None

    return AlgorithmRow(
        algorithm_name=algorithm_name,
        mapping_algorithm_id=mapping_algorithm_id,
        row_id=row_id,
        tag_name=tag_name,
        is_input=is_input,
    )


def mapping_algorithm_to_dict(template: MappingAlgorithm) -> dict:
    """Преобразует шаблон алгоритма в JSON-совместимый словарь."""
    input_rows = sorted(template.rows_by_tag.values(), key=lambda row: (normalize_name(row.tag_name), row.row_id))
    output_rows = sorted(template.output_rows, key=lambda row: (normalize_name(row.tag_name), row.row_id))
    return {
        "algorithm_name": template.algorithm_name,
        "mapping_algorithm_id": template.mapping_algorithm_id,
        "input_rows": [algorithm_row_to_dict(row) for row in input_rows],
        "output_rows": [algorithm_row_to_dict(row) for row in output_rows],
    }


def mapping_algorithm_from_dict(data: dict) -> Optional[MappingAlgorithm]:
    """Восстанавливает шаблон алгоритма из словаря, полученного из Kafka или state."""
    if not isinstance(data, dict):
        return None

    mapping_algorithm_id = str(data.get("mapping_algorithm_id", "")).strip()
    algorithm_name = str(data.get("algorithm_name", "")).strip() or mapping_algorithm_id
    if not algorithm_name or not mapping_algorithm_id:
        return None

    rows_by_tag: Dict[str, AlgorithmRow] = {}
    required_input_row_ids: set[str] = set()
    output_rows: list[AlgorithmRow] = []

    for row_data in data.get("input_rows", []):
        row = algorithm_row_from_dict(row_data)
        if row is None:
            continue
        rows_by_tag[normalize_name(row.tag_name)] = row
        required_input_row_ids.add(row.row_id)

    for row_data in data.get("output_rows", []):
        row = algorithm_row_from_dict(row_data)
        if row is None:
            continue
        output_rows.append(row)

    return MappingAlgorithm(
        algorithm_name=algorithm_name,
        mapping_algorithm_id=mapping_algorithm_id,
        rows_by_tag=rows_by_tag,
        required_input_row_ids=required_input_row_ids,
        output_rows=output_rows,
    )


def parse_algorithm_templates(raw: str) -> tuple[str, Dict[str, MappingAlgorithm]]:
    """
    Разбирает Kafka-пакет с мастер-данными алгоритмов.

    Ожидается массив объектов:
    [{"AlgorithmID": "...", "Timestamp": "...", "Tags": [...]}]
    AlgorithmName опционален. Если его нет, AlgorithmID используется как внутренний ключ маршрутизации.
    """
    log_step("MASTER_PARSE", "start chars=%d payload=%s", len(raw or ""), payload_preview(raw))
    try:
        payload = json.loads(raw)
    except Exception as exc:
        logger.warning("[ORCH][MASTER_PARSE] invalid JSON error=%s payload=%s", exc, payload_preview(raw))
        return "", {}

    if isinstance(payload, dict):
        algorithms = [payload]
    elif isinstance(payload, list):
        algorithms = payload
    else:
        logger.warning("[ORCH][MASTER_PARSE] unsupported root type=%s", type(payload).__name__)
        return "", {}

    log_step("MASTER_PARSE", "decoded items=%d root_type=%s", len(algorithms), type(payload).__name__)

    version = ""
    templates: Dict[str, MappingAlgorithm] = {}

    for item in algorithms:
        if not isinstance(item, dict):
            logger.warning("[ORCH][MASTER_PARSE] skip non-object item type=%s", type(item).__name__)
            continue

        raw_algorithm_id = item.get("AlgorithmID")
        algorithm_id = str(raw_algorithm_id or "").strip()
        algorithm_name = str(item.get("AlgorithmName", "")).strip()
        timestamp = str(item.get("Timestamp", "")).strip()
        tags = item.get("Tags")

        if timestamp:
            version = max(version, timestamp)

        if isinstance(raw_algorithm_id, list) and isinstance(item.get("Params"), list):
            # В общем MasterTopic также находятся параметры экземпляров алгоритмов
            # (например, LineStatus_A3Flex). Это другой валидный контракт, а не
            # поврежденный пакет маппинга AlgorithmRows.
            log_step(
                "MASTER_PARSE",
                "runtime parameters message ignored by mapping parser algorithm_ids=%s object_id=%s params=%d",
                raw_algorithm_id,
                item.get("ObjectID"),
                len(item.get("Params") or []),
            )
            continue

        if not algorithm_id or not isinstance(tags, list):
            logger.warning("Пропуск элемента с неверным форматом маппинга алгоритма: %s", item)
            continue

        try:
            uuid.UUID(algorithm_id)
        except Exception:
            logger.warning("Пропуск алгоритма с невалидным uuid AlgorithmID %s'.", algorithm_id)
            continue

        if not algorithm_name:
            algorithm_name = algorithm_id
            logger.info("AlgorithmName не указан для AlgorithmID '%s'", algorithm_id)

        rows_by_tag: Dict[str, AlgorithmRow] = {}
        required_input_row_ids: set[str] = set()
        output_rows: list[AlgorithmRow] = []

        for tag in tags:
            if not isinstance(tag, dict):
                continue

            tag_name = str(tag.get("TagName", "")).strip()
            is_input = parse_bool(tag.get("IsInput"))
            if not tag_name or is_input is None:
                logger.warning("Пропуск невалидного тега: '%s' для алгоритма: %s", tag, algorithm_name)
                continue

            row = AlgorithmRow(
                algorithm_name=algorithm_name,
                mapping_algorithm_id=algorithm_id,
                row_id=build_algorithm_row_id(algorithm_id, tag_name, is_input),
                tag_name=tag_name,
                is_input=is_input,
            )

            if row.is_input:
                rows_by_tag[normalize_name(row.tag_name)] = row
                required_input_row_ids.add(row.row_id)
            else:
                output_rows.append(row)

        templates[algorithm_name] = MappingAlgorithm(
            algorithm_name=algorithm_name,
            mapping_algorithm_id=algorithm_id,
            rows_by_tag=rows_by_tag,
            required_input_row_ids=required_input_row_ids,
            output_rows=output_rows,
        )

        log_step(
            "MASTER_PARSE",
            "template parsed algorithm=%s id=%s inputs=%s outputs=%s",
            algorithm_name,
            algorithm_id,
            sorted(row.tag_name for row in rows_by_tag.values()),
            sorted(row.tag_name for row in output_rows),
        )

    resolved_version = version or now_utc_iso()
    log_step("MASTER_PARSE", "finish version=%s templates=%d", resolved_version, len(templates))
    return resolved_version, templates


def encode_templates(templates: Dict[str, MappingAlgorithm]) -> str:
    """Сериализует шаблоны алгоритмов для хранения в broadcast state."""
    payload = {name: mapping_algorithm_to_dict(template) for name, template in templates.items()}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def decode_templates(raw: Optional[str]) -> Dict[str, MappingAlgorithm]:
    """Десериализует шаблоны алгоритмов из broadcast state."""
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    templates: Dict[str, MappingAlgorithm] = {}
    for name, template_data in payload.items():
        template = mapping_algorithm_from_dict(template_data)
        if template is not None:
            templates[str(name)] = template
    return templates


# class PostgresStateRepository:
#     """
#     Работает с текущими C# сущностями в PostgreSQL
#       MappingAlgorithms(ID, AlgorithmName)
#       AlgorithmRows(ID, AlgorithmID, TagName, IsInput)
#       AlgorithmStates(ID, MappingAlgorithmID, StartTime, FinishTime, Status)
#       TagValues(ID, AlgorithmRowID, ValueString, ValueDouble, ValueInt, Quality, Created, Modified)
#       AlgorithmStateDetails(
#           ID, AlgorithmStateID, AlgorithmRowID, ValueString, ValueDouble, ValueInt, Quality, SnapshotTime
#       )
#     """
#
#     def __init__(self, db_dsn: str):
#         """Сохраняет строку подключения и имена таблиц"""
#         self._db_dsn = db_dsn
#         self._conn = None
#
#         # Имена таблиц можно переопределить через переменные среды
#         self._t_mapping_algorithms = os.getenv("MASTER_DB_TABLE_MAPPING_ALGORITHMS", "MappingAlgorithms")
#         self._t_algorithm_rows = os.getenv("MASTER_DB_TABLE_ALGORITHM_ROWS", "AlgorithmRows")
#         self._t_algorithm_states = os.getenv("MASTER_DB_TABLE_ALGORITHM_STATES", "AlgorithmStates")
#         self._t_algorithm_state_details = os.getenv("MASTER_DB_TABLE_ALGORITHM_STATE_DETAILS", "AlgorithmStateDetails")
#         self._t_tag_values = os.getenv("MASTER_DB_TABLE_TAG_VALUES", "TagValues")
#
#     @staticmethod
#     def _q(name: str) -> str:
#         """Экранирует имя таблицы для PostgreSQL"""
#         return f'"{name}"'
#
#     def open(self):
#         """Открывает соединение с PostgreSQL"""
#         self._conn = psycopg2.connect(self._db_dsn)
#         self._conn.autocommit = False
#
#     def close(self):
#         """Закрывает соединение с PostgreSQL"""
#         if self._conn:
#             self._conn.close()
#
#     def load_algorithms(self) -> Dict[str, MappingAlgorithm]:
#         """Загружает маппинг алгоритмов и оставляет только входные строки"""
#         sql = f"""
#             SELECT
#                 ma."ID" AS mapping_algorithm_id,
#                 ma."AlgorithmName" AS algorithm_name,
#                 ar."ID" AS row_id,
#                 ar."TagName" AS tag_name,
#                 ar."IsInput" AS is_input
#             FROM {self._q(self._t_mapping_algorithms)} ma
#             JOIN {self._q(self._t_algorithm_rows)} ar ON ar."AlgorithmID" = ma."ID"
#             ORDER BY ma."AlgorithmName", ar."TagName"
#         """
#
#         templates: Dict[str, MappingAlgorithm] = {}
#         with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
#             cur.execute(sql)
#             rows = cur.fetchall()
#
#         for row_data in rows:
#             algorithm_name = str(row_data["algorithm_name"])
#             if algorithm_name not in templates:
#                 templates[algorithm_name] = MappingAlgorithm(
#                     algorithm_name=algorithm_name,
#                     mapping_algorithm_id=str(row_data["mapping_algorithm_id"]),
#                     rows_by_tag={},
#                     required_input_row_ids=set(),
#                     output_rows=[],
#                 )
#
#             row = AlgorithmRow(
#                 algorithm_name=algorithm_name,
#                 mapping_algorithm_id=str(row_data["mapping_algorithm_id"]),
#                 row_id=str(row_data["row_id"]),
#                 tag_name=str(row_data["tag_name"]),
#                 is_input=bool(row_data["is_input"]),
#             )
#
#             # Мастер формирует состояние только по входным строкам
#             if row.is_input:
#                 templates[algorithm_name].rows_by_tag[normalize_name(row.tag_name)] = row
#                 templates[algorithm_name].required_input_row_ids.add(row.row_id)
#             else:
#                 templates[algorithm_name].output_rows.append(row)
#
#         return templates
#
#     def upsert_algorithm_template(self, template: MappingAlgorithm):
#         """Синхронизирует алгоритм и его строки в PostgreSQL по данным из Kafka."""
#         upsert_named_algorithm_sql = f"""
#             INSERT INTO {self._q(self._t_mapping_algorithms)}
#             ("ID", "AlgorithmName")
#             VALUES (%s, %s)
#             ON CONFLICT("ID") DO UPDATE SET
#                 "AlgorithmName" = excluded."AlgorithmName"
#         """
#
#         insert_fallback_algorithm_sql = f"""
#             INSERT INTO {self._q(self._t_mapping_algorithms)}
#             ("ID", "AlgorithmName")
#             VALUES (%s, %s)
#             ON CONFLICT("ID") DO NOTHING
#         """
#
#         upsert_row_sql = f"""
#             INSERT INTO {self._q(self._t_algorithm_rows)}
#             ("ID", "AlgorithmID", "TagName", "IsInput")
#             VALUES (%s, %s, %s, %s)
#             ON CONFLICT("ID") DO UPDATE SET
#                 "AlgorithmID" = excluded."AlgorithmID",
#                 "TagName" = excluded."TagName",
#                 "IsInput" = excluded."IsInput"
#         """
#
#         try:
#             with self._conn.cursor() as cur:
#                 algorithm_sql = (
#                     insert_fallback_algorithm_sql
#                     if is_algorithm_name_fallback(template.algorithm_name, template.mapping_algorithm_id)
#                     else upsert_named_algorithm_sql
#                 )
#                 cur.execute(
#                     algorithm_sql,
#                     (template.mapping_algorithm_id, template.algorithm_name),
#                 )
#
#                 rows = list(template.rows_by_tag.values()) + list(template.output_rows)
#                 execute_batch(
#                     cur,
#                     upsert_row_sql,
#                     [
#                         (
#                             row.row_id,
#                             row.mapping_algorithm_id,
#                             row.tag_name,
#                             row.is_input,
#                         )
#                         for row in rows
#                     ],
#                 )
#             self._conn.commit()
#         except Exception:
#             self._conn.rollback()
#             raise
#
#     def publish_complete_state(
#         self,
#         mapping_algorithm_id: str,
#         start_time: str,
#         required_row_ids: set[str],
#         updates_by_row: Dict[str, dict],
#     ) -> Optional[dict]:
#         """
#         Обновляет текущие TagValue
#         Проверяет полноту входных тегов
#         При полном наборе создает AlgorithmState и AlgorithmStateDetail
#         """
#         if not required_row_ids:
#             return None
#
#         upsert_tag_value_sql = f"""
#             INSERT INTO {self._q(self._t_tag_values)}
#             ("ID", "AlgorithmRowID", "ValueString", "ValueDouble", "ValueInt", "Quality", "Created", "Modified")
#             VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
#             ON CONFLICT("ID") DO UPDATE SET
#                 "AlgorithmRowID" = excluded."AlgorithmRowID",
#                 "ValueString" = excluded."ValueString",
#                 "ValueDouble" = excluded."ValueDouble",
#                 "ValueInt" = excluded."ValueInt",
#                 "Quality" = excluded."Quality",
#                 "Modified" = excluded."Created"
#         """
#
#         snapshot_row_sql_template = """
#             SELECT
#                 ar."ID" AS row_id,
#                 ar."TagName" AS tag_name,
#                 tv."ValueString" AS value_string,
#                 tv."ValueDouble" AS value_double,
#                 tv."ValueInt" AS value_int,
#                 tv."Quality" AS quality,
#                 COALESCE(tv."Modified", tv."Created") AS snapshot_time
#             FROM {algorithm_rows} ar
#             LEFT JOIN {tag_values} tv ON tv."ID" = (
#                 SELECT tv2."ID"
#                 FROM {tag_values} tv2
#                 WHERE tv2."AlgorithmRowID" = ar."ID"
#                 ORDER BY COALESCE(tv2."Modified", tv2."Created") DESC, tv2."Created" DESC, tv2."ID" DESC
#                 LIMIT 1
#             )
#             WHERE ar."ID" IN ({placeholders})
#             ORDER BY ar."ID"
#         """
#
#         select_state_sql = f"""
#             SELECT 1
#             FROM {self._q(self._t_algorithm_states)}
#             WHERE "ID" = %s
#             LIMIT 1
#         """
#
#         insert_state_sql = f"""
#             INSERT INTO {self._q(self._t_algorithm_states)}
#             ("ID", "MappingAlgorithmID", "StartTime", "FinishTime", "Status")
#             VALUES (%s, %s, %s, NULL, %s)
#         """
#
#         insert_state_detail_sql = f"""
#             INSERT INTO {self._q(self._t_algorithm_state_details)}
#             ("ID", "AlgorithmStateID", "AlgorithmRowID", "ValueString", "ValueDouble", "ValueInt", "Quality", "SnapshotTime")
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#             ON CONFLICT("ID") DO NOTHING
#         """
#
#         try:
#             with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
#                 upsert_rows = [
#                     (
#                         deterministic_id("tag-value-current", row_id),
#                         row_id,
#                         row_value.get("value_string"),
#                         row_value.get("value_double"),
#                         row_value.get("value_int"),
#                         row_value.get("quality"),
#                         row_value.get("created"),
#                     )
#                     for row_id, row_value in updates_by_row.items()
#                 ]
#                 execute_batch(cur, upsert_tag_value_sql, upsert_rows)
#
#                 ordered_row_ids = sorted(required_row_ids)
#                 placeholders = ", ".join("%s" for _ in ordered_row_ids)
#                 snapshot_row_sql = snapshot_row_sql_template.format(
#                     algorithm_rows=self._q(self._t_algorithm_rows),
#                     tag_values=self._q(self._t_tag_values),
#                     placeholders=placeholders,
#                 )
#
#                 cur.execute(snapshot_row_sql, ordered_row_ids)
#                 snapshot_rows = [
#                     {
#                         "algorithm_row_id": str(row["row_id"]),
#                         "tag_name": str(row["tag_name"]),
#                         "is_input": True,
#                         "value_string": serialize_db_value(row["value_string"]),
#                         "value_double": serialize_db_value(row["value_double"]),
#                         "value_int": serialize_db_value(row["value_int"]),
#                         "quality": serialize_db_value(row["quality"]),
#                         "snapshot_time": serialize_db_value(row["snapshot_time"]),
#                     }
#                     for row in cur.fetchall()
#                 ]
#
#                 snapshot_parts = []
#                 for row in snapshot_rows:
#                     snapshot_parts.extend(
#                         [
#                             row["algorithm_row_id"],
#                             row["value_string"],
#                             row["value_double"],
#                             row["value_int"],
#                             row["quality"],
#                             row["snapshot_time"],
#                         ]
#                     )
#
#                 state_id = deterministic_id(
#                     "algorithm-state",
#                     mapping_algorithm_id,
#                     start_time,
#                     *snapshot_parts,
#                 )
#
#                 cur.execute(select_state_sql, (state_id,))
#                 state_exists = cur.fetchone()
#                 if state_exists is not None:
#                     self._conn.commit()
#                     return {
#                         "state": {
#                             "id": state_id,
#                             "mapping_algorithm_id": mapping_algorithm_id,
#                             "start_time": start_time,
#                             "finish_time": None,
#                             "status": "Pending",
#                             "status_code": AlgorithmStateStatus.Pending,
#                             "plant_id": "",
#                             "object_id": "",
#                         },
#                         "details": snapshot_rows,
#                         "inserted": False,
#                     }
#
#                 cur.execute(
#                     insert_state_sql,
#                     (
#                         state_id,
#                         mapping_algorithm_id,
#                         start_time,
#                         AlgorithmStateStatus.Pending,
#                     ),
#                 )
#
#                 detail_rows = [
#                     (
#                         deterministic_id("algorithm-state-detail", state_id, row["algorithm_row_id"]),
#                         state_id,
#                         row["algorithm_row_id"],
#                         row["value_string"],
#                         row["value_double"],
#                         row["value_int"],
#                         row["quality"],
#                         row["snapshot_time"] or start_time,
#                     )
#                     for row in snapshot_rows
#                 ]
#                 execute_batch(cur, insert_state_detail_sql, detail_rows)
#
#             self._conn.commit()
#             return {
#                 "state": {
#                     "id": state_id,
#                     "mapping_algorithm_id": mapping_algorithm_id,
#                     "start_time": start_time,
#                     "finish_time": None,
#                     "status": "Pending",
#                     "status_code": AlgorithmStateStatus.Pending,
#                 },
#                 "details": snapshot_rows,
#                 "inserted": True,
#             }
#         except Exception:
#             self._conn.rollback()
#             raise


class MssqlStateRepository:
    """
    Работает с текущими C# сущностями в Microsoft SQL Server.

    Схема таблиц совпадает с PostgresStateRepository, а SQL использует
    T-SQL и параметризацию pyodbc.
    """

    def __init__(self, db_dsn: str, db_schema: Optional[str] = None):
        """Сохраняет ODBC-строку подключения, схему и имена таблиц."""
        self._db_dsn = db_dsn
        self._conn = None
        configured_schema = db_schema if db_schema is not None else os.getenv("MASTER_DB_SCHEMA", "dbo")
        self._db_schema = str(configured_schema).strip()

        self._t_mapping_algorithms = os.getenv("MASTER_DB_TABLE_MAPPING_ALGORITHMS", "MappingAlgorithms")
        self._t_algorithm_rows = os.getenv("MASTER_DB_TABLE_ALGORITHM_ROWS", "AlgorithmRows")
        self._t_algorithm_states = os.getenv("MASTER_DB_TABLE_ALGORITHM_STATES", "AlgorithmStates")
        self._t_algorithm_state_details = os.getenv(
            "MASTER_DB_TABLE_ALGORITHM_STATE_DETAILS",
            "AlgorithmStateDetails",
        )
        self._t_tag_values = os.getenv("MASTER_DB_TABLE_TAG_VALUES", "TagValues")

    def _q(self, name: str) -> str:
        """Экранирует составное имя объекта для Microsoft SQL Server."""
        parts = [part.strip() for part in name.split(".") if part.strip()]
        if len(parts) == 1 and self._db_schema:
            parts.insert(0, self._db_schema)
        return ".".join(f"[{part.replace(']', ']]')}]" for part in parts)

    @staticmethod
    def _rows_as_dicts(cursor) -> list[dict]:
        """Преобразует строки pyodbc в словари по алиасам SELECT."""
        columns = [str(column[0]).lower() for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def _execute_batch(cursor, sql: str, rows: list[tuple]):
        """Выполняет параметризованный T-SQL для набора строк."""
        if not rows:
            return
        cursor.executemany(sql, rows)

    @staticmethod
    def _to_datetime2(value: Any) -> Any:
        """Нормализует ISO-время в UTC без timezone для столбца datetime2."""
        if value is None:
            return None

        parsed = value
        if not isinstance(parsed, datetime):
            text = str(parsed).strip()
            if not text:
                return value
            try:
                normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return value

        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    def open(self):
        """Открывает транзакционное соединение с Microsoft SQL Server."""
        log_step("DB", "opening MSSQL connection schema=%s", self._db_schema)
        if pyodbc is None:
            raise RuntimeError(
                "Для MASTER_DB_BACKEND=mssql требуется пакет pyodbc. "
                "Установите зависимости из python/requirements.txt."
            )
        self._conn = pyodbc.connect(self._db_dsn, autocommit=False)
        log_step("DB", "MSSQL connection opened schema=%s", self._db_schema)

    def close(self):
        """Закрывает соединение с Microsoft SQL Server."""
        if self._conn:
            self._conn.close()
            log_step("DB", "MSSQL connection closed schema=%s", self._db_schema)

    def load_algorithms(self) -> Dict[str, MappingAlgorithm]:
        """Загружает маппинг алгоритмов и разделяет входные и выходные строки."""
        log_step("DB_MAPPING", "loading MappingAlgorithms and AlgorithmRows")
        sql = f"""
            SELECT
                ma.[ID] AS mapping_algorithm_id,
                ma.[AlgorithmName] AS algorithm_name,
                ar.[ID] AS row_id,
                ar.[TagName] AS tag_name,
                ar.[IsInput] AS is_input
            FROM {self._q(self._t_mapping_algorithms)} ma
            JOIN {self._q(self._t_algorithm_rows)} ar ON ar.[AlgorithmID] = ma.[ID]
            ORDER BY ma.[AlgorithmName], ar.[TagName]
        """

        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            rows = self._rows_as_dicts(cursor)
        finally:
            cursor.close()

        log_step("DB_MAPPING", "SQL returned rows=%d", len(rows))

        templates: Dict[str, MappingAlgorithm] = {}
        for row_data in rows:
            algorithm_name = str(row_data["algorithm_name"])
            if algorithm_name not in templates:
                templates[algorithm_name] = MappingAlgorithm(
                    algorithm_name=algorithm_name,
                    mapping_algorithm_id=str(row_data["mapping_algorithm_id"]),
                    rows_by_tag={},
                    required_input_row_ids=set(),
                    output_rows=[],
                )

            row = AlgorithmRow(
                algorithm_name=algorithm_name,
                mapping_algorithm_id=str(row_data["mapping_algorithm_id"]),
                row_id=str(row_data["row_id"]),
                tag_name=str(row_data["tag_name"]),
                is_input=bool(row_data["is_input"]),
            )
            if row.is_input:
                templates[algorithm_name].rows_by_tag[normalize_name(row.tag_name)] = row
                templates[algorithm_name].required_input_row_ids.add(row.row_id)
            else:
                templates[algorithm_name].output_rows.append(row)

        log_step("DB_MAPPING", "templates built=%d", len(templates))
        return templates

    def upsert_algorithm_template(self, template: MappingAlgorithm):
        """Синхронизирует алгоритм и его строки через атомарные T-SQL MERGE."""
        log_step(
            "DB_MAPPING",
            "upsert start algorithm=%s id=%s inputs=%d outputs=%d",
            template.algorithm_name,
            template.mapping_algorithm_id,
            len(template.rows_by_tag),
            len(template.output_rows),
        )
        upsert_named_algorithm_sql = f"""
            MERGE {self._q(self._t_mapping_algorithms)} WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?)) AS source ([ID], [AlgorithmName])
                ON target.[ID] = source.[ID]
            WHEN MATCHED THEN
                UPDATE SET [AlgorithmName] = source.[AlgorithmName]
            WHEN NOT MATCHED THEN
                INSERT ([ID], [AlgorithmName])
                VALUES (source.[ID], source.[AlgorithmName]);
        """

        insert_fallback_algorithm_sql = f"""
            MERGE {self._q(self._t_mapping_algorithms)} WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?)) AS source ([ID], [AlgorithmName])
                ON target.[ID] = source.[ID]
            WHEN NOT MATCHED THEN
                INSERT ([ID], [AlgorithmName])
                VALUES (source.[ID], source.[AlgorithmName]);
        """

        upsert_row_sql = f"""
            MERGE {self._q(self._t_algorithm_rows)} WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?)) AS source ([ID], [AlgorithmID], [TagName], [IsInput])
                ON target.[ID] = source.[ID]
            WHEN MATCHED THEN
                UPDATE SET
                    [AlgorithmID] = source.[AlgorithmID],
                    [TagName] = source.[TagName],
                    [IsInput] = source.[IsInput]
            WHEN NOT MATCHED THEN
                INSERT ([ID], [AlgorithmID], [TagName], [IsInput])
                VALUES (source.[ID], source.[AlgorithmID], source.[TagName], source.[IsInput]);
        """

        try:
            cursor = self._conn.cursor()
            try:
                algorithm_sql = (
                    insert_fallback_algorithm_sql
                    if is_algorithm_name_fallback(template.algorithm_name, template.mapping_algorithm_id)
                    else upsert_named_algorithm_sql
                )
                cursor.execute(
                    algorithm_sql,
                    template.mapping_algorithm_id,
                    template.algorithm_name,
                )

                rows = list(template.rows_by_tag.values()) + list(template.output_rows)
                self._execute_batch(
                    cursor,
                    upsert_row_sql,
                    [
                        (
                            row.row_id,
                            row.mapping_algorithm_id,
                            row.tag_name,
                            row.is_input,
                        )
                        for row in rows
                    ],
                )
            finally:
                cursor.close()
            self._conn.commit()
            log_step("DB_MAPPING", "upsert committed algorithm=%s", template.algorithm_name)
        except Exception:
            self._conn.rollback()
            logger.exception("[ORCH][DB_MAPPING] upsert rolled back algorithm=%s", template.algorithm_name)
            raise

    def publish_complete_state(
        self,
        mapping_algorithm_id: str,
        start_time: str,
        required_row_ids: set[str],
        updates_by_row: Dict[str, dict],
    ) -> Optional[dict]:
        """Обновляет текущие значения и атомарно сохраняет полный снимок состояния."""
        if not required_row_ids:
            log_step(
                "DB_STATE",
                "skip publish because required rows are empty mapping_id=%s timestamp=%s",
                mapping_algorithm_id,
                start_time,
            )
            return None

        log_step(
            "DB_STATE",
            "publish start mapping_id=%s timestamp=%s required_rows=%d update_rows=%d update_row_ids=%s",
            mapping_algorithm_id,
            start_time,
            len(required_row_ids),
            len(updates_by_row),
            sorted(updates_by_row),
        )

        upsert_tag_value_sql = f"""
            MERGE {self._q(self._t_tag_values)} WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS source
                ([ID], [AlgorithmRowID], [ValueString], [ValueDouble], [ValueInt], [Quality], [Created])
                ON target.[ID] = source.[ID]
            WHEN MATCHED THEN
                UPDATE SET
                    [AlgorithmRowID] = source.[AlgorithmRowID],
                    [ValueString] = source.[ValueString],
                    [ValueDouble] = source.[ValueDouble],
                    [ValueInt] = source.[ValueInt],
                    [Quality] = source.[Quality],
                    [Modified] = source.[Created]
            WHEN NOT MATCHED THEN
                INSERT
                    ([ID], [AlgorithmRowID], [ValueString], [ValueDouble], [ValueInt], [Quality], [Created], [Modified])
                VALUES
                    (source.[ID], source.[AlgorithmRowID], source.[ValueString], source.[ValueDouble],
                     source.[ValueInt], source.[Quality], source.[Created], NULL);
        """

        snapshot_row_sql_template = """
            SELECT
                ar.[ID] AS row_id,
                ar.[TagName] AS tag_name,
                tv.[ValueString] AS value_string,
                tv.[ValueDouble] AS value_double,
                tv.[ValueInt] AS value_int,
                tv.[Quality] AS quality,
                COALESCE(tv.[Modified], tv.[Created]) AS snapshot_time
            FROM {algorithm_rows} ar
            LEFT JOIN {tag_values} tv ON tv.[ID] = (
                SELECT TOP (1) tv2.[ID]
                FROM {tag_values} tv2
                WHERE tv2.[AlgorithmRowID] = ar.[ID]
                ORDER BY COALESCE(tv2.[Modified], tv2.[Created]) DESC, tv2.[Created] DESC, tv2.[ID] DESC
            )
            WHERE ar.[ID] IN ({placeholders})
            ORDER BY ar.[ID]
        """

        select_state_sql = f"""
            SELECT TOP (1) 1
            FROM {self._q(self._t_algorithm_states)}
            WHERE [ID] = ?
        """

        insert_state_sql = f"""
            INSERT INTO {self._q(self._t_algorithm_states)}
            ([ID], [MappingAlgorithmID], [StartTime], [FinishTime], [Status])
            VALUES (?, ?, ?, NULL, ?)
        """

        insert_state_detail_sql = f"""
            MERGE {self._q(self._t_algorithm_state_details)} WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source
                ([ID], [AlgorithmStateID], [AlgorithmRowID], [ValueString],
                 [ValueDouble], [ValueInt], [Quality], [SnapshotTime])
                ON target.[ID] = source.[ID]
            WHEN NOT MATCHED THEN
                INSERT
                    ([ID], [AlgorithmStateID], [AlgorithmRowID], [ValueString],
                     [ValueDouble], [ValueInt], [Quality], [SnapshotTime])
                VALUES
                    (source.[ID], source.[AlgorithmStateID], source.[AlgorithmRowID], source.[ValueString],
                     source.[ValueDouble], source.[ValueInt], source.[Quality], source.[SnapshotTime]);
        """

        try:
            cursor = self._conn.cursor()
            try:
                upsert_rows = [
                    (
                        deterministic_id("tag-value-current", row_id),
                        row_id,
                        row_value.get("value_string"),
                        row_value.get("value_double"),
                        row_value.get("value_int"),
                        row_value.get("quality"),
                        self._to_datetime2(row_value.get("created")),
                    )
                    for row_id, row_value in updates_by_row.items()
                ]
                self._execute_batch(cursor, upsert_tag_value_sql, upsert_rows)
                log_step("DB_STATE", "TagValues upserted rows=%d", len(upsert_rows))

                ordered_row_ids = sorted(required_row_ids)
                placeholders = ", ".join("?" for _ in ordered_row_ids)
                snapshot_row_sql = snapshot_row_sql_template.format(
                    algorithm_rows=self._q(self._t_algorithm_rows),
                    tag_values=self._q(self._t_tag_values),
                    placeholders=placeholders,
                )

                cursor.execute(snapshot_row_sql, ordered_row_ids)
                snapshot_rows = [
                    {
                        "algorithm_row_id": str(row["row_id"]),
                        "tag_name": str(row["tag_name"]),
                        "is_input": True,
                        "value_string": serialize_db_value(row["value_string"]),
                        "value_double": serialize_db_value(row["value_double"]),
                        "value_int": serialize_db_value(row["value_int"]),
                        "quality": serialize_db_value(row["quality"]),
                        "snapshot_time": serialize_db_value(row["snapshot_time"]),
                    }
                    for row in self._rows_as_dicts(cursor)
                ]
                missing_snapshot_tags = [
                    row["tag_name"]
                    for row in snapshot_rows
                    if row["snapshot_time"] is None
                ]
                log_step(
                    "DB_STATE",
                    "snapshot selected rows=%d tags=%s missing_values=%s",
                    len(snapshot_rows),
                    [row["tag_name"] for row in snapshot_rows],
                    missing_snapshot_tags,
                )

                snapshot_parts = []
                for row in snapshot_rows:
                    snapshot_parts.extend(
                        [
                            row["algorithm_row_id"],
                            row["value_string"],
                            row["value_double"],
                            row["value_int"],
                            row["quality"],
                            row["snapshot_time"],
                        ]
                    )

                state_id = deterministic_id(
                    "algorithm-state",
                    mapping_algorithm_id,
                    start_time,
                    *snapshot_parts,
                )
                log_step("DB_STATE", "deterministic state_id=%s", state_id)

                cursor.execute(select_state_sql, state_id)
                state_exists = cursor.fetchone()
                if state_exists is not None:
                    self._conn.commit()
                    log_step("DB_STATE", "state already exists state_id=%s transaction committed", state_id)
                    return {
                        "state": {
                            "id": state_id,
                            "mapping_algorithm_id": mapping_algorithm_id,
                            "start_time": start_time,
                            "finish_time": None,
                            "status": "Pending",
                            "status_code": AlgorithmStateStatus.Pending,
                            "plant_id": "",
                            "object_id": "",
                        },
                        "details": snapshot_rows,
                        "inserted": False,
                    }

                cursor.execute(
                    insert_state_sql,
                    state_id,
                    mapping_algorithm_id,
                    self._to_datetime2(start_time),
                    AlgorithmStateStatus.Pending,
                )
                log_step("DB_STATE", "AlgorithmStates row inserted state_id=%s", state_id)

                detail_rows = [
                    (
                        deterministic_id("algorithm-state-detail", state_id, row["algorithm_row_id"]),
                        state_id,
                        row["algorithm_row_id"],
                        row["value_string"],
                        row["value_double"],
                        row["value_int"],
                        row["quality"],
                        self._to_datetime2(row["snapshot_time"] or start_time),
                    )
                    for row in snapshot_rows
                ]
                self._execute_batch(cursor, insert_state_detail_sql, detail_rows)
                log_step("DB_STATE", "AlgorithmStateDetails upserted state_id=%s rows=%d", state_id, len(detail_rows))
            finally:
                cursor.close()

            self._conn.commit()
            log_step("DB_STATE", "publish committed state_id=%s inserted=true", state_id)
            return {
                "state": {
                    "id": state_id,
                    "mapping_algorithm_id": mapping_algorithm_id,
                    "start_time": start_time,
                    "finish_time": None,
                    "status": "Pending",
                    "status_code": AlgorithmStateStatus.Pending,
                },
                "details": snapshot_rows,
                "inserted": True,
            }
        except Exception:
            self._conn.rollback()
            logger.exception(
                "[ORCH][DB_STATE] publish rolled back mapping_id=%s timestamp=%s",
                mapping_algorithm_id,
                start_time,
            )
            raise


def normalize_db_backend(db_backend: str) -> str:
    """Приводит поддерживаемые псевдонимы backend к каноническому имени."""
    normalized = str(db_backend or "postgres").strip().lower()
    if normalized in ("postgres", "postgresql"):
        return "postgres"
    if normalized in ("mssql", "sqlserver", "sql-server"):
        return "mssql"
    raise ValueError(f"Неподдерживаемый MASTER_DB_BACKEND: {db_backend!r}")


def create_state_repository(db_backend: str, db_dsn: str, db_schema: Optional[str] = None):
    """Создает репозиторий для выбранного SQL backend."""
    normalized = normalize_db_backend(db_backend)
    # if normalized == "postgres":
    #     return PostgresStateRepository(db_dsn)
    return MssqlStateRepository(db_dsn, db_schema=db_schema)


def build_output_details(output_rows: list[AlgorithmRow]) -> list[dict]:
    """Строит заглушки для выходных тегов без значений"""
    return [
        {
            "algorithm_row_id": row.row_id,
            "tag_name": row.tag_name,
            "is_input": False,
            "value_string": None,
            "value_double": None,
            "value_int": None,
            "quality": None,
            "snapshot_time": None,
        }
        for row in output_rows
    ]


def build_kafka_source(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    start_from: str,
) -> KafkaSource:
    """Создает Kafka source с нужной начальной позицией"""
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


def build_routing_index(templates: Dict[str, MappingAlgorithm]) -> Dict[str, list[AlgorithmRow]]:
    """Строит индекс тегов для маршрутизации по алгоритмам"""
    index: Dict[str, list[AlgorithmRow]] = {}
    for template in templates.values():
        for row in template.rows_by_tag.values():
            if not row.is_input:
                continue
            key = normalize_name(row.tag_name)
            index.setdefault(key, []).append(row)
    return index


def build_dependency_index(templates: Dict[str, MappingAlgorithm]) -> dict:
    """Builds a full algorithm DAG from input/output tag mapping rows."""
    output_tags: Dict[str, list[dict]] = {}
    input_tags: Dict[str, list[dict]] = {}
    algorithm_names = sorted(templates)

    for template in templates.values():
        for row in template.output_rows:
            key = normalize_name(row.tag_name)
            output_tags.setdefault(key, []).append(
                {
                    "algorithm_name": template.algorithm_name,
                    "mapping_algorithm_id": template.mapping_algorithm_id,
                    "row_id": row.row_id,
                    "tag_name": row.tag_name,
                }
            )

        for row in template.rows_by_tag.values():
            key = normalize_name(row.tag_name)
            input_tags.setdefault(key, []).append(
                {
                    "algorithm_name": template.algorithm_name,
                    "mapping_algorithm_id": template.mapping_algorithm_id,
                    "row_id": row.row_id,
                    "tag_name": row.tag_name,
                }
            )

    dependent_output_tags = set(output_tags).intersection(input_tags)
    derived_inputs_by_algorithm: Dict[str, list[dict]] = {}
    tracked_outputs_by_algorithm: Dict[str, list[dict]] = {}
    direct_upstream: Dict[str, set[str]] = {name: set() for name in algorithm_names}
    direct_downstream: Dict[str, set[str]] = {name: set() for name in algorithm_names}

    for tag_key in dependent_output_tags:
        producers = output_tags.get(tag_key, [])
        consumers = input_tags.get(tag_key, [])
        producer_names = sorted({str(item["algorithm_name"]) for item in producers})
        consumer_names = sorted({str(item["algorithm_name"]) for item in consumers})

        for consumer in consumers:
            consumer_name = str(consumer["algorithm_name"])
            derived_inputs_by_algorithm.setdefault(consumer_name, []).append(
                {
                    "tag_name": consumer["tag_name"],
                    "normalized_tag": tag_key,
                    "row_id": consumer["row_id"],
                    "producer_algorithms": producer_names,
                }
            )
            for producer_name in producer_names:
                direct_upstream.setdefault(consumer_name, set()).add(producer_name)
                direct_downstream.setdefault(producer_name, set()).add(consumer_name)

        for producer in producers:
            producer_name = str(producer["algorithm_name"])
            tracked_outputs_by_algorithm.setdefault(producer_name, []).append(
                {
                    "tag_name": producer["tag_name"],
                    "normalized_tag": tag_key,
                    "row_id": producer["row_id"],
                    "downstream_algorithms": consumer_names,
                }
            )

    cycles: list[list[str]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit_for_cycles(algorithm_name: str, path: list[str]):
        if algorithm_name in visiting:
            start_index = path.index(algorithm_name) if algorithm_name in path else 0
            cycle = path[start_index:] if path and path[-1] == algorithm_name else path[start_index:] + [algorithm_name]
            if cycle not in cycles:
                cycles.append(cycle)
            return
        if algorithm_name in visited:
            return

        visiting.add(algorithm_name)
        for child in sorted(direct_downstream.get(algorithm_name, set())):
            visit_for_cycles(child, path + [child])
        visiting.remove(algorithm_name)
        visited.add(algorithm_name)

    for algorithm_name in algorithm_names:
        visit_for_cycles(algorithm_name, [algorithm_name])

    blocking_output_tags_by_algorithm: Dict[str, list[dict]] = {}

    def collect_blocking_tags(algorithm_name: str, path: set[str]) -> Dict[str, dict]:
        result: Dict[str, dict] = {}
        for item in derived_inputs_by_algorithm.get(algorithm_name, []):
            tag_key = str(item.get("normalized_tag") or "")
            if tag_key:
                result[tag_key] = {
                    "tag_name": item.get("tag_name"),
                    "normalized_tag": tag_key,
                    "source_algorithm": algorithm_name,
                    "source_row_id": item.get("row_id"),
                    "distance": 0,
                }

            for producer_name in item.get("producer_algorithms", []):
                if producer_name in path:
                    continue
                for upstream_tag, upstream_item in collect_blocking_tags(
                    str(producer_name),
                    path | {str(producer_name)},
                ).items():
                    candidate = dict(upstream_item)
                    candidate["distance"] = int(candidate.get("distance") or 0) + 1
                    existing = result.get(upstream_tag)
                    if existing is None or candidate["distance"] < int(existing.get("distance") or 0):
                        result[upstream_tag] = candidate
        return result

    algorithm_depth: Dict[str, int] = {}

    def depth_for(algorithm_name: str, path: set[str]) -> int:
        parents = direct_upstream.get(algorithm_name, set())
        if not parents:
            return 0
        depths = []
        for parent in parents:
            if parent in path:
                depths.append(0)
            else:
                depths.append(1 + depth_for(parent, path | {parent}))
        return max(depths or [0])

    for algorithm_name in algorithm_names:
        blocking_values = list(collect_blocking_tags(algorithm_name, {algorithm_name}).values())
        blocking_values.sort(key=lambda item: (int(item.get("distance") or 0), item["normalized_tag"]))
        blocking_output_tags_by_algorithm[algorithm_name] = blocking_values
        algorithm_depth[algorithm_name] = depth_for(algorithm_name, {algorithm_name})

    for values in derived_inputs_by_algorithm.values():
        values.sort(key=lambda item: (item["normalized_tag"], item["row_id"]))
    for values in tracked_outputs_by_algorithm.values():
        values.sort(key=lambda item: (item["normalized_tag"], item["row_id"]))

    return {
        "producers_by_tag": output_tags,
        "consumers_by_tag": input_tags,
        "derived_inputs_by_algorithm": derived_inputs_by_algorithm,
        "tracked_outputs_by_algorithm": tracked_outputs_by_algorithm,
        "blocking_output_tags_by_algorithm": blocking_output_tags_by_algorithm,
        "direct_downstream_algorithms_by_algorithm": {
            name: sorted(values) for name, values in direct_downstream.items() if values
        },
        "algorithm_depth": algorithm_depth,
        "cycles": cycles,
        "has_cycles": bool(cycles),
    }


def dependency_info_for_algorithm(dependency_index: dict, algorithm_name: str) -> dict:
    """Returns DAG metadata for one algorithm."""
    if not isinstance(dependency_index, dict):
        dependency_index = {}

    def values_by_algorithm(key: str) -> dict:
        payload = dependency_index.get(key, {})
        return payload if isinstance(payload, dict) else {}

    depth_by_algorithm = values_by_algorithm("algorithm_depth")
    return {
        "derived_input_tags": list(values_by_algorithm("derived_inputs_by_algorithm").get(algorithm_name, [])),
        "tracked_output_tags": list(values_by_algorithm("tracked_outputs_by_algorithm").get(algorithm_name, [])),
        "blocking_output_tags": list(
            values_by_algorithm("blocking_output_tags_by_algorithm").get(algorithm_name, [])
        ),
        "algorithm_depth": int(depth_by_algorithm.get(algorithm_name) or 0),
        "dependency_has_cycles": bool(dependency_index.get("has_cycles")),
        "dependency_cycles": list(dependency_index.get("cycles") or []),
    }


def route_packet_tags(
    raw: str,
    routing_index: Dict[str, list[AlgorithmRow]],
    templates: Dict[str, MappingAlgorithm],
    dependency_index: Optional[dict] = None,
) -> Iterable[str]:
    """
    Разбирает входящий пакет тегов
    Группирует обновления по алгоритмам
    Добавляет в каждое событие актуальный шаблон алгоритма из Kafka-мастер-данных

    Формат входного пакета
    {
      "Timestamp": "2026-03-03T10:25:00Z",
      "Tags": [{"FullName":"...", "Value":..., "Quality":"...", "Type":...}]
    }
    """
    try:
        obj = json.loads(raw)
    except Exception:
        return []
    if not isinstance(obj, dict):
        return []

    timestamp = str(obj.get("Timestamp", "")).strip()
    object_id = str(obj.get("ObjectId", "") or "").strip()
    plant_id = str(obj.get("PlantId", "") or "").strip()
    source_type = str(obj.get("SourceType", "") or "").strip()

    tags = obj.get("Tags")
    if not timestamp or not isinstance(tags, list):
        return []

    grouped_updates: Dict[str, dict] = {}

    for tag in tags:
        if not isinstance(tag, dict):
            continue

        full_name = str(tag.get("FullName", "")).strip()
        if not full_name:
            continue

        routes = routing_index.get(normalize_name(full_name), [])
        if not routes:
            continue

        for route in routes:
            template = templates.get(route.algorithm_name)
            if template is None:
                continue

            dependency_info = dependency_info_for_algorithm(
                dependency_index or {},
                route.algorithm_name,
            )
            algo_payload = grouped_updates.setdefault(
                route.algorithm_name,
                {
                    "plant_id": plant_id,
                    "object_id": object_id,
                    "source_type": source_type,
                    "algorithm_name": route.algorithm_name,
                    "timestamp": timestamp,
                    "source_event_id": obj.get("EventId") or obj.get("Id") or obj.get("MessageId"),
                    "template": mapping_algorithm_to_dict(template),
                    "derived_input_tags": dependency_info["derived_input_tags"],
                    "tracked_output_tags": dependency_info["tracked_output_tags"],
                    "blocking_output_tags": dependency_info["blocking_output_tags"],
                    "algorithm_depth": dependency_info["algorithm_depth"],
                    "dependency_has_cycles": dependency_info["dependency_has_cycles"],
                    "dependency_cycles": dependency_info["dependency_cycles"],
                    "trigger_tags": [],
                    "updates_by_row": {},
                },
            )
            if full_name not in algo_payload["trigger_tags"]:
                algo_payload["trigger_tags"].append(full_name)
            algo_payload["updates_by_row"][route.row_id] = {
                "full_name": full_name,
                "value": tag.get("Value"),
                "quality": tag.get("Quality"),
                "type": tag.get("Type"),
                "timestamp": timestamp,
            }

    routed_events = []
    for item in grouped_updates.values():
        trigger_parts = []
        for row_id, row_update in sorted(item["updates_by_row"].items()):
            trigger_parts.extend(
                [
                    row_id,
                    row_update.get("full_name"),
                    row_update.get("timestamp"),
                    row_update.get("value"),
                    row_update.get("quality"),
                ]
            )
        item["trigger_id"] = deterministic_id(
            "algorithm-trigger",
            item.get("plant_id"),
            item.get("object_id"),
            item.get("source_type"),
            item.get("algorithm_name"),
            item.get("timestamp"),
            *trigger_parts,
        )
        item["trigger_tags"].sort(key=normalize_name)
        routed_events.append(item)

    routed_events.sort(
        key=lambda item: (
            int(item.get("algorithm_depth") or 0),
            str(item.get("algorithm_name") or ""),
        )
    )
    return [json.dumps(item, ensure_ascii=False) for item in routed_events]


def raw_packet_log_context(raw: str) -> Optional[dict]:
    """Возвращает безопасный контекст raw-пакета без значений тегов."""
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    tags = payload.get("Tags")
    tag_names = []
    if isinstance(tags, list):
        for tag in tags:
            if not isinstance(tag, dict):
                continue
            full_name = str(tag.get("FullName", "")).strip()
            if full_name:
                tag_names.append(full_name)

    return {
        "timestamp": str(payload.get("Timestamp", "")).strip(),
        "object_id": str(payload.get("ObjectId", "") or "").strip(),
        "source_type": str(payload.get("SourceType", "") or "").strip(),
        "tag_names": tag_names,
    }


class TraceKafkaRecord(ProcessFunction):
    """Логирует прохождение записи непосредственно после source или перед sink."""

    def __init__(self, stage: str, topic: str):
        self._stage = stage
        self._topic = topic

    def process_element(self, value: str, ctx):
        del ctx
        raw = str(value or "")
        log_step(
            self._stage,
            "record topic=%s utf8_bytes=%d payload=%s",
            self._topic,
            len(raw.encode("utf-8")),
            payload_preview(raw),
        )
        yield value


class RoutePacketTagsByAlgorithmConfig(KeyedBroadcastProcessFunction):
    """
    Маршрутизирует пакеты тегов по актуальному маппингу алгоритмов.
    Маппинг приходит отдельным Kafka-потоком и хранится в broadcast state.
    """

    def __init__(self, db_dsn: str, db_backend: str = "postgres", db_schema: str = "dbo"):
        """Готовит кеш broadcast state и подключение для синхронизации мастер-данных."""
        self._db_backend = db_backend
        self._db_dsn = db_dsn
        self._db_schema = db_schema
        self._repo = None
        self._cached_version: Optional[str] = None
        self._templates: Dict[str, MappingAlgorithm] = {}
        self._routing_index: Dict[str, list[AlgorithmRow]] = {}
        self._dependency_index: dict = {}

    def open(self, runtime_context):
        """Открывает соединение с выбранной БД для upsert маппинга из MasterTopic."""
        del runtime_context

        log_step(
            "ROUTER_OPEN",
            "operator opening db_backend=%s db_schema=%s",
            self._db_backend,
            self._db_schema,
        )
        setup_debugpy_if_enabled(logger)

        self._repo = create_state_repository(self._db_backend, self._db_dsn, self._db_schema)
        self._repo.open()
        self._bootstrap_templates_from_db()
        log_step(
            "ROUTER_OPEN",
            "operator ready cached_version=%s algorithms=%d routing_tags=%d",
            self._cached_version,
            len(self._templates),
            len(self._routing_index),
        )

    def close(self):
        """Закрывает соединение с БД."""
        if self._repo:
            self._repo.close()

    def _sync_templates(self, templates: Dict[str, MappingAlgorithm]):
        """Сразу записывает полученные из Kafka шаблоны алгоритмов в выбранную БД."""
        if self._repo is None:
            raise RuntimeError("Репозиторий состояния не инициализирован.")

        log_step("MASTER_SYNC", "sync to DB templates=%d", len(templates))
        for template in templates.values():
            self._repo.upsert_algorithm_template(template)
        log_step("MASTER_SYNC", "sync to DB completed templates=%d", len(templates))

    def _bootstrap_templates_from_db(self):
        """Loads the last known algorithm mapping from the database before MasterTopic events arrive."""
        if self._repo is None:
            raise RuntimeError("Репозиторий состояния не инициализирован.")

        log_step("MAPPING_BOOTSTRAP", "loading mapping from db_backend=%s", self._db_backend)
        try:
            templates = self._repo.load_algorithms()
        except Exception:
            logger.exception("Не удалось загрузить таблицу маппинга алгоритмов из %s.", self._db_backend)
            return

        if not templates:
            logger.warning("%s не вернул ни одного шаблона алгоритма для bootstrap маппинга.", self._db_backend)
            return

        routing_index = build_routing_index(templates)
        dependency_index = build_dependency_index(templates)
        if dependency_index.get("has_cycles"):
            logger.error("Detected algorithm dependency cycle(s) in DB mapping: %s", dependency_index.get("cycles"))

        self._cached_version = f"db-bootstrap:{now_utc_iso()}"
        self._templates = templates
        self._routing_index = routing_index
        self._dependency_index = dependency_index
        log_mapping_snapshot(
            "MAPPING_BOOTSTRAP",
            self._cached_version,
            self._templates,
            self._routing_index,
        )
        logger.info(
            "Загружено %d шаблонов алгоритмов из %s для bootstrap; routing tags=%d",
            len(self._templates),
            self._db_backend,
            len(self._routing_index),
        )

    def process_broadcast_element(self, value: str, ctx):
        """Обрабатывает мастер-данные из Kafka, пишет их в БД и обновляет broadcast state."""
        log_step(
            "MASTER_OPERATOR",
            "broadcast record received chars=%d payload=%s",
            len(value or ""),
            payload_preview(value),
        )
        version, templates = parse_algorithm_templates(value)
        if not templates:
            log_step("MASTER_OPERATOR", "record produced no mapping templates; broadcast state unchanged")
            return

        self._sync_templates(templates)

        state = ctx.get_broadcast_state(ALGORITHMS_STATE_DESCRIPTOR)
        merged_templates = dict(self._templates) or decode_templates(state.get(ALGORITHMS_TEMPLATES_STATE_KEY))
        for template in templates.values():
            duplicate_keys = [
                key
                for key, active_template in merged_templates.items()
                if active_template.mapping_algorithm_id == template.mapping_algorithm_id
                and key != template.algorithm_name
            ]
            for key in duplicate_keys:
                log_step(
                    "MASTER_OPERATOR",
                    "remove previous mapping key=%s for mapping_id=%s",
                    key,
                    template.mapping_algorithm_id,
                )
                del merged_templates[key]
        merged_templates.update(templates)

        routing_index = build_routing_index(merged_templates)
        dependency_index = build_dependency_index(merged_templates)
        if dependency_index.get("has_cycles"):
            logger.error("Detected algorithm dependency cycle(s): %s", dependency_index.get("cycles"))
        state.put(ALGORITHMS_VERSION_STATE_KEY, version)
        state.put(ALGORITHMS_TEMPLATES_STATE_KEY, encode_templates(merged_templates))
        state.put(
            ALGORITHMS_ROUTING_STATE_KEY,
            json.dumps(
                {
                    tag_name: [algorithm_row_to_dict(row) for row in rows]
                    for tag_name, rows in routing_index.items()
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        state.put(
            ALGORITHMS_DEPENDENCY_STATE_KEY,
            json.dumps(dependency_index, ensure_ascii=False, sort_keys=True),
        )

        self._cached_version = version
        self._templates = merged_templates
        self._routing_index = routing_index
        self._dependency_index = dependency_index
        log_mapping_snapshot("MAPPING_BROADCAST", version, self._templates, self._routing_index)
        logger.info(
            "Обновлено %d шаблонов алгоритма; активные шаблоны =%d",
            len(templates),
            len(self._templates),
        )

    def _refresh_cache_if_needed(self, ctx):
        """Обновляет локальный кеш, если broadcast state получил новую версию маппинга."""
        state = ctx.get_broadcast_state(ALGORITHMS_STATE_DESCRIPTOR)
        version = state.get(ALGORITHMS_VERSION_STATE_KEY)
        if not version or version == self._cached_version:
            log_step(
                "MAPPING_CACHE",
                "cache unchanged broadcast_version=%s cached_version=%s algorithms=%d routing_tags=%d",
                version,
                self._cached_version,
                len(self._templates),
                len(self._routing_index),
            )
            return

        log_step(
            "MAPPING_CACHE",
            "refresh start old_version=%s new_version=%s",
            self._cached_version,
            version,
        )

        templates = decode_templates(state.get(ALGORITHMS_TEMPLATES_STATE_KEY))
        try:
            routing_payload = json.loads(state.get(ALGORITHMS_ROUTING_STATE_KEY) or "{}")
        except Exception:
            routing_payload = {}
        try:
            dependency_index = json.loads(state.get(ALGORITHMS_DEPENDENCY_STATE_KEY) or "{}")
        except Exception:
            dependency_index = {}

        routing_index: Dict[str, list[AlgorithmRow]] = {}
        if isinstance(routing_payload, dict):
            for tag_name, rows in routing_payload.items():
                parsed_rows = []
                if isinstance(rows, list):
                    for row_data in rows:
                        row = algorithm_row_from_dict(row_data)
                        if row is not None:
                            parsed_rows.append(row)
                if parsed_rows:
                    routing_index[str(tag_name)] = parsed_rows

        self._cached_version = version
        self._templates = templates
        self._routing_index = routing_index
        self._dependency_index = dependency_index if isinstance(dependency_index, dict) else {}
        log_mapping_snapshot("MAPPING_CACHE", version, self._templates, self._routing_index)

    def process_element(self, value: str, ctx):
        """Маршрутизирует пакет тегов в события конкретных алгоритмов."""
        log_step("ROUTER", "raw record entered routing operator payload=%s", payload_preview(value))
        self._refresh_cache_if_needed(ctx)

        if not self._routing_index or not self._templates:
            logger.warning(
                "[ORCH][ROUTER] skip raw record because mapping is empty algorithms=%d routing_tags=%d payload=%s",
                len(self._templates),
                len(self._routing_index),
                payload_preview(value),
            )
            return

        routed_events = list(
            route_packet_tags(
                raw=value,
                routing_index=self._routing_index,
                templates=self._templates,
                dependency_index=self._dependency_index,
            )
        )
        packet_context = raw_packet_log_context(value)
        unmatched_tags = []
        if packet_context is not None:
            unmatched_tags = [
                tag_name
                for tag_name in packet_context["tag_names"]
                if normalize_name(tag_name) not in self._routing_index
            ]

        if not routed_events:
            if packet_context is None:
                logger.warning("Получен raw-пакет с невалидным JSON")
            else:
                logger.info(
                    "Raw-пакет получен, но не маршрутизирован: timestamp=%s object_id=%s source_type=%s tags=%s",
                    packet_context["timestamp"],
                    packet_context["object_id"],
                    packet_context["source_type"],
                    packet_context["tag_names"],
                )
            return

        routed_algorithms = []
        for routed in routed_events:
            try:
                routed_algorithm = str(json.loads(routed).get("algorithm_name", "")).strip()
            except Exception:
                routed_algorithm = ""
            if routed_algorithm and routed_algorithm not in routed_algorithms:
                routed_algorithms.append(routed_algorithm)

        logger.info(
            "Raw-пакет маршрутизирован: timestamp=%s object_id=%s source_type=%s events=%d algorithms=%s unmatched_tags=%s",
            packet_context["timestamp"] if packet_context else "",
            packet_context["object_id"] if packet_context else "",
            packet_context["source_type"] if packet_context else "",
            len(routed_events),
            routed_algorithms,
            unmatched_tags,
        )

        for routed in routed_events:
            log_step("ROUTER_OUTPUT", "emit routed event payload=%s", payload_preview(routed))
            yield routed


def get_normalized_value(raw_value: Any):
    """Преобразует входное значение в строку, целое или вещественное число"""

    try:
        return None, None, int(raw_value)
    except (TypeError, ValueError):
        pass

    try:
        return None, float(raw_value), None
    except (TypeError, ValueError):
        pass

    return None if raw_value is None else str(raw_value), None, None


class BuildAndPublishState(KeyedProcessFunction):
    """
    Обрабатывает поток по ключу имени алгоритма
    Обновляет текущие значения тегов
    Публикует готовый снимок состояния
    """

    def __init__(self, db_dsn: str, db_backend: str = "postgres", db_schema: str = "dbo"):
        """Сохраняет строку подключения для записи состояний алгоритмов."""
        self._db_backend = db_backend
        self._db_dsn = db_dsn
        self._db_schema = db_schema

        self._repo = None
        self._template_signatures: Dict[str, str] = {}
        self._pending_derived_state = None
        self._buffered_triggers_state = None

    def open(self, runtime_context):
        """Инициализирует доступ к БД без bootstrap-чтения маппинга алгоритмов."""
        log_step(
            "STATE_OPEN",
            "operator opening db_backend=%s db_schema=%s",
            self._db_backend,
            self._db_schema,
        )
        setup_debugpy_if_enabled(logger)

        self._pending_derived_state = runtime_context.get_state(PENDING_DERIVED_STATE_DESCRIPTOR)
        self._buffered_triggers_state = runtime_context.get_state(BUFFERED_TRIGGERS_STATE_DESCRIPTOR)

        self._repo = create_state_repository(self._db_backend, self._db_dsn, self._db_schema)
        self._repo.open()
        log_step("STATE_OPEN", "operator ready")

    def close(self):
        """Закрывает ресурсы функции"""
        if self._repo:
            self._repo.close()

    @staticmethod
    def _stream_key(event: dict) -> str:
        """Возвращает ключ потока по имени алгоритма"""
        return "|".join(
            [
                str(event.get("plant_id") or ""),
                str(event.get("object_id") or ""),
                str(event.get("source_type") or ""),
            ]
        )

    def _sync_template_if_needed(self, template: MappingAlgorithm, template_payload: dict):
        """Обновляет строки алгоритма в БД только при изменении Kafka-шаблона."""
        if self._repo is None:
            raise RuntimeError("Репозиторий состояния не инициализирован.")

        signature = json.dumps(template_payload, ensure_ascii=False, sort_keys=True)
        if self._template_signatures.get(template.algorithm_name) == signature:
            log_step("STATE_TEMPLATE", "template unchanged algorithm=%s", template.algorithm_name)
            return

        log_step("STATE_TEMPLATE", "template changed; syncing algorithm=%s", template.algorithm_name)
        self._repo.upsert_algorithm_template(template)
        self._template_signatures[template.algorithm_name] = signature
        log_step("STATE_TEMPLATE", "template synced algorithm=%s", template.algorithm_name)

    @staticmethod
    def _json_list_from_state(state) -> list[dict]:
        raw = state.value() if state is not None else None
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except Exception:
            return []
        return payload if isinstance(payload, list) else []

    @staticmethod
    def _put_json_list_to_state(state, values: list[dict]):
        if state is not None:
            state.update(json.dumps(values, ensure_ascii=False, sort_keys=True))

    @staticmethod
    def _event_trigger_id(event: dict) -> str:
        trigger_id = str(event.get("trigger_id") or "").strip()
        if trigger_id:
            return trigger_id

        trigger_parts = []
        updates_by_row = event.get("updates_by_row")
        if isinstance(updates_by_row, dict):
            for row_id, row_update in sorted(updates_by_row.items()):
                if not isinstance(row_update, dict):
                    continue
                trigger_parts.extend(
                    [
                        row_id,
                        row_update.get("full_name"),
                        row_update.get("timestamp"),
                        row_update.get("value"),
                        row_update.get("quality"),
                    ]
                )

        return deterministic_id(
            "algorithm-trigger",
            event.get("plant_id"),
            event.get("object_id"),
            event.get("source_type"),
            event.get("algorithm_name"),
            event.get("timestamp"),
            *trigger_parts,
        )

    @staticmethod
    def _normalized_trigger_tags(event: dict) -> set[str]:
        result = set()
        trigger_tags = event.get("trigger_tags")
        if isinstance(trigger_tags, list):
            for tag_name in trigger_tags:
                normalized = normalize_name(tag_name)
                if normalized:
                    result.add(normalized)

        updates_by_row = event.get("updates_by_row")
        if isinstance(updates_by_row, dict):
            for row_update in updates_by_row.values():
                if not isinstance(row_update, dict):
                    continue
                normalized = normalize_name(row_update.get("full_name"))
                if normalized:
                    result.add(normalized)
        return result

    def _close_pending_for_event(self, event: dict) -> list[dict]:
        trigger_tags = self._normalized_trigger_tags(event)
        if not trigger_tags:
            log_step("DEPENDENCY", "no trigger tags algorithm=%s", event.get("algorithm_name"))
            return []

        timestamp = str(event.get("timestamp", "")).strip()
        pending = self._json_list_from_state(self._pending_derived_state)
        log_step(
            "DEPENDENCY",
            "checking pending closure algorithm=%s timestamp=%s trigger_tags=%s pending=%d",
            event.get("algorithm_name"),
            timestamp,
            sorted(trigger_tags),
            len(pending),
        )
        remaining = []
        closed = []

        for item in pending:
            output_tag = str(item.get("output_tag") or "")
            item_timestamp = item.get("timestamp")
            if output_tag in trigger_tags and same_timestamp(item_timestamp, timestamp):
                closed.append(item)
            else:
                remaining.append(item)

        if closed:
            self._put_json_list_to_state(self._pending_derived_state, remaining)
            logger.info(
                "Closed %d pending derived output(s) for algorithm=%s timestamp=%s tags=%s",
                len(closed),
                event.get("algorithm_name"),
                timestamp,
                sorted(trigger_tags),
            )
        else:
            log_step("DEPENDENCY", "no pending outputs closed remaining=%d", len(remaining))
        return closed

    def _blocking_pending_for_event(self, event: dict) -> list[dict]:
        blocking_output_tags = event.get("blocking_output_tags")
        if not isinstance(blocking_output_tags, list) or not blocking_output_tags:
            blocking_output_tags = event.get("derived_input_tags")
        if not isinstance(blocking_output_tags, list) or not blocking_output_tags:
            log_step("DEPENDENCY", "algorithm=%s has no blocking output tags", event.get("algorithm_name"))
            return []

        timestamp = str(event.get("timestamp", "")).strip()
        blocking_tags = {
            str(item.get("normalized_tag") or "")
            for item in blocking_output_tags
            if isinstance(item, dict) and item.get("normalized_tag")
        }
        if not timestamp or not blocking_tags:
            log_step(
                "DEPENDENCY",
                "blocking check skipped algorithm=%s timestamp=%s blocking_tags=%s",
                event.get("algorithm_name"),
                timestamp,
                sorted(blocking_tags),
            )
            return []

        result = []
        for item in self._json_list_from_state(self._pending_derived_state):
            output_tag = str(item.get("output_tag") or "")
            if output_tag in blocking_tags and timestamp_lte(item.get("timestamp"), timestamp):
                result.append(item)
        log_step(
            "DEPENDENCY",
            "blocking check algorithm=%s timestamp=%s tags=%s blocking_pending=%d",
            event.get("algorithm_name"),
            timestamp,
            sorted(blocking_tags),
            len(result),
        )
        return result

    def _buffer_event(self, event: dict, blocking_pending: list[dict]):
        buffered = self._json_list_from_state(self._buffered_triggers_state)
        trigger_id = self._event_trigger_id(event)
        item = {
            "id": trigger_id,
            "event": event,
            "blocking_pending_ids": [str(item.get("id") or "") for item in blocking_pending],
            "buffered_at": now_utc_iso(),
        }

        replaced = False
        for index, existing in enumerate(buffered):
            if str(existing.get("id") or "") == trigger_id:
                buffered[index] = item
                replaced = True
                break
        if not replaced:
            buffered.append(item)

        self._put_json_list_to_state(self._buffered_triggers_state, buffered)
        logger.info(
            "Buffered algorithm trigger algorithm=%s timestamp=%s trigger_id=%s blocking=%d",
            event.get("algorithm_name"),
            event.get("timestamp"),
            trigger_id,
            len(blocking_pending),
        )

    def _register_pending_outputs(self, event: dict):
        tracked_output_tags = event.get("tracked_output_tags")
        if not isinstance(tracked_output_tags, list) or not tracked_output_tags:
            log_step("DEPENDENCY", "algorithm=%s has no tracked outputs", event.get("algorithm_name"))
            return

        timestamp = str(event.get("timestamp", "")).strip()
        trigger_id = self._event_trigger_id(event)
        pending = self._json_list_from_state(self._pending_derived_state)
        existing_ids = {str(item.get("id") or "") for item in pending}
        added = 0

        for tag_info in tracked_output_tags:
            if not isinstance(tag_info, dict):
                continue
            output_tag = str(tag_info.get("normalized_tag") or "").strip()
            if not output_tag:
                continue

            pending_id = deterministic_id("pending-derived", trigger_id, output_tag, timestamp)
            if pending_id in existing_ids:
                continue

            pending.append(
                {
                    "id": pending_id,
                    "output_tag": output_tag,
                    "output_tag_name": tag_info.get("tag_name"),
                    "timestamp": timestamp,
                    "producer_algorithm": event.get("algorithm_name"),
                    "producer_trigger_id": trigger_id,
                    "created_at": now_utc_iso(),
                }
            )
            existing_ids.add(pending_id)
            added += 1

        if added:
            self._put_json_list_to_state(self._pending_derived_state, pending)
            logger.info(
                "Registered %d pending derived output(s) for algorithm=%s timestamp=%s trigger_id=%s",
                added,
                event.get("algorithm_name"),
                timestamp,
                trigger_id,
            )
        else:
            log_step(
                "DEPENDENCY",
                "no pending outputs added algorithm=%s tracked=%d existing_pending=%d",
                event.get("algorithm_name"),
                len(tracked_output_tags),
                len(pending),
            )

    def _publish_event(self, event: dict) -> list[str]:
        algorithm_name = str(event.get("algorithm_name", "")).strip()
        timestamp = str(event.get("timestamp", "")).strip() or now_utc_iso()
        updates_by_row = event.get("updates_by_row")
        plant_id = str(event.get("plant_id", "")).strip() or None
        source_type = str(event.get("source_type", "")).strip() or None
        object_id = str(event.get("object_id", "")).strip() or None

        log_step(
            "STATE_BUILD",
            "start algorithm=%s timestamp=%s plant_id=%s object_id=%s source_type=%s updates_type=%s",
            algorithm_name,
            timestamp,
            plant_id,
            object_id,
            source_type,
            type(updates_by_row).__name__,
        )

        if not algorithm_name or not isinstance(updates_by_row, dict) or not updates_by_row:
            logger.warning(
                "[ORCH][STATE_BUILD] invalid routed event algorithm=%s updates=%s payload=%s",
                algorithm_name,
                updates_by_row,
                payload_preview(json.dumps(event, ensure_ascii=False, default=str)),
            )
            return []

        template_payload = event.get("template")
        template = mapping_algorithm_from_dict(template_payload)
        if template is None:
            logger.warning("Skipping algorithm '%s' because mapping template is missing", algorithm_name)
            return []
        if template.algorithm_name != algorithm_name:
            logger.warning(
                "Skipping algorithm event '%s': template belongs to '%s'",
                algorithm_name,
                template.algorithm_name,
            )
            return []

        normalized_updates: Dict[str, dict] = {}
        for row_id, row_update in updates_by_row.items():
            if not isinstance(row_update, dict):
                continue
            v_str, v_dbl, v_int = get_normalized_value(row_update.get("value"))
            normalized_updates[str(row_id)] = {
                "value_string": v_str,
                "value_double": v_dbl,
                "value_int": v_int,
                "quality": row_update.get("quality"),
                "created": row_update.get("timestamp") or timestamp,
            }

        if not normalized_updates:
            logger.warning("[ORCH][STATE_BUILD] no valid updates algorithm=%s", algorithm_name)
            return []

        log_step(
            "STATE_BUILD",
            "updates normalized algorithm=%s rows=%d row_ids=%s required_rows=%d",
            algorithm_name,
            len(normalized_updates),
            sorted(normalized_updates),
            len(template.required_input_row_ids),
        )

        if self._repo is None:
            logger.error("State repository is not initialized.")
            return []

        self._sync_template_if_needed(template, template_payload)

        log_step("STATE_BUILD", "calling repository.publish_complete_state algorithm=%s", algorithm_name)

        published_state = self._repo.publish_complete_state(
            mapping_algorithm_id=template.mapping_algorithm_id,
            start_time=timestamp,
            required_row_ids=template.required_input_row_ids,
            updates_by_row=normalized_updates,
        )

        if published_state is None:
            logger.info(
                "Состояние алгоритма не опубликовано: algorithm=%s timestamp=%s required_rows=%d updates=%d",
                algorithm_name,
                timestamp,
                len(template.required_input_row_ids),
                len(normalized_updates),
            )
            return []

        if not published_state.get("inserted", True):
            logger.info(
                "Существующее состояние алгоритма будет повторно опубликовано: algorithm=%s timestamp=%s state_id=%s",
                algorithm_name,
                timestamp,
                published_state.get("state", {}).get("id"),
            )

        state_payload = dict(published_state["state"])
        state_payload["algorithm_name"] = algorithm_name
        published_details = list(published_state["details"])
        published_details.extend(build_output_details(template.output_rows))
        output_payload = {
            "event": "state_published",
            "publish_time": timestamp,
            "algorithm_state": state_payload,
            "details": published_details,
            "source_type": source_type,
            "plant_id": plant_id,
            "object_id": object_id,
            "trigger_id": self._event_trigger_id(event),
            "trigger_tags": event.get("trigger_tags") or [],
        }
        logger.info(
            "Сформировано сообщение для preprocessed: algorithm=%s timestamp=%s state_id=%s details=%d",
            algorithm_name,
            timestamp,
            state_payload.get("id"),
            len(published_details),
        )
        output_json = json.dumps(output_payload, ensure_ascii=False)
        log_step("STATE_OUTPUT", "emit preprocessed record payload=%s", payload_preview(output_json))
        return [output_json]

    def _handle_event(self, event: dict, release_buffered: bool = True) -> list[str]:
        log_step(
            "STATE_HANDLE",
            "start algorithm=%s timestamp=%s trigger_id=%s release_buffered=%s",
            event.get("algorithm_name"),
            event.get("timestamp"),
            self._event_trigger_id(event),
            release_buffered,
        )
        self._close_pending_for_event(event)
        blocking_pending = self._blocking_pending_for_event(event)
        if blocking_pending:
            self._buffer_event(event, blocking_pending)
            log_step("STATE_HANDLE", "event buffered outputs=0")
            return []

        outputs = self._publish_event(event)
        if outputs:
            self._register_pending_outputs(event)

        if release_buffered:
            outputs.extend(self._release_ready_buffered())
        log_step("STATE_HANDLE", "finish outputs=%d", len(outputs))
        return outputs

    def _release_ready_buffered(self) -> list[str]:
        outputs = []

        log_step(
            "DEPENDENCY",
            "release scan buffered=%d",
            len(self._json_list_from_state(self._buffered_triggers_state)),
        )

        while True:
            buffered = self._json_list_from_state(self._buffered_triggers_state)
            ready_index = None
            ready_event = None

            for index, item in enumerate(buffered):
                event = item.get("event") if isinstance(item, dict) else None
                if not isinstance(event, dict):
                    ready_index = index
                    ready_event = None
                    break
                if not self._blocking_pending_for_event(event):
                    ready_index = index
                    ready_event = event
                    break

            if ready_index is None:
                log_step("DEPENDENCY", "no ready buffered event; release scan finished outputs=%d", len(outputs))
                break

            buffered.pop(ready_index)
            self._put_json_list_to_state(self._buffered_triggers_state, buffered)
            if ready_event is None:
                continue

            logger.info(
                "Releasing buffered algorithm trigger algorithm=%s timestamp=%s trigger_id=%s",
                ready_event.get("algorithm_name"),
                ready_event.get("timestamp"),
                self._event_trigger_id(ready_event),
            )
            outputs.extend(self._handle_event(ready_event, release_buffered=False))

        return outputs

    def process_element(self, value: str, ctx):
        """Processes one routed algorithm event and emits ready snapshots."""
        del ctx

        log_step("STATE_INPUT", "routed record received payload=%s", payload_preview(value))
        try:
            event = json.loads(value)
        except Exception as exc:
            logger.warning(
                "[ORCH][STATE_INPUT] invalid routed JSON error=%s payload=%s",
                exc,
                payload_preview(value),
            )
            return
        if not isinstance(event, dict):
            logger.warning("[ORCH][STATE_INPUT] routed root is not object type=%s", type(event).__name__)
            return

        outputs = self._handle_event(event)
        log_step(
            "STATE_INPUT",
            "routed record handled algorithm=%s outputs=%d",
            event.get("algorithm_name"),
            len(outputs),
        )
        for output in outputs:
            log_step("STATE_OUTPUT", "yield record payload=%s", payload_preview(output))
            yield output

def main():
    """Собирает и запускает задачу Flink"""
    log_step("JOB", "building OrchestratorAlgorithms job")
    config = Configuration()
    config.set_string("flink.partition-discovery.interval-millis", "30000")

    if JobConfig.CheckpointDir:
        config.set_string("state.checkpoints.dir", normalize_fs_uri(JobConfig.CheckpointDir))
    if JobConfig.SavepointDir:
        config.set_string("state.savepoints.dir", normalize_fs_uri(JobConfig.SavepointDir))

    local_debug = env_flag("FLINK_LOCAL_DEBUG", False)
    env = StreamExecutionEnvironment.get_execution_environment(config)

    env.set_parallelism(1)
    env.enable_checkpointing(JobConfig.CheckpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    ck = env.get_checkpoint_config()
    ck.set_min_pause_between_checkpoints(10_000)
    ck.set_checkpoint_timeout(120_000)
    ck.set_max_concurrent_checkpoints(1)
    ck.set_tolerable_checkpoint_failure_number(3)
    ck.set_externalized_checkpoint_retention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)

    db_backend = normalize_db_backend(MASTER_DB_BACKEND)
    db_schema = MASTER_DB_SCHEMA
    db_dsn = MASTER_DB_DSN

    events_group_id = os.getenv(
        "MASTER_EVENTS_GROUP_ID",
        "master_state_builder_debug" if local_debug else "master_state_builder",
    )
    algorithms_group_id = os.getenv(
        "MASTER_ALGORITHMS_GROUP_ID",
        "master_algorithms_debug" if local_debug else "master_algorithms",
    )

    events_start_from = os.getenv("MASTER_EVENTS_START_FROM", "latest")
    algorithms_start_from = os.getenv("MASTER_ALGORITHMS_START_FROM", "latest")
    algorithms_topic = os.getenv("MASTER_ALGORITHMS_TOPIC", JobConfig.MasterTopic)
    snapshot_topic = os.getenv("MASTER_SNAPSHOT_TOPIC") or os.getenv("MASTER_RESULT_TOPIC") or JobConfig.ResultTopic
    enable_kafka_sink = env_flag("MASTER_ENABLE_KAFKA_SINK", True)

    logger.info(
        "Master job: raw_topic=%s raw_group_id=%s raw_start_from=%s algorithms_topic=%s algorithms_group_id=%s algorithms_start_from=%s db_backend=%s snapshot_topic=%s sink_enabled=%s",
        JobConfig.RawTopic,
        events_group_id,
        events_start_from,
        algorithms_topic,
        algorithms_group_id,
        algorithms_start_from,
        db_backend,
        snapshot_topic,
        enable_kafka_sink,
    )
    log_step(
        "JOB",
        "configuration bootstrap_server=%s raw_topic=%s raw_group=%s raw_start=%s master_topic=%s master_group=%s master_start=%s preprocessed_topic=%s sink_enabled=%s",
        JobConfig.BootstrapServer,
        JobConfig.RawTopic,
        events_group_id,
        events_start_from,
        algorithms_topic,
        algorithms_group_id,
        algorithms_start_from,
        snapshot_topic,
        enable_kafka_sink,
    )

    events_source = build_kafka_source(
        bootstrap_servers=JobConfig.BootstrapServer,
        topic=JobConfig.RawTopic,
        group_id=events_group_id,
        start_from=events_start_from,
    )
    log_step("JOB", "raw KafkaSource configured topic=%s group=%s", JobConfig.RawTopic, events_group_id)

    algorithms_source = build_kafka_source(
        bootstrap_servers=JobConfig.BootstrapServer,
        topic=algorithms_topic,
        group_id=algorithms_group_id,
        start_from=algorithms_start_from,
    )
    log_step("JOB", "master KafkaSource configured topic=%s group=%s", algorithms_topic, algorithms_group_id)

    raw_stream = env.from_source(
        events_source,
        WatermarkStrategy.no_watermarks(),
        "tags-source",
    ).uid("tags-source-v1")

    raw_stream = raw_stream.process(
        TraceKafkaRecord("RAW_SOURCE", JobConfig.RawTopic),
        output_type=Types.STRING(),
    ).uid("trace-tags-source-v1")

    algorithms_stream = env.from_source(
        algorithms_source,
        WatermarkStrategy.no_watermarks(),
        "algorithms-source",
    ).uid("algorithms-source-v1")

    algorithms_stream = algorithms_stream.process(
        TraceKafkaRecord("MASTER_SOURCE", algorithms_topic),
        output_type=Types.STRING(),
    ).uid("trace-algorithms-source-v1")

    algorithms_broadcast = algorithms_stream.broadcast(ALGORITHMS_STATE_DESCRIPTOR)
    raw_keyed = raw_stream.key_by(lambda _: "algorithm-routing", key_type=Types.STRING())

    routed = raw_keyed.connect(algorithms_broadcast).process(
        RoutePacketTagsByAlgorithmConfig(
            db_backend=db_backend,
            db_dsn=db_dsn,
            db_schema=db_schema,
        ),
        output_type=Types.STRING(),
    ).uid("route-packet-tags-v2")
    log_step("JOB", "routing operator configured uid=route-packet-tags-v2")

    published = (
        routed
        .key_by(lambda raw: BuildAndPublishState._stream_key(json.loads(raw)), key_type=Types.STRING())
        .process(
            BuildAndPublishState(
                db_backend=db_backend,
                db_dsn=db_dsn,
                db_schema=db_schema,
            ),
            output_type=Types.STRING(),
        )
        .uid("build-and-publish-state-v1")
    )
    log_step("JOB", "state builder configured uid=build-and-publish-state-v1")

    published.print()
    log_step("JOB", "stdout print sink configured for preprocessed records")

    if enable_kafka_sink:
        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(JobConfig.BootstrapServer)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(snapshot_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
        )
        sink_input = published.process(
            TraceKafkaRecord("PREPROCESSED_SINK_INPUT", snapshot_topic),
            output_type=Types.STRING(),
        ).uid("trace-preprocessed-sink-v1")
        sink_input.sink_to(sink).uid("snapshot-sink-v1")
        log_step("JOB", "KafkaSink configured topic=%s uid=snapshot-sink-v1", snapshot_topic)
    else:
        logger.info("Kafka snapshot sink is disabled (MASTER_ENABLE_KAFKA_SINK=0).")

    log_step("JOB", "submitting Flink job name=OrchestratorAlgorithms")
    env.execute("OrchestratorAlgorithms")
    print()


if __name__ == "__main__":
    main()
