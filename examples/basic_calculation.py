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