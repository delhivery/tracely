import re
import copy
import pytest
from unittest.mock import patch

from src.tracely import constants
from src.tracely.clean_trace import CleanTrace
from src.tracely.exceptions.custom_exceptions import ValidationException
from tests.testing_utils import load_trace_payload


valid_output_trace = {
    "cleaned_trace": [{"ping_id": "1533",
    "input_latitude": 19.052419,
    "input_longitude": 73.072199,
    "timestamp": 1706755887680,
    "error_radius": 24.9,
    "event_type": '"event_a"',
    "force_retain": True,
    "cleaned_latitude": 19.052441,
    "cleaned_longitude": 73.072202,
    "update_status": "updated",
    "last_updated_by": "map_match_trace",
    "stop_event_status": False,
    "cumulative_stop_event_time": "0 minutes and 0 seconds",
    "representative_stop_event_latitude": None,
    "representative_stop_event_longitude": None,
    "stop_event_sequence_number": -1,
    "time_since_prev_ping": 0.0,
    "dist_from_prev_ping": 0.0,
    "cleaned_trace_cumulative_dist": 0.0,
    "cleaned_trace_cumulative_time": 0.0,
    "metadata": {"meta_field_1": "meta_value_1"}}],
    
    "cleaning_summary": {"total_pings_in_input": 3114,
    "total_non_null_pings_in_input": 2965,
    "total_non_null_pings_in_output": 1644,
    "total_trace_time": "7 hours, 24 minutes and 38 seconds",
    "unchanged_percentage": 41.28,
    "drop_percentage": 44.55,
    "updation_percentage": 14.17,
    "interpolation_percentage": 0.0,
    "total_execution_time": 0.32049},
    
    "distance_summary": {"cumulative_distance_of_raw_trace": 36788.37,
    "cumulative_distance_of_clean_trace": 15994.22,
    "percent_reduction_in_dist": 56.52},
    
    "stop_summary": {"stop_events_info": [{"stop_event_sequence_number": 1,
    "start_time": "2024-02-01 08:21:27",
    "end_time": "2024-02-01 08:26:13",
    "total_stop_event_time": "0 hours, 4 minutes and 46 seconds",
    "number_of_pings": 34,
    "representative_latitude": 19.052406794117648,
    "representative_longitude": 73.07219488235295}],

    "global_stop_events_info": {"total_trace_time": "7 hours, 24 minutes and 38 seconds",
    "total_stop_events_time": "0 hours, 0 minutes and 0 seconds",
    "stop_event_percentage": 0.0}}
    }


#################################################
# Check missing mandatory keys
#################################################

# Cleaning summary
def _test_missing_key_from_cleaned_trace(key):
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaned_trace to manipulate
    cleaned_trace_copy = copy.deepcopy(valid_output_trace["cleaned_trace"])

    # Remove the specified key
    cleaned_trace_copy[0].pop(key)
    expected_error_msg = re.escape(f'("Expected key: \'{key}\' missing from the dictionary", 4001)')
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_trace", return_value=cleaned_trace_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_successful_output():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)
    trace_data_obj.add_stop_events_info()
    
    trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()
    
    for ping in trace_cleaning_output["cleaned_trace"]:
        assert len(list(ping.keys())) == constants.CLEAN_PING_KEYS_COUNT

        if ping["stop_event_status"] == False:
            assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
            assert ping["stop_event_sequence_number"] == -1
            assert ping["representative_stop_event_latitude"] == None
            assert ping["representative_stop_event_longitude"] == None

        else:
            assert ping["stop_event_sequence_number"] >= 1
            assert isinstance(ping["representative_stop_event_latitude"],(int, float))
            assert isinstance(ping["representative_stop_event_longitude"],(int, float))


def test_missing_ping_id():
    """Test output payload with ping_id mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("ping_id")


def test_missing_input_latitude():
    """Test output payload with input_latitude mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("input_latitude")


def test_missing_input_longitude():
    """Test output payload with input_longitude mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("input_longitude")


def test_missing_timestamp():
    """Test output payload with timestamp mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("timestamp")


def test_missing_error_radius():
    """Test output payload with error_radius mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("error_radius")


def test_missing_event_type():
    """Test output payload with event_type mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("event_type")


def test_missing_force_retain():
    """Test output payload with force_retain mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("force_retain")


def test_missing_cleaned_latitude():
    """Test output payload with cleaned_latitude mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("cleaned_latitude")


def test_missing_cleaned_longitude():
    """Test output payload with cleaned_longitude mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("cleaned_longitude")


def test_missing_update_status():
    """Test output payload with update_status mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("update_status")


def test_missing_last_updated_by():
    """Test output payload with last_updated_by mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("last_updated_by")


def test_missing_stop_event_status():
    """Test output payload with stop_event_status mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("stop_event_status")


def test_missing_cumulative_stop_event_time():
    """Test output payload with cumulative_stop_event_time mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("cumulative_stop_event_time")


def test_missing_representative_stop_event_latitude():
    """Test output payload with representative_stop_event_latitude mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("representative_stop_event_latitude")


def test_missing_representative_stop_event_longitude():
    """Test output payload with representative_stop_event_longitude mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("representative_stop_event_longitude")


def test_missing_stop_event_sequence_number_in_cleaned_trace():
    """Test output payload with stop_event_sequence_number mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("stop_event_sequence_number")


def test_missing_time_since_prev_ping():
    """Test output payload with time_since_prev_ping mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("time_since_prev_ping")


def test_missing_dist_from_prev_ping():
    """Test output payload with dist_from_prev_ping mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("dist_from_prev_ping")


def test_missing_cleaned_trace_cumulative_dist():
    """Test output payload with cleaned_trace_cumulative_dist mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("cleaned_trace_cumulative_dist")


def test_missing_cleaned_trace_cumulative_time():
    """Test output payload with cleaned_trace_cumulative_time mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("cleaned_trace_cumulative_time")


def test_missing_metadata():
    """Test output payload with metadata mandatory key missing from cleaned_trace."""
    
    _test_missing_key_from_cleaned_trace("metadata")


# Cleaning summary
def _test_missing_key_from_cleaning_summary(key):
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaning_summary to manipulate
    cleaning_summary_copy = copy.deepcopy(valid_output_trace["cleaning_summary"])

    # Remove the specified key
    cleaning_summary_copy.pop(key)
    expected_error_msg = re.escape(
        f'("Expected key: \'{key}\' missing from the dictionary", 4001)')
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_cleaning_summary", return_value=cleaning_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_missing_total_pings_in_input():
    """Test output payload with total_pings_in_input mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("total_pings_in_input")


def test_missing_total_non_null_pings_in_input():
    """Test output payload with total_not_null_pings mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("total_non_null_pings_in_input")


def test_missing_total_non_null_pings_in_output():
    """Test output payload with total_non_null_pings_in_output mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("total_non_null_pings_in_output")


def test_missing_total_trace_time():
    """Test output payload with total_trace_time mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("total_trace_time")


def test_missing_unchanged_percentage():
    """Test output payload with unchanged_percentage mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("unchanged_percentage")


def test_missing_drop_percentage():
    """Test output payload with drop_percentage mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("drop_percentage")


def test_missing_updation_percentage():
    """Test output payload with updation_percentage mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("updation_percentage")


def test_missing_interpolation_percentage():
    """Test output payload with interpolation_percentage mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("interpolation_percentage")


def test_missing_total_execution_time():
    """Test output payload with total_execution_time mandatory key missing from cleaning_summary."""
    
    _test_missing_key_from_cleaning_summary("total_execution_time")


# Distance summary
def _test_missing_key_from_distance_summary(key):
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the distance_summary to manipulate
    distance_summary_copy = copy.deepcopy(valid_output_trace["distance_summary"])

    # Remove the specified key
    distance_summary_copy.pop(key)
    expected_error_msg = re.escape(f'("Expected key: \'{key}\' missing from the dictionary", 4001)')
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_distance_summary", return_value=distance_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_missing_cumulative_distance_of_raw_trace():
    """Test output payload with cumulative_distance_of_raw_trace mandatory key missing from distance_summary."""
    
    _test_missing_key_from_distance_summary("cumulative_distance_of_raw_trace")


def test_missing_cumulative_distance_of_clean_trace():
    """Test output payload with cumulative_distance_of_clean_trace mandatory key missing from distance_summary."""
    
    _test_missing_key_from_distance_summary("cumulative_distance_of_clean_trace")


def test_missing_percent_reduction_in_dist():
    """Test output payload with percent_reduction_in_dist mandatory key missing from distance_summary."""
    
    _test_missing_key_from_distance_summary("percent_reduction_in_dist")


# Stop summary
def _test_missing_key_from_stop_summary(key):
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])

    # Remove the specified key
    stop_summary_copy.pop(key)
    expected_error_msg = re.escape(f'("Expected key: \'{key}\' missing from the dictionary", 4001)')
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_missing_percent_stop_events_info():
    """Test output payload with stop_events_info mandatory key missing from stop_summary."""
    
    _test_missing_key_from_stop_summary("stop_events_info")


def test_missing_global_stop_events_info():
    """Test output payload with global_stop_events_info mandatory key missing from stop_summary."""
    
    _test_missing_key_from_stop_summary("global_stop_events_info")


def _test_missing_key_from_global_stop_events_info(key):
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])

    # Remove the specified key
    stop_summary_copy["global_stop_events_info"].pop(key)
    expected_error_msg = re.escape(f'("Expected key: \'{key}\' missing from the dictionary", 4001)')
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_missing_total_trace_time_in_global_stop_events_info():
    """Test output payload with total_trace_time mandatory key missing from global_stop_events_info."""
    
    _test_missing_key_from_global_stop_events_info("total_trace_time")


def test_missing_total_stop_events_time():
    """Test output payload with total_stop_events_time mandatory key missing from global_stop_events_info."""
    
    _test_missing_key_from_global_stop_events_info("total_stop_events_time")


def test_missing_stop_event_percentage():
    """Test output payload with stop_event_percentage mandatory key missing from global_stop_events_info."""
    
    _test_missing_key_from_global_stop_events_info("stop_event_percentage")


def _test_missing_key_from_stop_events_info(key):
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])

    # Remove the specified key
    stop_summary_copy["stop_events_info"][0].pop(key)
    expected_error_msg = re.escape(f'("Expected key: \'{key}\' missing from the dictionary", 4001)')
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_missing_stop_event_sequence_number():
    """Test output payload with stop_event_sequence_number mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("stop_event_sequence_number")


def test_missing_start_time():
    """Test output payload with start_time mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("start_time")


def test_missing_end_time():
    """Test output payload with end_time mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("end_time")


def test_missing_total_stop_event_time():
    """Test output payload with total_stop_event_time mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("total_stop_event_time")


def test_missing_number_of_pings():
    """Test output payload with number_of_pings mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("number_of_pings")


def test_missing_representative_latitude():
    """Test output payload with representative_latitude mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("representative_latitude")


def test_missing_representative_longitude():
    """Test output payload with representative_longitude mandatory key missing from stop_events_info."""
    
    _test_missing_key_from_stop_events_info("representative_longitude")


#################################################
# Check datatype of keys
#################################################

def _test_invalid_column_value_in_cleaned_trace(column_info, error_msg):
    """Test output payload with a rigged column_value for a specific column_name."""
    
    # Extract the column name and the rigged value
    column_name, column_value = list(column_info.items())[0]

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaned_trace to manipulate
    cleaned_trace_copy = copy.deepcopy(valid_output_trace["cleaned_trace"])
    
    # Assign the rigged value to the specified column_name
    cleaned_trace_copy[0][column_name] = column_value

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_trace", return_value=cleaned_trace_copy):
        with pytest.raises(ValidationException, match=error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def _test_invalid_column_value_in_cleaning_summary(column_info, error_msg):
    """Test output payload with a rigged column_value for a specific column_name."""
    
    # Extract the column name and the rigged value
    column_name, column_value = list(column_info.items())[0]

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaning_summary to manipulate
    cleaning_summary_copy = copy.deepcopy(valid_output_trace["cleaning_summary"])
    
    # Assign the rigged value to the specified column_name
    cleaning_summary_copy[column_name] = column_value

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_cleaning_summary", return_value=cleaning_summary_copy):
        with pytest.raises(ValidationException, match=error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def _test_invalid_column_value_in_distance_summary(column_info, error_msg):
    """Test output payload with a rigged column_value for a specific column_name."""
    
    # Extract the column name and the rigged value
    column_name, column_value = list(column_info.items())[0]

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the distance_summary to manipulate
    distance_summary_copy = copy.deepcopy(valid_output_trace["distance_summary"])
    
    # Assign the rigged value to the specified column_name
    distance_summary_copy[column_name] = column_value

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_distance_summary", return_value=distance_summary_copy):
        with pytest.raises(ValidationException, match=error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def _test_invalid_column_value_in_stop_summary(column_info, error_msg):
    """Test output payload with a rigged column_value for a specific column_name."""
    
    # Extract the column name and the rigged value
    column_name, column_value = list(column_info.items())[0]

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])
    
    # Assign the rigged value to the specified column_name
    stop_summary_copy[column_name] = column_value

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def _test_invalid_column_value_in_stop_events_info(column_info,error_msg):
    """Test output payload with a rigged column_value for a specific column_name."""
    
    # Extract the column name and the rigged value
    column_name, column_value = list(column_info.items())[0]

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])
    
    # Assign the rigged value to the specified column_name
    stop_summary_copy["stop_events_info"][0][column_name] = column_value

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def _test_invalid_column_value_in_global_stop_events_info(column_info, error_msg):
    """Test output payload with a rigged column_value for a specific column_name."""
    
    # Extract the column name and the rigged value
    column_name, column_value = list(column_info.items())[0]

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])
    
    # Assign the rigged value to the specified column_name
    stop_summary_copy["global_stop_events_info"][column_name] = column_value

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()

# Test function using _test_invalid_column_value_in_cleaned_trace
def test_ping_id_of_invalid_dtype():
    """Test output payload with an invalid ping_id."""
    
    error_msg = re.escape('("ping_id must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"ping_id": 0}, error_msg)


def test_input_latitude_of_invalid_dtype():
    """Test output payload with an invalid input_latitude."""
    
    error_msg = re.escape('("input_latitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"input_latitude": "999.999999"}, error_msg)


def test_input_longitude_of_invalid_dtype():
    """Test output payload with an invalid input_longitude."""
    
    error_msg = re.escape('("input_longitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"input_longitude": "999.999999"}, error_msg)


def test_timestamp_of_invalid_dtype():
    """Test output payload with an invalid timestamp."""
    
    error_msg = re.escape('("timestamp must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"timestamp": "invalid_timestamp"}, error_msg)


def test_error_radius_of_invalid_dtype():
    """Test output payload with an invalid error_radius."""
    
    error_msg = re.escape('("error_radius must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"error_radius": "-1"}, error_msg)


def test_event_type_of_invalid_dtype():
    """Test output payload with an invalid event_type."""
    
    error_msg = re.escape('("event_type must be of type String or None but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"event_type": 0}, error_msg)


def test_force_retain_of_invalid_dtype():
    """Test output payload with an invalid force_retain."""
    
    error_msg = re.escape('("force_retain must be of type Bool but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"force_retain": "not_a_boolean"}, error_msg)  


def test_cleaned_latitude_of_invalid_dtype():
    """Test output payload with an invalid cleaned_latitude."""
    
    error_msg = re.escape('("cleaned_latitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"cleaned_latitude": "999.999999"}, error_msg)  


def test_cleaned_longitude_of_invalid_dtype():
    """Test output payload with an invalid cleaned_longitude."""
    
    error_msg = re.escape('("cleaned_longitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"cleaned_longitude": "999.999999"}, error_msg)  


def test_update_status_of_invalid_dtype():
    """Test output payload with an invalid update_status."""
    
    error_msg = re.escape('("update_status must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"update_status": 0}, error_msg)  


def test_last_updated_by_of_invalid_dtype():
    """Test output payload with an invalid last_updated_by."""
    
    error_msg = re.escape('("last_updated_by must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"last_updated_by": 0}, error_msg)  


def test_stop_event_status_of_invalid_dtype():
    """Test output payload with an invalid stop_event_status."""
    
    error_msg = re.escape('("stop_event_status must be of type Bool but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"stop_event_status": "not_a_boolean"}, error_msg)  


def test_cumulative_stop_event_time_of_invalid_dtype():
    """Test output payload with an invalid cumulative_stop_event_time."""
    
    error_msg = re.escape('("cumulative_stop_event_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"cumulative_stop_event_time": -1}, error_msg)  


def test_representative_stop_event_latitude_of_invalid_dtype():
    """Test output payload with an invalid representative_stop_event_latitude."""
    
    error_msg = re.escape('("representative_stop_event_latitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"representative_stop_event_latitude": "999.999999"}, error_msg)   


def test_representative_stop_event_longitude_of_invalid_dtype():
    """Test output payload with an invalid representative_stop_event_longitude."""
    
    error_msg = re.escape('("representative_stop_event_longitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"representative_stop_event_longitude": "999.99999"}, error_msg)  


def test_stop_event_sequence_number_of_invalid_dtype_in_cleaned_trace():
    """Test output payload with an invalid stop_event_sequence_number."""
    
    error_msg = re.escape('("stop_event_sequence_number must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"stop_event_sequence_number": "invalid_sequence"}, error_msg)  


def test_time_since_prev_ping_of_invalid_dtype():
    """Test output payload with an invalid time_since_prev_ping."""
    
    error_msg = re.escape('("time_since_prev_ping must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"time_since_prev_ping": "invalid_time"}, error_msg)  


def test_dist_from_prev_ping_of_invalid_dtype():
    """Test output payload with an invalid dist_from_prev_ping."""
    
    error_msg = re.escape('("dist_from_prev_ping must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"dist_from_prev_ping": "-1"}, error_msg)  


def test_cleaned_trace_cumulative_dist_of_invalid_dtype():
    """Test output payload with an invalid cleaned_trace_cumulative_dist."""
    
    error_msg = re.escape('("cleaned_trace_cumulative_dist must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"cleaned_trace_cumulative_dist": "-1"}, error_msg)


def test_cleaned_trace_cumulative_time_of_invalid_dtype():
    """Test output payload with an invalid cleaned_trace_cumulative_time."""
    
    error_msg = re.escape('("cleaned_trace_cumulative_time must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"cleaned_trace_cumulative_time": "-1"}, error_msg)    


def test_metadata_of_invalid_dtype():
    """Test output payload with an invalid metadata."""
    
    error_msg = re.escape('("metadata must be of type Dict but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"metadata": "invalid_metadata"}, error_msg)    


# Test function using _test_invalid_column_value_in_cleaning_summary
def test_total_pings_in_input_of_invalid_dtype():
    """Test output payload with an invalid total_pings_in_input."""
    
    error_msg = re.escape('("total_pings_in_input must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"total_pings_in_input": "0"}, error_msg)


def test_total_non_null_pings_in_output_of_invalid_dtype():
    """Test output payload with an invalid total_non_null_pings_in_output."""
    
    error_msg = re.escape('("total_non_null_pings_in_output must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"total_non_null_pings_in_output": "0"}, error_msg)


def test_total_non_null_pings_in_input_of_invalid_dtype():
    """Test output payload with an invalid total_non_null_pings_in_input."""
    
    error_msg = re.escape('("total_non_null_pings_in_input must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"total_non_null_pings_in_input": "0"}, error_msg)


def test_total_trace_time_of_invalid_dtype():
    """Test output payload with an invalid total_trace_time."""
    
    error_msg = re.escape('("total_trace_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"total_trace_time": 0}, error_msg)


def test_unchanged_percentage_of_invalid_dtype():
    """Test output payload with an invalid unchanged_percentage."""
    
    error_msg = re.escape('("unchanged_percentage must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"unchanged_percentage": "0"}, error_msg)


def test_drop_percentage_of_invalid_dtype():
    """Test output payload with an invalid drop_percentage."""
    
    error_msg = re.escape('("drop_percentage must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"drop_percentage": "0"}, error_msg)


def test_updation_percentage_of_invalid_dtype():
    """Test output payload with an invalid updation_percentage."""
    
    error_msg = re.escape('("updation_percentage must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"updation_percentage": "0"}, error_msg)


def test_interpolation_percentage_of_invalid_dtype():
    """Test output payload with an invalid interpolation_percentage."""
    
    error_msg = re.escape('("interpolation_percentage must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"interpolation_percentage": "0"}, error_msg)


def test_total_execution_time_of_invalid_dtype():
    """Test output payload with an invalid total_execution_time."""
    
    error_msg = re.escape('("total_execution_time must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaning_summary({"total_execution_time": "0"}, error_msg)


# Test function using _test_invalid_column_value_in_distance_summary
def test_cumulative_distance_of_raw_trace_of_invalid_dtype():
    """Test output payload with an invalid cumulative_distance_of_raw_trace."""
    
    error_msg = re.escape('("cumulative_distance_of_raw_trace must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_distance_summary({"cumulative_distance_of_raw_trace": "0"}, error_msg)


def test_cumulative_distance_of_clean_trace_of_invalid_dtype():
    """Test output payload with an invalid cumulative_distance_of_clean_trace."""
    
    error_msg = re.escape('("cumulative_distance_of_clean_trace must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_distance_summary({"cumulative_distance_of_clean_trace": "0"}, error_msg)


def test_percent_reduction_in_dist_of_invalid_dtype():
    """Test output payload with an invalid percent_reduction_in_dist."""
    
    error_msg = re.escape('("percent_reduction_in_dist must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_distance_summary({"percent_reduction_in_dist": "0"}, error_msg)


# Test function using _test_invalid_column_value_in_stop_summary
def test_stop_events_info_of_invalid_dtype():
    """Test output payload with an invalid stop_events_info."""
    
    error_msg = re.escape('("stop_events_info must be of type List but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_stop_summary({"stop_events_info": "0"}, error_msg)


def test_global_stop_events_info_of_invalid_dtype():
    """Test output payload with an invalid global_stop_events_info."""
    
    error_msg = re.escape('("global_stop_events_info must be of type Dict but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_stop_summary({"global_stop_events_info": "0"}, error_msg)


# Test function using _test_invalid_column_value_in_stop_events_info
def test_stop_event_sequence_number_of_invalid_dtype():
    """Test output payload with an invalid stop_event_sequence_number."""
    
    error_msg = re.escape('("stop_event_sequence_number must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"stop_event_sequence_number": "0"}, error_msg)


def test_start_time_of_invalid_dtype_in_stop_events_info():
    """Test output payload with an invalid start_time."""
    
    error_msg = re.escape('("start_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"start_time": 0}, error_msg)


def test_end_time_of_invalid_dtype_in_stop_events_info():
    """Test output payload with an invalid end_time."""
    
    error_msg = re.escape('("end_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"end_time": 0}, error_msg)


def test_total_stop_event_time_of_invalid_dtype():
    """Test output payload with an invalid total_stop_event_time."""
    
    error_msg = re.escape('("total_stop_event_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"total_stop_event_time": 0}, error_msg)


def test_number_of_pings_of_invalid_dtype_in_stop_events_info():
    """Test output payload with an invalid number_of_pings."""
    
    error_msg = re.escape('("number_of_pings must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"number_of_pings": "0"}, error_msg)


def test_representative_latitude_of_invalid_dtype_in_stop_events_info():
    """Test output payload with an invalid representative_latitude."""
    
    error_msg = re.escape('("representative_latitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"representative_latitude": "0"}, error_msg)


def test_representative_longitude_of_invalid_dtype_in_stop_events_info():
    """Test output payload with an invalid representative_longitude."""
    
    error_msg = re.escape('("representative_longitude must be of type Int, Float or None but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_stop_events_info({"representative_longitude": "0"}, error_msg)


# Test function using _test_invalid_column_value_in_global_stop_events_info
def test_total_trace_time_of_invalid_dtype_in_global_stop_events_info():
    """Test output payload with an invalid total_trace_time."""
    
    error_msg = re.escape('("total_trace_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_global_stop_events_info({"total_trace_time": 0}, error_msg)


def test_total_stop_events_time_of_invalid_dtype_in_global_stop_events_info():
    """Test output payload with an invalid total_stop_events_time."""
    
    error_msg = re.escape('("total_stop_events_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_global_stop_events_info({"total_stop_events_time": 0}, error_msg)


def test_stop_event_percentage_of_invalid_dtype_in_global_stop_events_info():
    """Test output payload with an invalid stop_event_percentage."""
    
    error_msg = re.escape('("stop_event_percentage must be of type Int or Float but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_global_stop_events_info({"stop_event_percentage": "0"}, error_msg)


#################################################
# Check value of keys
#################################################

# Test function using _test_invalid_column_value_in_cleaned_trace
def test_invalid_input_latitude_value():
    """Test output payload with an invalid input_latitude."""
    
    error_msg = re.escape("('input_latitude must be within range [-90 to 90] but found latitude = 999', 4005)")
    _test_invalid_column_value_in_cleaned_trace({"input_latitude": 999}, error_msg)


def test_invalid_input_longitude_value():
    """Test output payload with an invalid input_longitude."""
    
    error_msg = re.escape("('input_longitude must be within range [-180 to 180] but found longitude = 999', 4005)")
    _test_invalid_column_value_in_cleaned_trace({"input_longitude": 999}, error_msg)


def test_invalid_timestamp_value():
    """Test output payload with an invalid timestamp."""
    
    error_msg = re.escape("('timestamp must be in milliseconds and unix epoch format within range [0, 2145916800000] but found timestamp = -1', 4004)")
    _test_invalid_column_value_in_cleaned_trace({"timestamp": -1}, error_msg)


def test_invalid_error_radius_value():
    """Test output payload with an invalid error_radius."""
    
    error_msg = re.escape("('error_radius cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaned_trace({"error_radius": -1}, error_msg)


def test_invalid_cleaned_latitude_value():
    """Test output payload with an invalid cleaned_latitude."""
    
    error_msg = re.escape("('cleaned_latitude must be within range [-90 to 90] but found latitude = -200', 4005)")
    _test_invalid_column_value_in_cleaned_trace({"cleaned_latitude": -200}, error_msg)  


def test_invalid_cleaned_longitude_value():
    """Test output payload with an invalid cleaned_longitude."""
    
    error_msg = re.escape("('cleaned_longitude must be within range [-180 to 180] but found longitude = -200', 4005)")
    _test_invalid_column_value_in_cleaned_trace({"cleaned_longitude": -200}, error_msg)   


def test_invalid_cumulative_stop_event_time_value():
    """Test output payload with an invalid cumulative_stop_event_time."""
    
    error_msg = re.escape('("cumulative_stop_event_time must be of type String but found <class \'int\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"cumulative_stop_event_time": -1}, error_msg)   


def test_invalid_representative_stop_event_latitude_value():
    """Test output payload with an invalid representative_stop_event_latitude."""
    
    error_msg = re.escape("('representative_stop_event_latitude must be within range [-90 to 90] but found latitude = 1000', 4005)")
    _test_invalid_column_value_in_cleaned_trace({"representative_stop_event_latitude": 1000}, error_msg)  


def test_invalid_representative_stop_event_longitude_value():
    """Test output payload with an invalid representative_stop_event_longitude."""
    
    error_msg = re.escape("('representative_stop_event_longitude must be within range [-180 to 180] but found longitude = 1000', 4005)")
    _test_invalid_column_value_in_cleaned_trace({"representative_stop_event_longitude": 1000}, error_msg)  


def test_invalid_stop_event_sequence_number_value():
    """Test output payload with an invalid stop_event_sequence_number."""
    
    error_msg = re.escape('("stop_event_sequence_number must be of type Int but found <class \'str\'>", 4002)')
    _test_invalid_column_value_in_cleaned_trace({"stop_event_sequence_number": "1"}, error_msg)  


def test_invalid_time_since_prev_ping_value():
    """Test output payload with an invalid time_since_prev_ping."""
    
    error_msg = re.escape("('time_since_prev_ping cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaned_trace({"time_since_prev_ping": -1}, error_msg)  


def test_invalid_dist_from_prev_ping_value():
    """Test output payload with an invalid dist_from_prev_ping."""
    
    error_msg = re.escape("('dist_from_prev_ping cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaned_trace({"dist_from_prev_ping": -1}, error_msg)  


def test_invalid_cleaned_trace_cumulative_dist_value():
    """Test output payload with an invalid cleaned_trace_cumulative_dist."""
    
    error_msg = re.escape("('cleaned_trace_cumulative_dist cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaned_trace({"cleaned_trace_cumulative_dist": -1}, error_msg)  


def test_invalid_cleaned_trace_cumulative_time_value():
    """Test output payload with an invalid cleaned_trace_cumulative_time."""
    
    error_msg = re.escape("('cleaned_trace_cumulative_time cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaned_trace({"cleaned_trace_cumulative_time": -1}, error_msg)  


def test_invalid_metadata_value():
    """Test output payload with an invalid metadata."""
    
    error_msg = re.escape("('Unexpected key provided in ping dictionary', 4001)")
    _test_invalid_column_value_in_cleaned_trace({1: "invalid_metadata"}, error_msg)  


# Test function using _test_invalid_column_value_in_cleaning_summary
def test_total_pings_in_input_of_invalid_value():
    """Test output payload with an invalid total_pings_in_input."""
    
    error_msg = re.escape("('total_pings_in_input cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"total_pings_in_input": -1}, error_msg)


def test_total_non_null_pings_in_output_of_invalid_value():
    """Test output payload with an invalid total_non_null_pings_in_output."""
    
    error_msg = re.escape("('total_non_null_pings_in_output cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"total_non_null_pings_in_output": -1}, error_msg)


def test_total_non_null_pings_in_input_of_invalid_value():
    """Test output payload with an invalid total_non_null_pings_in_input."""
    
    error_msg = re.escape("('total_non_null_pings_in_input cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"total_non_null_pings_in_input": -1}, error_msg)


def test_unchanged_percentage_of_invalid_value():
    """Test output payload with an invalid unchanged_percentage."""
    
    error_msg = re.escape("('unchanged_percentage cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"unchanged_percentage": -1}, error_msg)


def test_drop_percentage_of_invalid_value():
    """Test output payload with an invalid drop_percentage."""
    
    error_msg = re.escape("('drop_percentage cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"drop_percentage": -1}, error_msg)


def test_updation_percentage_of_invalid_value():
    """Test output payload with an invalid updation_percentage."""
    
    error_msg = re.escape("('updation_percentage cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"updation_percentage": -1}, error_msg)


def test_interpolation_percentage_of_invalid_value():
    """Test output payload with an invalid interpolation_percentage."""
    
    error_msg = re.escape("('interpolation_percentage cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"interpolation_percentage": -1}, error_msg)


def test_total_execution_time_of_invalid_value():
    """Test output payload with an invalid total_execution_time."""
    
    error_msg = re.escape("('total_execution_time cannot be negative', 4003)")
    _test_invalid_column_value_in_cleaning_summary({"total_execution_time": -1}, error_msg)


# Test function using _test_invalid_column_value_in_distance_summary
def test_cumulative_distance_of_raw_trace_of_invalid_value():
    """Test output payload with an invalid cumulative_distance_of_raw_trace."""
    
    error_msg = re.escape("('cumulative_distance_of_raw_trace cannot be negative', 4003)")
    _test_invalid_column_value_in_distance_summary({"cumulative_distance_of_raw_trace": -1}, error_msg)


def test_cumulative_distance_of_clean_trace_of_invalid_value():
    """Test output payload with an invalid cumulative_distance_of_clean_trace."""
    
    error_msg = re.escape("('cumulative_distance_of_clean_trace cannot be negative', 4003)")
    _test_invalid_column_value_in_distance_summary({"cumulative_distance_of_clean_trace": -1}, error_msg)


def test_percent_reduction_in_dist_of_invalid_value():
    """Test output payload with an invalid percent_reduction_in_dist."""
    
    error_msg = re.escape("('percent_reduction_in_dist cannot be negative', 4003)")
    _test_invalid_column_value_in_distance_summary({"percent_reduction_in_dist": -1}, error_msg)


# Test function using _test_invalid_column_value_in_stop_events_info
def test_number_of_pings_of_invalid_value_in_stop_events_info():
    """Test output payload with an invalid number_of_pings."""
    
    error_msg = re.escape("('number_of_pings cannot be negative', 4003)")
    _test_invalid_column_value_in_stop_events_info({"number_of_pings": -1}, error_msg)


def test_representative_latitude_of_invalid_value_in_stop_events_info():
    """Test output payload with an invalid representative_latitude."""
    
    error_msg = re.escape("('representative_latitude must be within range [-90 to 90] but found latitude = -1000', 4005)")
    _test_invalid_column_value_in_stop_events_info({"representative_latitude": -1000}, error_msg)


def test_representative_longitude_of_invalid_value_in_stop_events_info():
    """Test output payload with an invalid representative_longitude."""
    
    error_msg = re.escape("('representative_longitude must be within range [-180 to 180] but found longitude = 1000', 4005)")
    _test_invalid_column_value_in_stop_events_info({"representative_longitude": 1000}, error_msg)


# Test function using _test_invalid_column_value_in_global_stop_events_info
def test_stop_event_percentage_of_invalid_value_in_global_stop_events_info():
    """Test output payload with an invalid stop_event_percentage."""
    
    error_msg = re.escape("('stop_event_percentage cannot be negative', 4003)")
    _test_invalid_column_value_in_global_stop_events_info({"stop_event_percentage": -1}, error_msg)


def test_sorted_timestamp_in_output():
    """Test a simple valid output payload for timestamp in sorted order."""

    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)
    trace_output = trace_data_obj.get_trace_cleaning_output()["cleaned_trace"]
    
    for i in range(1, len(trace_output)-1):
        assert trace_output[i]["timestamp"] >= trace_output[i-1]["timestamp"]

################################
# Test unexpected keys in output
################################

def test_unexpected_key_in_cleaned_trace():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaned_trace to manipulate
    cleaned_trace_copy = copy.deepcopy(valid_output_trace["cleaned_trace"])

    cleaned_trace_copy[0]["unexpected_key"] = "unexpected_data"
    expected_error_msg = re.escape("('Unexpected key provided in ping dictionary', 4001)")
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_trace", match=expected_error_msg, return_value=cleaned_trace_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_unexpected_key_in_cleaning_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaning_summary to manipulate
    cleaning_summary_copy = copy.deepcopy(valid_output_trace["cleaning_summary"])

    cleaning_summary_copy["unexpected_key"] = "unexpected_data"
    expected_error_msg = re.escape("('Unexpected key provided in cleaning summary dictionary', 4001)")
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_cleaning_summary", match=expected_error_msg, return_value=cleaning_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_unexpected_key_in_distance_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the distance_summary to manipulate
    distance_summary_copy = copy.deepcopy(valid_output_trace["distance_summary"])

    distance_summary_copy["unexpected_key"] = "unexpected_data"
    expected_error_msg = re.escape("('Unexpected key provided in distance_summary dictionary', 4001)")
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_distance_summary", match=expected_error_msg, return_value=distance_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_unexpected_key_in_stop_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])

    stop_summary_copy["unexpected_key"] = "unexpected_data"
    expected_error_msg = re.escape("('Unexpected key provided in stop_summary dictionary', 4001)")
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", match=expected_error_msg, return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_unexpected_key_in_individual_stop_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])

    stop_summary_copy["stop_events_info"][0]["unexpected_key"] = "unexpected_data"
    expected_error_msg = re.escape("('Unexpected key provided in stop event info dictionary', 4001)")
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", match=expected_error_msg, return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_unexpected_key_in_global_stop_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the stop_summary to manipulate
    stop_summary_copy = copy.deepcopy(valid_output_trace["stop_summary"])

    stop_summary_copy["global_stop_events_info"]["unexpected_key"] = "unexpected_data"
    expected_error_msg = re.escape("('Unexpected key provided in global stop event info dictionary', 4001)")
    
    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_stop_summary", match=expected_error_msg, return_value=stop_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


#####################
# Logical constraints
#####################
def test_invalid_sum_of_percentages_in_cleaning_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaned_trace to manipulate
    cleaning_summary_copy = copy.deepcopy(valid_output_trace["cleaning_summary"])

    # Remove the specified key
    cleaning_summary_copy["unchanged_percentage"] = 5.0
    cleaning_summary_copy["drop_percentage"] = 5.0
    cleaning_summary_copy["updation_percentage"] = 5.0
    cleaning_summary_copy["interpolation_percentage"] = 5.0
    
    expected_error_msg = re.escape("('Sum of percentages of various update statuses should be at least 99.9', 4003)")

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_cleaning_summary", return_value=cleaning_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()


def test_incorrect_count_of_pings_in_cleaning_summary():
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Create a copy of the cleaned_trace to manipulate
    cleaning_summary_copy = copy.deepcopy(valid_output_trace["cleaning_summary"])

    # Remove the specified key
    cleaning_summary_copy["total_pings_in_input"] = 3100
    
    expected_error_msg = re.escape("('total_pings_in_input in cleaning_summary must be equal to number of pings in input payload', 4003)")

    # Patch the method before calling get_trace_cleaning_output
    with patch.object(CleanTrace, "_create_output_cleaning_summary", return_value=cleaning_summary_copy):
        with pytest.raises(ValidationException, match=expected_error_msg):
            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()
