import re
import os
import pytest
import folium
import tempfile
import subprocess
import pandas as pd
from unittest.mock import patch

from src.tracely.clean_trace import CleanTrace
from src.tracely.exceptions.custom_exceptions import ValidationException, InputOutputException, OSRMException
from src.tracely.utils.utils import get_haversine_distance, \
                                    calculate_trace_distance, \
                                    calculate_initial_compass_bearing, \
                                    calculate_change_in_direction, \
                                    convert_unix_timestamp_to_human_readable, \
                                    convert_time_interval_to_human_readable, \
                                    is_filename, \
                                    create_path

from src.tracely.utils.osrm_utils import get_osrm_route, \
                                         get_osrm_match
                                    
from tests.testing_utils import load_trace_payload
from src.tracely.utils.input_output_utils import convert_csv_to_trace_payload
from src.tracely import constants


#########################
# Check payload data type
#########################

def test_payload_datatype():
    """Test data type of the payload."""

    payload = []
    expected_error_msg = re.escape('("trace payload must be of type Dict but found <class \'list\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_empty_payload():
    """Test if input payload is empty."""

    payload = {}
    expected_error_msg = re.escape("('trace payload cannot be an empty dictionary', 4002)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


##############################
# Check missing mandatory keys
##############################

def test_trace_key_missing_in_payload():
    """Test if "trace" key is missing in the payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload.pop("trace")
    expected_error_msg = re.escape('("Expected key: \'trace\' missing from the dictionary", 4001)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


####################
# Check "trace" data
####################

def test_trace_key_data_type():
    """Test data type of the value of the key "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = {}
    expected_error_msg = re.escape('("trace must be of type List but found <class \'dict\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_trace_key_with_only_null_lat_lng():
    """Test "trace" with only null latitude and longitude values."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    # Set coordinates of all pings as None
    for ping in payload["trace"]:
        ping["latitude"], ping["latitude"] = None, None

    expected_error_msg = re.escape("('Trace should have at least one ping with non null latitude and longitude', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_payload_with_zero_pings():
    """Test a payload with zero pings."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = []
    expected_error_msg = re.escape("('trace cannot be an empty list', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_payload_with_empty_dict_ping():
    """Test a payload with at least one ping which is empty dictionary."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0] = {}
    expected_error_msg = re.escape("('ping cannot be an empty dictionary', 4002)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_timestamp_key_missing_in_trace():
    """Test if "timestamp" key is missing in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0].pop("timestamp")

    expected_error_msg = re.escape('("Expected key: \'timestamp\' missing from the dictionary", 4001)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_latitude_key_missing_in_trace():
    """Test if "latitude" key is missing in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0].pop("latitude")

    expected_error_msg = re.escape('("Expected key: \'latitude\' missing from the dictionary", 4001)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_longitude_key_missing_in_trace():
    """Test if "longitude" key is missing in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0].pop("longitude")

    expected_error_msg = re.escape('("Expected key: \'longitude\' missing from the dictionary", 4001)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_payload_with_invalid_ping():
    """Test a payload with at least one ping which is of invalid datatype."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0] = ()
    expected_error_msg = re.escape('("ping must be of type Dict but found <class \'tuple\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_lat_value_in_a_ping():
    """Test out of range value of "latitude" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["latitude"] = 255.2

    expected_error_msg = re.escape("('latitude must be within range [-90 to 90] but found latitude = 255.2', 4005)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_lat_type_in_a_ping():
    """Test data type of "latitude" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["latitude"] = "value"

    expected_error_msg = re.escape('("latitude must be of type Int, Float or None but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_lng_value_in_a_ping():
    """Test value of "longitude" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["longitude"] = 255.2

    expected_error_msg = re.escape("('longitude must be within range [-180 to 180] but found longitude = 255.2', 4005)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_lng_type_in_a_ping():
    """Test data type of "longitude" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["longitude"] = "invalid"
    expected_error_msg = re.escape('("longitude must be of type Int, Float or None but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_timestamp_value_in_a_ping():
    """Test value of "timestamp" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["timestamp"] = -1
    expected_error_msg = re.escape("('timestamp must be in milliseconds and unix epoch format within range [0, 2145916800000] but found timestamp = -1', 4004)")
    
    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_timestamp_type_in_a_ping():
    """Test data type of "timestamp" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["timestamp"] = "invalid"
    expected_error_msg = re.escape('("timestamp must be of type Int but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_error_radius_value_in_a_ping():
    """Test value of "error_radius" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["error_radius"] = -1
    expected_error_msg = re.escape("('error_radius cannot be negative', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_error_radius_type_in_a_ping():
    """Test data type of "error_radius" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["error_radius"] = "invalid"
    expected_error_msg = re.escape('("error_radius must be of type Int, Float or None but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_force_retain_type_in_a_ping():
    """Test data type of "force_retain" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["force_retain"] = "invalid"
    expected_error_msg = re.escape('("force_retain must be of type Bool but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_event_type_type_in_a_ping():
    """Test data type of "event_type" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["event_type"] = 1
    expected_error_msg = re.escape('("event_type must be of type String or None but found <class \'int\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_metadata_type_in_a_ping():
    """Test data type of "metadata" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["metadata"] = 1
    expected_error_msg = re.escape('("metadata must be of type Dict but found <class \'int\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_invalid_metadata_key_in_a_ping():
    """Test data type of "metadata" key in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["metadata"][1] = "1"
    expected_error_msg = re.escape("('Expected keys of only string type in metadata dictionary', 4001)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_missing_optional_keys_in_a_ping():
    """Test handling of a missing optional key in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")

    ping_id = payload["trace"][0]["ping_id"]
    payload["trace"][0].pop("error_radius")
    payload["trace"][0].pop("force_retain")
    payload["trace"][0].pop("metadata")
    
    trace_data_obj = CleanTrace(payload)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged")
        assert (ping["last_updated_by"] == "never_updated")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(list(ping.keys())) == constants.CLEAN_PING_KEYS_COUNT

        if ping["ping_id"] == ping_id:
            assert ping["error_radius"] == None
            assert ping["event_type"] == None
            assert ping["force_retain"] == False
            assert ping["metadata"] == {}


def test_unexpected_key_in_a_ping():
    """Test for an unexpected key in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["unexpected_key"] = None

    expected_error_msg = re.escape("('Unexpected key provided in ping dictionary', 4001)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_ping_ids_incomplete_presence():
    """Test for case where ping_id key in some pings but not all."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    # Remove all ping_ids
    for ping in payload["trace"]:
        ping.pop("ping_id")
    
    # Add ping_id for one ping
    payload["trace"][0]["ping_id"] = "0"

    expected_error_msg = re.escape("ping_id must be present in either all of the pings or in none of the pings")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_ping_ids_for_duplicate_presence():
    """Test for case where ping_id are duplicate."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    for ping in payload["trace"]:
        ping["ping_id"] = "0"

    expected_error_msg = re.escape("Expected values for 'ping_id' to be unique, but found duplicate values")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_ping_ids_for_invalid_datatypes():
    """Test for case where ping_id has invalid datatype."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"][0]["ping_id"] = None
    expected_error_msg = re.escape('("ping_id must be of type String but found <class \'NoneType\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)

    payload["trace"][0]["ping_id"] = 1
    expected_error_msg = re.escape('("ping_id must be of type String but found <class \'int\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_ping_ids_for_invalid_length():
    """Test for case where ping_id has zero length."""

    payload = load_trace_payload("dummy_trace_input_payload")
    counter = 0
    for ping in payload["trace"]:
        ping["ping_id"] = str(counter)
        counter += 1

    payload["trace"][0]["ping_id"] = ""

    expected_error_msg = re.escape("ping_id can not be an empty string")
    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_payload_with_no_ping_id():
    """Test for case where ping_id has zero length."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    for ping in payload["trace"]:
        ping.pop("ping_id")

    # Create trace data without any issue
    trace_data_obj = CleanTrace(payload)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert "ping_id" in ping.keys()
        assert isinstance(ping["ping_id"], str)


############################
# Check "vehicle_type" data
############################

def test_vehicle_type_key_data_type():
    """Test data type of the value of the key "vehicle_type"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    payload["vehicle_type"] = {}
    expected_error_msg = re.escape('("vehicle_type must be of type String but found <class \'dict\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


############################
# Check "vehicle_speed" data
############################

def test_vehicle_speed_key_data_type():
    """Test data type of the value of the key "vehicle_speed"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    payload["vehicle_speed"] = {}
    expected_error_msg = re.escape('("vehicle_speed must be of type Int or Float but found <class \'dict\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_vehicle_speed_key_negative_value():
    """Test negative value of the value of the key "vehicle_speed"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    payload["vehicle_speed"] = -1
    expected_error_msg = re.escape("('vehicle_speed cannot be less than or equal to zero', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


def test_vehicle_speed_key_zero_value():
    """Test zero value of the value of the key "vehicle_speed"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    payload["vehicle_speed"] = 0
    expected_error_msg = re.escape("('vehicle_speed cannot be less than or equal to zero', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


#############################
# Check for an unexpected key
#############################

def test_unexpected_key_in_a_trace_payload():
    """Test for an unexpected key in trace payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    payload["unexpected_key"] = None
    expected_error_msg = re.escape("('Unexpected key provided in trace payload dictionary', 4001)")

    with pytest.raises(ValidationException, match=expected_error_msg) as e:
        trace_data_obj = CleanTrace(payload)


#####################
# Test valid payloads
#####################

def test_valid_payload():
    """Test a simple valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])
    trace_data_obj = CleanTrace(payload)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    assert len(clean_output["cleaned_trace"]) == count_pings

    for ping in clean_output["cleaned_trace"]:
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT


def test_zero_error_radius_value_in_a_ping():
    """Test value of "error_radius" in a ping in "trace"."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    count_pings = len(payload["trace"])

    ping_id = payload["trace"][0]["ping_id"]
    payload["trace"][0]["error_radius"] = 0

    trace_data_obj = CleanTrace(payload)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    assert len(clean_output["cleaned_trace"]) == count_pings

    for ping in clean_output["cleaned_trace"]:
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

        if ping["ping_id"] == ping_id:
            assert ping["error_radius"] == 0


def test_remove_nearby_pings_with_first_ping_null():
    """Test remove nearby pings on valid payload, but first ping is null."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])

    # With null location of a point
    payload["trace"][0]["latitude"] = None
    payload["trace"][0]["longitude"] = None
    ping_id = payload["trace"][0]["ping_id"]

    trace_data_obj = CleanTrace(payload)
    trace_data_obj.remove_nearby_pings()
    clean_output = trace_data_obj.get_trace_cleaning_output()

    assert len(clean_output["cleaned_trace"]) == count_pings

    for ping in clean_output["cleaned_trace"]:
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

        if ping["ping_id"] == ping_id:
            # Since point's lat/lng are None, it should remain as it it
            assert ping["update_status"] == "unchanged"


def test_remove_nearby_pings():
    """Test remove nearby pings on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])

    trace_data_obj = CleanTrace(payload)
    trace_data_obj.remove_nearby_pings()
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "dropped")
        assert (ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "remove_nearby_pings")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

    assert len(clean_output["cleaned_trace"]) == count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] >= 0
    assert clean_output["cleaning_summary"]["drop_percentage"] > 0
    assert clean_output["cleaning_summary"]["updation_percentage"] == 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] == 0

    assert clean_output["distance_summary"]["percent_reduction_in_dist"] > 0
    assert clean_output["distance_summary"]["cumulative_distance_of_raw_trace"] > clean_output["distance_summary"]["cumulative_distance_of_clean_trace"]


def test_remove_nearby_pings_for_interpolated_pings():
    """Test remove nearby pings for pings that are interpolated. These pings should not be changed."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    # Check that point is not updated if its update_status is "interpolated"
    trace_data_obj.trace_df["update_status"] = "interpolated"
    trace_data_obj.remove_nearby_pings()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    # Check that update status is same for each ping
    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "interpolated")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"


def test_impute_distorted_pings_with_distance():
    """Test test_impute_distorted_pings_with_distance on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])
    trace_data_obj = CleanTrace(payload)
    trace_data_obj.impute_distorted_pings_with_distance()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "updated")
        assert (ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "impute_distorted_pings_with_distance")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

    assert len(clean_output["cleaned_trace"]) == count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] >= 0
    assert clean_output["cleaning_summary"]["drop_percentage"] == 0
    assert clean_output["cleaning_summary"]["updation_percentage"] > 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] == 0
    
    assert clean_output["distance_summary"]["percent_reduction_in_dist"] >= 0


def test_impute_distorted_pings_with_distance_for_interpolated_pings():
    """Test test_impute_distorted_pings_with_distance for pings that are interpolated. These pings should not be changed."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    # Check that point is not updated if its update_status is "interpolated"
    trace_data_obj.trace_df["update_status"] = "interpolated"
    trace_data_obj.impute_distorted_pings_with_distance()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    # Check that update status is same for each ping
    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "interpolated")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"


def test_impute_distorted_pings_with_angle():
    """Test test_impute_distorted_pings_with_angle on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])

    trace_data_obj = CleanTrace(payload)
    trace_data_obj.impute_distorted_pings_with_angle()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "updated")
        assert (ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "impute_distorted_pings_with_angle")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

    assert len(clean_output["cleaned_trace"]) == count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] >= 0
    assert clean_output["cleaning_summary"]["drop_percentage"] == 0
    assert clean_output["cleaning_summary"]["updation_percentage"] > 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] == 0
    
    assert clean_output["distance_summary"]["percent_reduction_in_dist"] >= 0


def test_impute_distorted_pings_with_angle_for_interpolated_pings():
    """Test impute_distorted_pings_with_angle for pings that are interpolated. These pings should not be changed."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    trace_data_obj.trace_df["update_status"] = "interpolated"
    trace_data_obj.impute_distorted_pings_with_angle()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    # Check that update status is same for each ping
    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "interpolated")
        assert (ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "interpolate_trace")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"


def test_map_match_trace():
    """Test map_match_trace on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])

    trace_data_obj = CleanTrace(payload)
    trace_data_obj.map_match_trace()
    
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "updated")
        assert (ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "map_match_trace")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

    assert len(clean_output["cleaned_trace"]) == count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] >= 0
    assert clean_output["cleaning_summary"]["drop_percentage"] == 0
    assert clean_output["cleaning_summary"]["updation_percentage"] > 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] == 0
    
    assert clean_output["distance_summary"]["percent_reduction_in_dist"] >= 0


def test_map_match_trace_for_interpolated_pings():
    """Test map_match_trace for pings that are interpolated. These pings should not be changed."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    # Check that point is not updated if its update_status is "interpolated"
    trace_data_obj.trace_df["last_updated_by"] = "interpolate_trace"
    trace_data_obj.map_match_trace()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    # Check that update status is same for each ping
    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged") or (ping["update_status"] == "interpolated")
        assert (ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "interpolate_trace")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"


def test_interpolate_trace():
    """Test interpolate_trace on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])

    trace_data_obj = CleanTrace(payload)

    # Apply map matching before interpolation of trace
    trace_data_obj.map_match_trace()
    trace_data_obj.interpolate_trace()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert ((ping["update_status"] == "unchanged") or (ping["update_status"] == "updated") or (ping["update_status"] == "interpolated"))
        assert ((ping["last_updated_by"] == "never_updated") or (ping["last_updated_by"] == "map_match_trace") or (ping["last_updated_by"] == "interpolate_trace"))
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

        if ping["update_status"] == "interpolated":
            assert pd.isna(ping["input_latitude"])
            assert pd.isna(ping["input_longitude"])
            assert pd.notna(ping["cleaned_latitude"])
            assert pd.notna(ping["cleaned_longitude"])

    assert len(clean_output["cleaned_trace"]) >= count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] >= 0
    assert clean_output["cleaning_summary"]["drop_percentage"] == 0
    assert clean_output["cleaning_summary"]["updation_percentage"] > 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] > 0
    assert clean_output["distance_summary"]["percent_reduction_in_dist"] >= 0


def test_interpolate_trace_flag_consistency():
    """Test that interpolated points are not updated by map matching function."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    # First apply map matching as interpolation works after map matching
    trace_data_obj.map_match_trace()
    trace_data_obj.interpolate_trace()

    interpolated_ping_ids = trace_data_obj.trace_df.loc[trace_data_obj.trace_df["last_updated_by"] == "interpolate_trace", "ping_id"]

    trace_data_obj.map_match_trace()
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        if ping["ping_id"] in interpolated_ping_ids:
            assert ping["last_updated_by"] == "interpolate_trace"


def test_add_stop_events_info():
    """Test add_stop_events_info on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])

    trace_data_obj = CleanTrace(payload)
    trace_data_obj.add_stop_events_info()

    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged")
        assert (ping["last_updated_by"] == "never_updated")
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

    assert len(clean_output["cleaned_trace"]) == count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] == 100
    assert clean_output["cleaning_summary"]["drop_percentage"] == 0
    assert clean_output["cleaning_summary"]["updation_percentage"] == 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] == 0
    
    assert clean_output["distance_summary"]["percent_reduction_in_dist"] == 0


def test_get_trace_cleaning_output():
    """Test get_trace_cleaning_output on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    count_pings = len(payload["trace"])
    trace_data_obj = CleanTrace(payload)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert (ping["update_status"] == "unchanged")
        assert (ping["last_updated_by"] == "never_updated")
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
        assert ping["representative_stop_event_latitude"] == None
        assert ping["representative_stop_event_longitude"] == None
        assert ping["cumulative_stop_event_time"] == "0 minutes and 0 seconds"
        assert len(ping.keys()) == constants.CLEAN_PING_KEYS_COUNT

    assert len(clean_output["cleaned_trace"]) == count_pings
    assert clean_output["cleaning_summary"]["unchanged_percentage"] == 100
    assert clean_output["cleaning_summary"]["drop_percentage"] == 0
    assert clean_output["cleaning_summary"]["updation_percentage"] == 0
    assert clean_output["cleaning_summary"]["interpolation_percentage"] == 0
    
    assert clean_output["distance_summary"]["percent_reduction_in_dist"] == 0


def test_get_trace_cleaning_output_for_trace_without_metadata():
    """Test get_trace_cleaning_output on valid payload, but metadata is absent in some pings."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    ping_id = payload["trace"][0]["ping_id"]
    payload["trace"][0].pop("metadata")

    trace_data_obj = CleanTrace(payload)
    trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()
    
    for ping in trace_cleaning_output["cleaned_trace"]:
        if ping["ping_id"] == ping_id:
            assert ping["metadata"] == {}


def test_plot_raw_trace():
    """Test plot_raw_trace on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)
    trace_map = trace_data_obj.plot_raw_trace()

    assert isinstance(trace_map, folium.Map)


def test_plot_clean_trace():
    """Test plot_clean_trace on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)
    trace_map = trace_data_obj.plot_clean_trace()

    assert isinstance(trace_map, folium.Map)


def test_plot_raw_vs_stop_comparison_map():
    """Test plot_raw_vs_stop_comparison_map on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)
    trace_data_obj.add_stop_events_info()
    trace_map = trace_data_obj.plot_raw_vs_stop_comparison_map()

    assert isinstance(trace_map, folium.plugins.DualMap)


def test_plot_cleaning_comparison_map():
    """Test plot_cleaning_comparison_map on valid payload."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]

    trace_data_obj = CleanTrace(payload)

    raw_output = trace_data_obj.get_trace_cleaning_output()
    raw_trace = raw_output['cleaned_trace']

    trace_data_obj.map_match_trace()
    trace_data_obj.interpolate_trace()

    clean_output = trace_data_obj.get_trace_cleaning_output()
    clean_trace = clean_output['cleaned_trace']

    # Vary position of trace with larger length
    trace_map_1 = trace_data_obj.plot_cleaning_comparison_map(raw_trace, raw_trace)
    trace_map_2 = trace_data_obj.plot_cleaning_comparison_map(clean_trace, raw_trace)

    assert isinstance(trace_map_1, folium.plugins.DualMap)
    assert isinstance(trace_map_2, folium.plugins.DualMap)


########################
# Test utility functions
########################

def test_get_haversine_distance():
    """Test correct distance calculation"""
    assert get_haversine_distance(52.2296756, 21.0122287,
                                  41.8919300, 12.5113300) == pytest.approx(1318276.83, 0.01)


def test_get_haversine_distance_for_same_point():
    """Test correct distance calculation when distance is calculated between same point"""

    assert get_haversine_distance(0, 0,
                                  0, 0) == 0.0


def test_get_haversine_distance_for_invalid_point():
    """Test get_haversine_distance for invalid point"""

    assert get_haversine_distance("invalid", 21, 41, 12) is None


def test_calculate_trace_distance():
    """Test calculate_trace_distance function for correct distance"""

    trace = [[52.2296756, 21.0122287], [41.8919300, 12.5113300]]
    assert calculate_trace_distance(trace) == pytest.approx(1315511.97, 0.01)


def test_calculate_trace_distance_for_single_point():
    """Test calculate_trace_distance for a single point, should return 0."""

    trace_single = [[52.2296756, 21.0122287]]
    assert calculate_trace_distance(trace_single) == 0.0


def test_calculate_initial_compass_bearing():
    """Test calculate_initial_compass_bearing for correct bearing calculation"""

    assert calculate_initial_compass_bearing(52.2296756, 21.0122287,
                                             41.8919300, 12.5113300) == pytest.approx(212.46, 0.01)


def test_calculate_initial_compass_bearing_for_invalid_input():
    """Test calculate_initial_compass_bearing for invalid input, should return None"""

    assert calculate_initial_compass_bearing(
        "invalid", 21.0122287, 41.8919300, 12.5113300) is None


def test_calculate_change_in_direction():
    """Test calculate_change_in_direction for correct change in direction"""

    assert calculate_change_in_direction((52.2296756, 21.0122287), 
                                         (51.0, 19.0), (41.8919300, 12.5113300)) == pytest.approx(16.11, 0.01)


def test_calculate_change_in_direction_for_invalid_input():
    """Test calculate_change_in_direction for invalid input, should return None"""

    assert calculate_change_in_direction((52.2296756, "invalid"), (51.0, 19.0),
                                         (41.8919300, 12.5113300)) is None


def test_convert_unix_timestamp_to_human_readable():
    """Test convert_unix_timestamp_to_human_readable for correct conversion into human readable string"""

    timestamp = 1609459200  # 2021-01-01 00:00:00 UTC, 2021-01-01 05:30:00 IST
    assert convert_unix_timestamp_to_human_readable(timestamp) == "2021-01-01 05:30:00"


def test_convert_unix_timestamp_to_human_readable_for_invalid_input():
    """Test convert_unix_timestamp_to_human_readable for invalid input, should return None."""

    assert convert_unix_timestamp_to_human_readable("invalid") is None


def test_convert_time_interval_to_human_readable():
    """Test convert_time_interval_to_human_readable for correct output in various formats."""

    # Test "hms" format
    assert convert_time_interval_to_human_readable(3661, "hms") == "1 hours, 1 minutes and 1 seconds"

    # Test "ms" format
    assert convert_time_interval_to_human_readable(3661, "ms") == "61 minutes and 1 seconds"

    # Test "s" format
    assert convert_time_interval_to_human_readable(3661, "s") == "3661 seconds"


def test_convert_time_interval_to_human_readable_for_invalid_input():
    """Test convert_time_interval_to_human_readable for invalid input, should return None."""

    assert convert_time_interval_to_human_readable("10") is None


def test_is_filename():
    """Test is_filename for valid file path."""
    assert is_filename("not_a_file_path") == False
    

def test_create_path():
    """Test create_path for invalid file path."""

    expected_error_msg = re.escape("[Errno 2] No such file or directory: ''")
    with pytest.raises(FileNotFoundError, match=expected_error_msg) as e:
        create_path("not_a_file_path")


#############################
# Test OSRM utility functions
#############################

def test_get_osrm_route_invalid_url():
    """Test OSRM function to get route using an invalid URL"""

    expected_error_msg = re.escape("('Can not connect to OSRM server at URL invalid_url-1111,1111;0,1;0,0?overview=full&annotations=speed', 2001)")
    with pytest.raises(OSRMException, match=expected_error_msg):
        get_osrm_route(geo_coords=[(1111,-1111),("1","0"),(0,0)],
                       osrm_url="invalid_url")


def test_get_osrm_match_invalid_url():
    """Test OSRM function to get OSRM match using an invalid URL"""

    expected_error_msg = re.escape("('Can not connect to OSRM server at URL invalid_url-1111,1111;0,1;0,0?overview=full&radiuses=10;10;10&generate_hints=false&skip_waypoints=false&gaps=ignore&geometries=geojson&annotations=true', 2001)")
    with pytest.raises(OSRMException, match=expected_error_msg):
        get_osrm_match(geo_coords=[(1111,-1111),("1","0"),(0,0)],
                       osrm_url="invalid_url",
                       map_matching_radius=10)


#################################################################
# Test input output utils function (convert_csv_to_trace_payload)
#################################################################

def test_convert_csv_to_trace_payload_success():
    """Test successful trace payload creation from CSV file"""

    csv_file_path = constants.TEST_PAYLOADS_BASE_PATH + "dummy_csv_file.csv"
    payload = convert_csv_to_trace_payload(csv_file_path)

    assert isinstance(payload, dict)
    assert "trace" in payload
    assert "vehicle_type" in payload
    assert "vehicle_speed" in payload
    assert payload["vehicle_type"] == constants.DEFAULT_VEHICLE_TYPE
    assert payload["vehicle_speed"] == constants.DEFAULT_VEHICLE_SPEED

    # Check trace payload
    trace = payload["trace"]
    assert len(trace) == 200
    assert trace[0]["latitude"] == 19.048257
    assert trace[0]["error_radius"] == 6.2

    # Optional field with default value
    assert trace[1]["error_radius"] == 36.2
    assert "metadata" in trace[0]
    assert trace[0]["metadata"]["metadata"] == "{'meta_info': 'value_1'}"

    # Check if values of ping_id is same in CSV file and payload
    csv_df = pd.read_csv(csv_file_path)
    for i in range(len(csv_df)):
        assert csv_df.iloc[i]["ping_id"] == payload["trace"][i]["ping_id"]


def test_convert_csv_to_trace_payload_with_parameters():
    """Test successful trace payload creation from CSV file"""

    csv_file_path = constants.TEST_PAYLOADS_BASE_PATH + "dummy_csv_file.csv"
    payload = convert_csv_to_trace_payload(csv_file_path, force_retain_event_types=True)

    assert isinstance(payload, dict)
    assert "trace" in payload
    assert "vehicle_type" in payload
    assert payload["vehicle_type"] == constants.DEFAULT_VEHICLE_TYPE

    # Check trace payload
    trace = payload["trace"]
    assert len(trace) == 200
    assert trace[0]["latitude"] == 19.048257
    assert trace[0]["error_radius"] == 6.2

    # Optional field with default value
    assert trace[1]["error_radius"] == 36.2
    assert "metadata" in trace[0]
    assert trace[0]["metadata"]["metadata"] == "{'meta_info': 'value_1'}"

    # Check is values of ping_id is same in CSV file and payload
    csv_df = pd.read_csv(csv_file_path)
    for i in range(len(csv_df)):
        assert csv_df.iloc[i]["ping_id"] == payload["trace"][i]["ping_id"]


def test_convert_csv_to_trace_payload_missing_ping_id():
    """Test successful trace payload creation from CSV file when ping_id column is not present."""

    csv_file_path = constants.TEST_PAYLOADS_BASE_PATH + "dummy_csv_file.csv"
    trace = pd.read_csv(csv_file_path)
    trace_with_missing_ping_id = trace.drop(columns=["ping_id"])

    with patch("pandas.read_csv", return_value=trace_with_missing_ping_id):
        payload = convert_csv_to_trace_payload(csv_file_path)

    assert isinstance(payload, dict)
    assert "trace" in payload
    assert "vehicle_type" in payload
    assert payload["vehicle_type"] == constants.DEFAULT_VEHICLE_TYPE

    # Check trace payload
    trace = payload["trace"]
    assert len(trace) == 200
    assert trace[0]["latitude"] == 19.048257
    assert trace[0]["error_radius"] == 6.2

    # Optional field with default value
    assert trace[1]["error_radius"] == 36.2
    assert trace[0]["metadata"]["metadata"] == "{'meta_info': 'value_1'}"

    # Since ping_id is not present in the CSV file, it should also be absent in payload as well
    for ping in payload["trace"]:
        assert "metadata" in ping
        assert "ping_id" not in ping


def test_convert_csv_to_trace_payload_missing_mandatory_column():
    """Test exception raised when a mandatory column is missing."""

    incomplete_data = pd.DataFrame({
        "longitude": [21.0122287, 12.5113300],  # Missing "latitude" column
        "timestamp": [1609459200, 1609462800]})

    expected_error_msg = re.escape("(\"Expected column: 'latitude' missing from the input CSV file\", 1002)")
    
    with patch("pandas.read_csv", return_value=incomplete_data):
        with pytest.raises(InputOutputException, match=expected_error_msg) as e:
            convert_csv_to_trace_payload("dummy_path.csv")


def test_convert_csv_to_trace_payload_read_error():
    """Test exception raised when the CSV file cannot be read."""

    # Expected error message to match the actual exception message format
    expected_error_msg = re.escape("('Unable to read input CSV file', 1001)")

    with pytest.raises(InputOutputException, match=expected_error_msg) as e:
        convert_csv_to_trace_payload("invalid_path.csv")


def test_convert_csv_to_trace_payload_force_retain_event_types():
    """Test the `force_retain` flag is correctly set based on `event_type`."""

    csv_file_path = constants.TEST_PAYLOADS_BASE_PATH + "dummy_csv_file.csv"
    trace_df = pd.read_csv(csv_file_path).drop(columns=["force_retain"])

    # trace_df with missing force_retain
    with patch("pandas.read_csv", return_value=trace_df):
        result = convert_csv_to_trace_payload("dummy_path.csv")

    # Set force_retain based on event_type
    assert result["trace"][0]["force_retain"] is False
    assert result["trace"][5]["force_retain"] is True


def test_convert_csv_to_trace_payload_optional_columns_handling():
    """Test that optional columns are filled with default values."""

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='w') as temp_csv:
        temp_csv.write("latitude,longitude,timestamp,error_radius\n")
        temp_csv.write("1,2,3,4\n")
        temp_csv.write("4,5,6,\n")
        temp_csv.write("7,8,9,10\n")
        temp_csv.close()

    payload = convert_csv_to_trace_payload(temp_csv.name)
    trace = payload["trace"]
    assert trace[1]["error_radius"] == None

    # Clean up the temporary file
    os.remove(temp_csv.name)


def test_convert_csv_to_trace_payload_with_invalid_latitude():
    """Test that ValidationException is raised when CSV file has invalid latitude."""

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='w') as temp_csv:
        temp_csv.write("latitude,longitude,timestamp,error_radius\n")
        temp_csv.write("1000,2,3,4\n")
        temp_csv.write("4,5,6,\n")
        temp_csv.write("7,8,9,10\n")
        temp_csv.close()

    exp_error_msg = re.escape("('latitude must be within range [-90 to 90] but found latitude = 1000', 4005)")

    with pytest.raises(ValidationException, match=exp_error_msg):
        payload = convert_csv_to_trace_payload(temp_csv.name)

    # Clean up the temporary file
    os.remove(temp_csv.name)


def test_convert_csv_to_trace_payload_metadata_handling():
    """Test that metadata fields are properly handled."""

    # Create a temporary CSV file with dictionary
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='w') as temp_csv:
        temp_csv.write("latitude,longitude,timestamp,extra_column\n")
        temp_csv.write("1,2,3,4\n")
        temp_csv.write("4,5,6,{'a':1}\n")  # Storing dictionary
        temp_csv.write("7,8,9,10\n")
        temp_csv.close()

    result = convert_csv_to_trace_payload(temp_csv.name)
    trace = result["trace"]
    assert "metadata" in trace[0]
    assert trace[1]["metadata"]["extra_column"] == "{'a':1}" # Converted into a string

    # Clean up the temporary file
    os.remove(temp_csv.name)


def test_convert_csv_to_trace_payload_corrupt_metadata_handling():
    """Test that metadata fields with corrupt values are properly handled."""

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='w') as temp_csv:
        temp_csv.write("latitude,longitude,timestamp,extra_column\n")
        temp_csv.write("1,2,3,4\n")
        temp_csv.write("4,5,6,{'a':1\n")  # Storing values with invalid syntax
        temp_csv.write("7,8,9,10\n")
        temp_csv.close()

    result = convert_csv_to_trace_payload(temp_csv.name)
    trace = result["trace"]
    assert "metadata" in trace[0]
    assert trace[1]["metadata"]["extra_column"] == "{'a':1" # Converted into a string

    # Clean up the temporary file
    os.remove(temp_csv.name)


def test_convert_csv_to_trace_payload_with_column_named_metadata():
    """Test that metadata field with name metadata is properly handled."""

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='w') as temp_csv:
        temp_csv.write("latitude,longitude,timestamp,metadata\n")
        temp_csv.write("1,2,3,4\n")
        temp_csv.write("4,5,6,7\n")
        temp_csv.close()

    payload = convert_csv_to_trace_payload(temp_csv.name)
    trace = payload["trace"]

    for ping in trace:
        assert "metadata" in ping["metadata"]

    # Clean up the temporary file
    os.remove(temp_csv.name)


def test_remove_nearby_pings_invalid_parameter_type():
    """Test for invalid data type for min_dist_bw_consecutive_pings."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("min_dist_bw_consecutive_pings must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.remove_nearby_pings(min_dist_bw_consecutive_pings="invalid_string")


def test_remove_nearby_pings_negative_parameter_value():
    """Test for negative value for min_dist_bw_consecutive_pings."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('min_dist_bw_consecutive_pings cannot be negative', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.remove_nearby_pings(min_dist_bw_consecutive_pings=-5)


def test_remove_nearby_pings_zero_parameter_value():
    """Test for zero value for min_dist_bw_consecutive_pings."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    # Should run without any error
    trace_data_obj.remove_nearby_pings(min_dist_bw_consecutive_pings=0)

    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert ping["update_status"] == "unchanged"


def test_impute_distorted_pings_invalid_type():
    """Test for invalid data type for max_dist_ratio."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("max_dist_ratio must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_distance(max_dist_ratio="invalid_string")


def test_impute_distorted_pings_negative_value():
    """Test for negative value for max_dist_ratio."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_dist_ratio can not be less than 0, but got -3', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_distance(max_dist_ratio=-3)


def test_impute_distorted_pings_zero_value():
    """Test for zero value for max_dist_ratio."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_dist_ratio can not be less than 0, but got 0', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_distance(max_dist_ratio=0)


def test_impute_distorted_pings_with_angle_invalid_parameter_type():
    """Test for invalid data type for max_delta_angle."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('ping_batch_size cannot be less than 2', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_angle(max_delta_angle="invalid_string")


def test_impute_distorted_pings_with_angle_negative_parameter_value():
    """Test for negative value for max_delta_angle."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_delta_angle cannot be negative', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_angle(max_delta_angle=-30)


def test_impute_distorted_pings_with_angle_zero_parameter_value():
    """Test for zero value for max_delta_angle."""

    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Should run without any issue
    trace_data_obj.impute_distorted_pings_with_angle(max_delta_angle=0)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for i in range(1, len(clean_output["cleaned_trace"])-1):
        ping = clean_output["cleaned_trace"][i]
        assert (ping["update_status"] == "updated") or (ping["update_status"] == "unchanged")


def test_impute_distorted_pings_with_angle_out_range_parameter_value():
    """Test for negative value for max_delta_angle."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_delta_angle can have value only in range 0 to 180, but got 190', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_angle(max_delta_angle=190)


def test_impute_distorted_pings_with_angle_invalid_parameter_type():
    """Test for invalid data type for max_delta_angle."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("max_delta_angle must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.impute_distorted_pings_with_angle(max_delta_angle="invalid_string")


def test_map_match_trace_invalid_osrm_url():
    """Test for invalid data type for osrm_url."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("osrm_url must be of type String but found <class \'int\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(osrm_url=12345)  # Invalid type: int


def test_map_match_trace_invalid_ping_batch_size():
    """Test for invalid data type for ping_batch_size."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("ping_batch_size must be of type Int but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(ping_batch_size="invalid_string")  # Invalid type: string


def test_map_match_trace_negative_ping_batch_size():
    """Test for negative value for ping_batch_size."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('ping_batch_size cannot be less than 2', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(ping_batch_size=-5)


def test_map_match_trace_zero_ping_batch_size():
    """Test for zero value for ping_batch_size."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('ping_batch_size cannot be less than 2', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(ping_batch_size=0)


def test_map_match_trace_invalid_value_ping_batch_size():
    """Test for value for ping_batch_size that is positive but <2."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('ping_batch_size cannot be less than 2', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(ping_batch_size=1)


def test_map_match_trace_negative_map_matching_radius():
    """Test for negative value for map_matching_radius."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('map_matching_radius cannot be negative', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(map_matching_radius=-20)


def test_map_match_trace_invalid_map_matching_radius():
    """Test for invalid data type value for map_matching_radius."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("map_matching_radius must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(map_matching_radius="invalid")


def test_map_match_trace_zero_map_matching_radius():
    """Test for zero value for map_matching_radius."""
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)

    # Should run without any error
    trace_data_obj.map_match_trace(map_matching_radius=0)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert ping["update_status"] == "unchanged"


def test_map_match_trace_negative_avg_snap_distance():
    """Test for negative value for avg_snap_distance."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('avg_snap_distance cannot be negative', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(avg_snap_distance=-10)


def test_map_match_trace_invalid_avg_snap_distance():
    """Test for invalid data type for avg_snap_distance."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("avg_snap_distance must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(avg_snap_distance="invalid")


def test_map_match_trace_zero_avg_snap_distance():
    """Test for zero data type for avg_snap_distance."""
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)
    
    # Should run without any error
    trace_data_obj.map_match_trace(avg_snap_distance=0)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert ping["update_status"] == "unchanged"


def test_map_match_trace_negative_max_matched_dist_to_raw_dist_ratio():
    """Test for negative value for max_matched_dist_to_raw_dist_ratio."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_matched_dist_to_raw_dist_ratio cannot be negative', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(max_matched_dist_to_raw_dist_ratio=-1.5)


def test_map_match_trace_zero_max_matched_dist_to_raw_dist_ratio():
    """Test for zero value for max_matched_dist_to_raw_dist_ratio."""
    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)
    
    # Should run without any error
    trace_data_obj.map_match_trace(max_matched_dist_to_raw_dist_ratio=0)
    clean_output = trace_data_obj.get_trace_cleaning_output()


def test_map_match_trace_invalid_max_matched_dist_to_raw_dist_ratio():
    """Test for invalid data type value for max_matched_dist_to_raw_dist_ratio."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("max_matched_dist_to_raw_dist_ratio must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.map_match_trace(max_matched_dist_to_raw_dist_ratio="invalid")


def test_interpolate_trace_invalid_osrm_url():
    """Test for invalid data type for osrm_url."""

    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("osrm_url must be of type String but found <class \'int\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(osrm_url=12345)  # Invalid type: int


def test_interpolate_trace_invalid_min_dist_from_prev_ping():
    """Test for invalid data type for min_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("min_dist_from_prev_ping must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(min_dist_from_prev_ping="invalid_string")  # Invalid type: string


def test_interpolate_trace_negative_min_dist_from_prev_ping():
    """Test for negative value for min_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('min_dist_from_prev_ping cannot be less than or equal to zero', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(min_dist_from_prev_ping=-20)


def test_interpolate_trace_zero_min_dist_from_prev_ping():
    """Test for zero value for min_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('min_dist_from_prev_ping cannot be less than or equal to zero', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(min_dist_from_prev_ping=0)


def test_interpolate_trace_invalid_max_dist_from_prev_ping():
    """Test for invalid data type for max_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape('("max_dist_from_prev_ping must be of type Int or Float but found <class \'str\'>", 4002)')

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(max_dist_from_prev_ping="invalid_string")  # Invalid type: string


def test_interpolate_trace_zero_max_dist_from_prev_ping():
    """Test for zero value for max_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_dist_from_prev_ping cannot be less than or equal to zero', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(max_dist_from_prev_ping=0)


def test_interpolate_trace_negative_max_dist_from_prev_ping():
    """Test for negative value for max_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('max_dist_from_prev_ping cannot be less than or equal to zero', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(max_dist_from_prev_ping=-200)


def test_interpolate_trace_invalid_combination_for_min_and_max_dist_from_prev_ping():
    """Test for invalid value combination for min_dist_from_prev_ping and max_dist_from_prev_ping."""
    payload = load_trace_payload("dummy_trace_input_payload")
    payload["trace"] = payload["trace"][:100]
    trace_data_obj = CleanTrace(payload)

    expected_error_msg = re.escape("('min_dist_from_prev_ping must be less than max_dist_from_prev_ping', 4003)")

    with pytest.raises(ValidationException, match=expected_error_msg):
        trace_data_obj.interpolate_trace(min_dist_from_prev_ping=200, max_dist_from_prev_ping=100)


###################
# Test run examples
###################

def test_run_stop_summary_example():

    tracely_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Construct the command to run
    command = ['python', '-m', 'examples.stop_summary_example']

    # Change the working directory to the tracely directory
    result = subprocess.run(command, cwd=tracely_dir)

    # Check if the command was successful
    assert result.returncode == 0


def test_run_trace_cleaning_example():

    tracely_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Construct the command to run
    command = ['python', '-m', 'examples.trace_cleaning_example']

    # Change the working directory to the tracely directory
    result = subprocess.run(command, cwd=tracely_dir)

    # Check if the command was successful
    assert result.returncode == 0
