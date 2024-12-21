from .. import constants
from .data_validation_utils import DataValidationUtils
from ..exceptions.custom_exceptions import ValidationException
from ..exceptions.error_messages import ValidationErrorMessage, ValidationErrorCode


def validate_cleaned_trace(cleaned_trace):
    """
    Validates the structure and contents of cleaned trace.

    Args:
        cleaned_trace (list): A list of dictionaries representing cleaned pings.

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the ping.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in the ping.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If metadata in ping contains a key of type other than str.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If input_latitude in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_latitude in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_stop_event_latitude in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If input_longitude in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_longitude in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_stop_event_longitude in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If timestamp in ping is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If error_radius in ping is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If event_type in ping is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If force_retain in ping is not of data type bool.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_status in ping is not of data type bool.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata in ping is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If update_status in ping is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If last_updated_by in ping is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id in ping is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cumulative_stop_event_time in ping is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_sequence_number in ping is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If time_since_prev_ping is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If dist_from_prev_ping is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_trace_cumulative_dist is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_trace_cumulative_time is not of data type int or float.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If error_radius in ping is negative or equal to zero.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If time_since_prev_ping is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If dist_from_prev_ping is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cleaned_trace_cumulative_dist is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cleaned_trace_cumulative_time is negative.

        ValidationException (INVALID_TIME_EXCEPTION_CODE: 4004): If timestamp in ping is not in range [0, 2145916800000].

        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If input_latitude in ping is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If cleaned_latitude in ping is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_stop_event_latitude in ping is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If input_longitude in ping is not in range [-180 to 180].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If cleaned_longitude in ping is not in range [-180 to 180].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_stop_event_longitude in ping is not in range [-180 to 180].
    """

    DataValidationUtils.check_empty_list(cleaned_trace, "cleaned_trace")

    # Mapping keys to their respective validation functions
    validation_functions = {
        "input_latitude": lambda value: DataValidationUtils.check_latitude(value, name="input_latitude"),
        "cleaned_latitude": lambda value: DataValidationUtils.check_latitude(value, name="cleaned_latitude"),
        "input_longitude": lambda value: DataValidationUtils.check_longitude(value, name="input_longitude"),
        "cleaned_longitude": lambda value: DataValidationUtils.check_longitude(value, name="cleaned_longitude"),

        "timestamp": lambda value: DataValidationUtils.check_timestamp(value, name="timestamp"),
        "error_radius": lambda value: DataValidationUtils.check_error_radius(value, name="error_radius"),
        "event_type": lambda value: DataValidationUtils.check_event_type(value, name="event_type"),
        "force_retain": lambda value: DataValidationUtils.check_bool(value, name="force_retain"),
        "metadata": lambda value: DataValidationUtils.check_dict_with_str_keys(value, name="metadata"),

        "update_status": lambda value: DataValidationUtils.check_string(value, name="update_status"),
        "last_updated_by": lambda value: DataValidationUtils.check_string(value, name="last_updated_by"),
        "ping_id": lambda value: DataValidationUtils.check_string(value, name="ping_id"),

        "stop_event_status": lambda value: DataValidationUtils.check_bool(value, name="stop_event_status"),
        "cumulative_stop_event_time": lambda value: DataValidationUtils.check_string(value, name="cumulative_stop_event_time"),
        "representative_stop_event_latitude": lambda value: DataValidationUtils.check_latitude(value, name="representative_stop_event_latitude"),
        "representative_stop_event_longitude": lambda value: DataValidationUtils.check_longitude(value, name="representative_stop_event_longitude"),
        "stop_event_sequence_number": lambda value: DataValidationUtils.check_int(value, name="stop_event_sequence_number"),

        "time_since_prev_ping": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="time_since_prev_ping"),
        "dist_from_prev_ping": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="dist_from_prev_ping"),

        "cleaned_trace_cumulative_dist": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="cleaned_trace_cumulative_dist"),
        "cleaned_trace_cumulative_time": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="cleaned_trace_cumulative_time")
    }

    # Check mandatory keys in each ping
    for ping in cleaned_trace:
        DataValidationUtils.check_empty_dict(ping, "ping")

        for key in constants.CLEAN_TRACE_COLUMNS:
            DataValidationUtils.check_key_in_a_dict(ping, key)

        # Check data type of keys in ping
        for key, value in ping.items():
            if key in validation_functions:
                validation_functions[key](value)
            else:
                raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format("ping"), 
                                          ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)


def _validate_cleaning_summary(cleaning_summary, raw_trace_length, name="cleaning summary"):
    """
    Validates the structure and contents of cleaning summary.

    Args:
        cleaning_summary (dict): A dictionary containing cleaning summary.
        raw_trace_length (int): An integer representing the number of pings in input payload.
        name (str, optional): The name of the `cleaning_summary` variable, used in error messages. Defaults to "cleaning summary".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the cleaning summary.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaning summary is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaning summary is an empty dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_pings_in_input is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_non_null_pings_in_input is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_non_null_pings_in_output is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_trace_time is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If unchanged_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If drop_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If updation_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If interpolation_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_execution_time is not of data type int or float.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_pings_in_input is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_non_null_pings_in_input is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_non_null_pings_in_output is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If unchanged_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If drop_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If updation_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If interpolation_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_execution_time is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_non_null_pings_in_input in cleaning summary is less than total_pings_in_input.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_pings_in_input in cleaning summary is not equal to raw_trace_length.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If the sum of unchanged_percentage, drop_percentage, updation_percentage and interpolation_percentage is less than 99.9%.
    """

    DataValidationUtils.check_empty_dict(cleaning_summary, "cleaning summary")

    for key in constants.CLEANING_SUMMARY_KEYS:
        DataValidationUtils.check_key_in_a_dict(cleaning_summary, key)

    # Mapping keys to their respective validation functions
    validation_functions = {
        "total_pings_in_input": lambda value: DataValidationUtils.check_non_negative_int(value, name="total_pings_in_input"),
        "total_non_null_pings_in_input": lambda value: DataValidationUtils.check_non_negative_int(value, name="total_non_null_pings_in_input"),
        "total_non_null_pings_in_output": lambda value: DataValidationUtils.check_non_negative_int(value, name="total_non_null_pings_in_output"),
        "total_trace_time": lambda value: DataValidationUtils.check_string(value, name="total_trace_time"),
        "unchanged_percentage": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="unchanged_percentage"),
        "drop_percentage": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="drop_percentage"),
        "updation_percentage": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="updation_percentage"),
        "interpolation_percentage": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="interpolation_percentage"),
        "total_execution_time": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="total_execution_time")
    }

    # Check data type of keys in ping
    for key, value in cleaning_summary.items():
        if key in validation_functions:
            validation_functions[key](value)
        else:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)
        
    if not (cleaning_summary["total_pings_in_input"] == raw_trace_length):
        raise ValidationException(ValidationErrorMessage.INCORRECT_PINGS_COUNT_IN_CLEANING_SUMMARY,
                                  ValidationErrorCode.VALUE_EXCEPTION_CODE)

    if not (cleaning_summary["total_non_null_pings_in_input"] <= cleaning_summary["total_pings_in_input"]):
        raise ValidationException(ValidationErrorMessage.INCORRECT_INPUT_PINGS_COUNT,
                                  ValidationErrorCode.VALUE_EXCEPTION_CODE)
    
    if not cleaning_summary["unchanged_percentage"] + \
           cleaning_summary["drop_percentage"] + \
           cleaning_summary["updation_percentage"] + \
           cleaning_summary["interpolation_percentage"] >= 99.9:
        
        raise ValidationException(ValidationErrorMessage.INCORRECT_STATUS_PERCENTAGES_SUM,
                                  ValidationErrorCode.VALUE_EXCEPTION_CODE)
    

def _validate_distance_summary(distance_summary, name="distance_summary"):
    """
    Validates the structure and contents of distance summary.

    Args:
        distance_summary (dict): A dictionary containing distance summary.
        name (str, optional): The name of the data variable, used in error messages. Defaults to "distance_summary".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the distance summary.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If drop_percentage in distance summary is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cumulative_distance_of_raw_trace in distance summary is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cumulative_distance_of_clean_trace in distance summary is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If percent_reduction_in_dist in distance summary is not of data type int, float or None.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If drop_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cumulative_distance_of_raw_trace is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cumulative_distance_of_clean_trace is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If percent_reduction_in_dist is negative.
    """

    DataValidationUtils.check_empty_dict(distance_summary, "distance_summary")

    for key in constants.DISTANCE_SUMMARY_KEYS:
        DataValidationUtils.check_key_in_a_dict(distance_summary, key)

    # Mapping keys to their respective validation functions
    validation_functions = {
        "drop_percentage": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="drop_percentage"),
        "cumulative_distance_of_raw_trace": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="cumulative_distance_of_raw_trace"),
        "cumulative_distance_of_clean_trace": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="cumulative_distance_of_clean_trace"),
        "percent_reduction_in_dist": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="percent_reduction_in_dist")
    }

    # Check data type of keys in ping
    for key, value in distance_summary.items():
        if key in validation_functions:
            validation_functions[key](value)
        else:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)


def _validate_stop_event_info(stop_event_info, name="stop event info"):
    """
    Validates the structure and contents of stop event info dictionary within stop events summary.

    Args:
        stop_event_info (dict): A dictionary containing stop event information of a stop point.
        name (str, optional): The name of the data variable.. Defaults to "stop event info".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from stop_events_info.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in stop_events_info.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_sequence_number is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If number_of_pings is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If start_time is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If end_time is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_stop_event_time is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_latitude is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_longitude is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_info is not of data type dict.
        
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_latitude is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_longitude is not in range [-180 to 180].
    """

    DataValidationUtils.check_empty_dict(stop_event_info, name)

    for key in constants.STOP_EVENT_INFO_KEYS:
        DataValidationUtils.check_key_in_a_dict(stop_event_info, key)

    # Mapping keys to their respective validation functions
    validation_functions = {
        "stop_event_sequence_number": lambda value: DataValidationUtils.check_int(value, name="stop_event_sequence_number"),
        "start_time": lambda value: DataValidationUtils.check_string(value, name="start_time"),
        "end_time": lambda value: DataValidationUtils.check_string(value, name="end_time"),
        "total_stop_event_time": lambda value: DataValidationUtils.check_string(value, name="total_stop_event_time"),
        "number_of_pings": lambda value: DataValidationUtils.check_non_negative_int(value, name="number_of_pings"),
        "representative_latitude": lambda value: DataValidationUtils.check_latitude(value, name="representative_latitude"),
        "representative_longitude": lambda value: DataValidationUtils.check_longitude(value, name="representative_longitude")
    }

    # Check data type of keys
    for key, value in stop_event_info.items():
        if key in validation_functions:
            validation_functions[key](value)
        else:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)


def _validate_global_stop_events_info(global_stop_events_info, name="global stop event info"):
    """
    Validates the structure and contents of global stop event info within stop events summary.

    Args:
        global_stop_events_info (dict): A dictionary containing the stop information of entire trace.
        name (str, optional): The name of the data variable. Defaults to "global stop event info".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from global_stop_events_info.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in global_stop_events_info.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_trace_time is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_stop_events_time is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If global_stop_events_info is not of data type dict.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If stop_event_percentage is negative.
    """

    DataValidationUtils.check_empty_dict(global_stop_events_info, name)

    for key in constants.GLOBAL_STOP_EVENTS_INFO_KEYS:
        DataValidationUtils.check_key_in_a_dict(global_stop_events_info, key)

    # Mapping keys to their respective validation functions
    validation_functions = {
        "stop_event_percentage": lambda value: DataValidationUtils.check_non_negative_int_or_float(value, name="stop_event_percentage"),
        "total_trace_time": lambda value: DataValidationUtils.check_string(value, name="total_trace_time"),
        "total_stop_events_time": lambda value: DataValidationUtils.check_string(value, name="total_stop_events_time")
    }

    # Check data type of keys
    for key, value in global_stop_events_info.items():
        if key in validation_functions:
            validation_functions[key](value)
        else:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)


def _validate_stop_summary(stop_summary, name="stop_summary"):
    """
    Validates the structure and contents of stop summary.

    Args:
        stop_summary (list): A dictionary containing distance summary.
        name (str, optional): The name of the data variable. Defaults to "stop_summary".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the stop summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from global_stop_events_info.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in global_stop_events_info.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from from an element in stop_events_info.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in an element in stop_events_info.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_events_info is not of data type list.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_percentage in global_stop_events_info is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_trace_time in global_stop_events_info is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_stop_events_time in global_stop_events_info is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If global_stop_events_info is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_sequence_number in an element in stop_events_info is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If number_of_pings in an element in stop_events_info is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If start_time in an element in stop_events_info is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If end_time in an element in stop_events_info is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_stop_event_time in an element in stop_events_info is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_latitude in an element in stop_events_info is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_longitude in an element in stop_events_info is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If an element in stop_events_info is not of data type dict.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If stop_event_percentage in global_stop_events_info is negative.

        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_latitude in an element in stop_events_info is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_longitude in an element in stop_events_info is not in range [-180 to 180].
    """
    
    DataValidationUtils.check_empty_dict(stop_summary, name)
    for key in constants.STOP_SUMMARY_KEYS:
        DataValidationUtils.check_key_in_a_dict(stop_summary, key)

    # Mapping keys to their respective validation functions
    validation_functions = {
        "stop_events_info": lambda value: DataValidationUtils.check_list(value, name="stop_events_info"),
        "global_stop_events_info": lambda value: DataValidationUtils.check_empty_dict(value, name="global_stop_events_info")
    }

    # Check data type of keys in ping
    for key, value in stop_summary.items():
        if key in validation_functions:
            validation_functions[key](value)
        else:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    # Validate information of each stop event
    for stop_event_info in stop_summary["stop_events_info"]:
        _validate_stop_event_info(stop_event_info)

    # Validate stop information of entire trace
    _validate_global_stop_events_info(stop_summary["global_stop_events_info"])


def validate_clean_trace_output(clean_trace_output, raw_trace_length, name="clean trace output"):
    """
    Validates the structure and contents of clean trace output.

    Args:
        clean_trace_output (dict): A dictionary containing cleaned trace, cleaning summary, distance summary, stop summary
        raw_trace_length (int): An integer representing the number of pings in input payload.
        name (str, optional): The name of the data variable. Defaults to "clean trace output".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the clean trace output.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from a ping in cleaned_trace.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in a ping in cleaned_trace.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in cleaning output.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in cleaning_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in distance_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in stop_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If metadata key in a ping in cleaned_trace contains a key of type other than string.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the cleaning summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the distance summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the stop summary. global_stop_events_info is present in stop_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from global_stop_events_info. global_stop_events_info is present in stop_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in global_stop_events_info. global_stop_events_info is present in stop_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from from an element in stop_events_info. global_stop_events_info is present in stop_summary.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in an element in stop_events_info. global_stop_events_info is present in stop_summary.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If a ping in cleaned_trace is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If input_latitude in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_latitude in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_stop_event_latitude in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If input_longitude in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_longitude in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_stop_event_longitude in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If timestamp in a ping in cleaned_trace is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If error_radius in a ping in cleaned_trace is not of data type int, float or None.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If event_type in a ping in cleaned_trace is not of data type string.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If force_retain in a ping in cleaned_trace is not of data type bool.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_status in a ping in cleaned_trace is not of data type bool.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata in a ping in cleaned_trace is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If update_status in a ping in cleaned_trace is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If last_updated_by in a ping in cleaned_trace is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id in a ping in cleaned_trace is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cumulative_stop_event_time in a ping in cleaned_trace is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_sequence_number in a ping in cleaned_trace is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If time_since_prev_ping is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If dist_from_prev_ping is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_trace_cumulative_dist in a ping in cleaned_trace is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaned_trace_cumulative_time is in a ping in cleaned_trace not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaning summary is not of data type dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cleaning summary is an empty dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_pings_in_input in cleaning_summary is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_non_null_pings_in_input in cleaning_summary is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_non_null_pings_in_output in cleaning_summary is not of data type int.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_trace_time in cleaning_summary is not of data type str.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If unchanged_percentage in cleaning_summary is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If drop_percentage in distance_summary is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If updation_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If interpolation_percentage is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_execution_time is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cumulative_distance_of_raw_trace in distance_summary is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If cumulative_distance_of_clean_trace in distance_summary is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If percent_reduction_in_dist in distance_summary is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_events_info in stop_summary is not of data type list.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_events_info is not of data type list. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_percentage in global_stop_events_info is not of data type int or float. global_stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_trace_time in global_stop_events_info is not of data type str. global_stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_stop_events_time in global_stop_events_info is not of data type str. global_stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If global_stop_events_info is not of data type dict. global_stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If stop_event_sequence_number in an element in stop_events_info is not of data type int. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If number_of_pings in an element in stop_events_info is not of data type int. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If start_time in an element in stop_events_info is not of data type str. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If end_time in an element in stop_events_info is not of data type str. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If total_stop_event_time in an element in stop_events_info is not of data type str. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_latitude in an element in stop_events_info is not of data type int, float or None. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If representative_longitude in an element in stop_events_info is not of data type int, float or None. stop_events_info is present in stop_summary.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If an element in stop_events_info is not of data type dict. stop_events_info is present in stop_summary.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If error_radius in a a ping in cleaned_trace in cleaned_trace is negative or equal to zero.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If time_since_prev_ping is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If dist_from_prev_ping is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cleaned_trace_cumulative_dist in a ping in cleaned_trace is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cleaned_trace_cumulative_time in a ping in cleaned_trace is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_pings_in_input in cleaning_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_non_null_pings_in_input in cleaning_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_non_null_pings_in_output in cleaning_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If unchanged_percentage in cleaning_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If drop_percentage in distance_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If updation_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If interpolation_percentage is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_execution_time is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cumulative_distance_of_raw_trace in distance_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If cumulative_distance_of_clean_trace in distance_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If percent_reduction_in_dist in distance_summary is negative.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If stop_event_percentage in global_stop_events_info is negative. global_stop_events_info is present in stop_summary.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_non_null_pings_in_input in cleaning_summary is less than total_pings_in_input.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If total_pings_in_input in cleaning_summary is not equal to raw_trace_length.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If the sum of unchanged_percentage, drop_percentage, updation_percentage and interpolation_percentage in cleaning_summary is less than 99.9%.

        ValidationException (INVALID_TIME_EXCEPTION_CODE: 4004): If timestamp in a ping in cleaned_trace is not an integer or not in range [0, 2145916800000].

        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If input_latitude in a ping in cleaned_trace is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If cleaned_latitude in a ping in cleaned_trace is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_stop_event_latitude in a ping in cleaned_trace is not in range [-90 to 90].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If input_longitude in a ping in cleaned_trace is not in range [-180 to 180].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If cleaned_longitude in a ping in cleaned_trace is not in range [-180 to 180].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_stop_event_longitude in a ping in cleaned_trace is not in range [-180 to 180].
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_latitude in an element in stop_events_info is not in range [-90 to 90]. stop_events_info is present in stop_summary.
        ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If representative_longitude in an element in stop_events_info is not in range [-180 to 180]. stop_events_info is present in stop_summary.
    """
    
    DataValidationUtils.check_empty_dict(clean_trace_output,name)

    for key in constants.CLEANING_OUTPUT_KEYS:
        DataValidationUtils.check_key_in_a_dict(clean_trace_output, key)

    for key, value in clean_trace_output.items():
        if key not in constants.CLEANING_OUTPUT_KEYS:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    validate_cleaned_trace(clean_trace_output["cleaned_trace"])
    _validate_cleaning_summary(clean_trace_output["cleaning_summary"], raw_trace_length=raw_trace_length)
    _validate_distance_summary(clean_trace_output["distance_summary"])
    _validate_stop_summary(clean_trace_output["stop_summary"])


def _validate_metadata(metadata, length_trace_1, length_trace_2, name="metadata"):
    """Validates the structure and contents of metadata in trace similarity output.

    Args:
        metadata (dict): Metadata to be validated.
        length_trace_1 (int): Length of trace_1 (number of pings).
        length_trace_2 (int): Length of trace_2 (number of pings).
        name (str, optional): Name of metadata dict. Defaults to "metadata".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from metadata.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from similarity data of individual trace in metadata.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in metadata.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in similarity data of individual trace in metadata.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata is an empty dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If similarity data of individual trace in metadata is an empty dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If similarity_percentage in similarity data of individual trace is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If overlapping_pings_indices in similarity data of individual trace is not a list.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If an index pair within overlapping_pings_indices is not a list.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If an index pair list within overlapping_pings_indices is not of length 2.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If similarity_percentage in similarity data of individual trace is not in range [1, 100].
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If length of overlapping_pings_indices in similarity_info_trace_1_to_2 is greater than length of trace_1 itself.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If length of overlapping_pings_indices in similarity_info_trace_2_to_1 is greater than length of trace_2 itself.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If an index in a pair in overlapping_pings_indices is greater than the length of its respective trace itself.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If similarity_percentage in similarity data of individual trace is negative.
    """

    # Check empty metadata dict
    DataValidationUtils.check_empty_dict(metadata, "metadata")
    
    # Check mandatory keys
    for key in constants.TRACE_SIMILARITY_OUTPUT_METADATA_KEYS:
        DataValidationUtils.check_key_in_a_dict(metadata, key)

    # Check unexpected keys
    for key, value in metadata.items():
        if key not in constants.TRACE_SIMILARITY_OUTPUT_METADATA_KEYS:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    # Within metadata, check individual trace's similarity data
    for metadata_key in constants.TRACE_SIMILARITY_OUTPUT_METADATA_KEYS:
        similarity_data = metadata[metadata_key]

        # Check empty similarity_data dict
        DataValidationUtils.check_empty_dict(similarity_data, "similarity data in metadata")
        
        # Check mandatory keys
        for sim_info_key in constants.TRACE_SIMILARITY_OUTPUT_METADATA_INDIVIDUAL_TRACE_SIM_INFO_KEYS:
            DataValidationUtils.check_key_in_a_dict(similarity_data, sim_info_key)

        # Check unexpected keys
        for sim_info_key, value in similarity_data.items():
            if sim_info_key not in constants.TRACE_SIMILARITY_OUTPUT_METADATA_INDIVIDUAL_TRACE_SIM_INFO_KEYS:
                raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                        ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

        # Validate similarity_percentage
        similarity_percentage = similarity_data["similarity_percentage"]

        # Check datatype and value
        DataValidationUtils.check_int_or_float(similarity_percentage, "similarity_percentage")

        if not (0 <= similarity_percentage <= 100):
            raise ValidationException(ValidationErrorMessage.INCORRECT_SIMILARITY_PERCENTAGE,
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)
       
        # Validate overlapping_pings_indices
        overlapping_pings_indices = similarity_data["overlapping_pings_indices"]
        DataValidationUtils.check_list(overlapping_pings_indices, "overlapping_pings_indices")
        
        # Validate each index pair in overlapping_pings_indices
        for index_pair in overlapping_pings_indices:
            # Index pair must be a list
            DataValidationUtils.check_list(index_pair, "index_pair")
 
            # Index pair must contain exactly 2 element (indices)
            if len(index_pair) != 2: 
                raise ValidationException(ValidationErrorMessage.INVALID_INDEX_PAIR,
                                        ValidationErrorCode.VALUE_EXCEPTION_CODE)
        
        # Length validations based on key
        if metadata_key == 'similarity_info_trace_1_to_2':
            # Check if count of index pairs is less than then count of pings in trace_1
            if len(overlapping_pings_indices) > length_trace_1:
                raise ValidationException(ValidationErrorMessage.INVALID_OVERLAPPING_INDICES_LIST_LENGTH,
                                        ValidationErrorCode.VALUE_EXCEPTION_CODE)
            
            # Check value of indices
            for index_pair in overlapping_pings_indices:
                if (index_pair[0] < 0) or (index_pair[0] > length_trace_1):
                    raise ValidationException(ValidationErrorMessage.INVALID_OVERLAPPING_INDEX_VALUE,
                                              ValidationErrorCode.VALUE_EXCEPTION_CODE)
                
                elif (index_pair[1] < 0) or (index_pair[1] > length_trace_2):
                    raise ValidationException(ValidationErrorMessage.INVALID_OVERLAPPING_INDEX_VALUE,
                                              ValidationErrorCode.VALUE_EXCEPTION_CODE)
        
        elif metadata_key == 'similarity_info_trace_2_to_1':
            # Check if count of index pairs is less than then count of pings in trace_2
            if len(overlapping_pings_indices) > length_trace_2:
                raise ValidationException(ValidationErrorMessage.INVALID_OVERLAPPING_INDICES_LIST_LENGTH,
                                            ValidationErrorCode.VALUE_EXCEPTION_CODE)
            
            # Check value of indices
            for index_pair in overlapping_pings_indices:
                if (index_pair[0] < 0) or (index_pair[0] > length_trace_2):
                    raise ValidationException(ValidationErrorMessage.INVALID_OVERLAPPING_INDEX_VALUE,
                                              ValidationErrorCode.VALUE_EXCEPTION_CODE)
                
                if (index_pair[1] < 0) or (index_pair[1] > length_trace_1):
                    raise ValidationException(ValidationErrorMessage.INVALID_OVERLAPPING_INDEX_VALUE,
                                              ValidationErrorCode.VALUE_EXCEPTION_CODE)


def validate_trace_similarity_output(trace_similarity_output, length_trace_1, length_trace_2, name="trace_similarity_output"):
    """Validates the structure and contents of trace similarity output.

    Args:
        trace_similarity_output (dict): Trace similarity dict to be validated.
        length_trace_1 (int): Length of trace_1 (number of pings).
        length_trace_2 (int): Length of trace_2 (number of pings).
        name (str, optional): Name of input dict. Defaults to "trace_similarity_output".

    Raises:
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from trace_similarity_output.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from similarity data of individual trace in metadata.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from metadata in trace_similarity_output.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in metadata in trace_similarity_output.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in similarity data of individual trace.
        ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in trace_similarity_output.

        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If trace_similarity_output is an empty dict. 
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata is an empty dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If similarity data of individual trace in metadata is an empty dict.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If similarity_percentage in metadata is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If similarity_percentage in trace_similarity_output is not of data type int or float.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If overlapping_pings_indices in metadata is not a list.
        ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If an index pair within overlapping_pings_indices in metadata is not a list.

        ValidationException (VALUE_EXCEPTION_CODE: 4003): If an index pair list within overlapping_pings_indices in metadata is not of length 2.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If similarity_percentage in metadata is not in range [1, 100].
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If similarity_percentage in trace_similarity_output is not in range [1, 100].
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If length of overlapping_pings_indices in similarity_info_trace_1_to_2 is greater than length of trace_1 itself.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If length of overlapping_pings_indices in similarity_info_trace_2_to_1 is greater than length of trace_2 itself.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If an index in overlapping_pings_indices is greater than length of its respective trace itself.
        ValidationException (VALUE_EXCEPTION_CODE: 4003): If similarity_percentage in similarity data of individual trace in metadata of trace_similarity_output is negative.
    """

    # Check empty trace similarity output dict
    DataValidationUtils.check_empty_dict(trace_similarity_output, name)

    # Check mandatory key
    for key in constants.TRACE_SIMILARITY_OUTPUT_KEYS:
        DataValidationUtils.check_key_in_a_dict(trace_similarity_output, key)

    # Check unexpected key
    for key, value in trace_similarity_output.items():
        if key not in constants.TRACE_SIMILARITY_OUTPUT_KEYS:
            raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    # Validate similarity_percentage
    similarity_percentage = trace_similarity_output['similarity_percentage']
    DataValidationUtils.check_int_or_float(similarity_percentage, "similarity_percentage")

    if not (0 <= similarity_percentage <= 100):
        raise ValidationException(ValidationErrorMessage.INCORRECT_SIMILARITY_PERCENTAGE,
                                    ValidationErrorCode.VALUE_EXCEPTION_CODE)

    # Validate metadata
    _validate_metadata(trace_similarity_output["metadata"], length_trace_1, length_trace_2, name="metadata")
