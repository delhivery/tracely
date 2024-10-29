import os
import datetime

########################
# Define path to tracely
########################
BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "")


##############################
# Define path to test payloads
##############################
TEST_PAYLOADS_BASE_PATH = os.path.join(BASE_PATH,
                                       "tests",
                                       "test_payloads", "")


########################
# Define timezone offset
########################
timezone_offset = datetime.timedelta(hours=5,
                                     minutes=30) # offset for IST


#######################
# Keys in trace payload
#######################
MANDATORY_KEYS_IN_TRACE_PAYLOAD = [
    "trace"
]

OPTIONAL_ITEMS_IN_A_TRACE_PAYLOAD = {
    "vehicle_type": "car",
    "vehicle_speed": 25  # km/hr
}


##############
# Keys in ping
##############
MANDATORY_KEYS_IN_A_PING = [
    "latitude",
    "longitude",
    "timestamp"
]

OPTIONAL_ITEMS_IN_A_PING = {
    "ping_id": None,
    "error_radius": None,
    "event_type": None,
    "force_retain": False,
    "metadata": {},
}


####################################################
# Range of values for unix timestamp in milliseconds
####################################################
MINIMUM_UNIX_TIMESTAMP = 0
# 1st Jan 2038 00:00:00 UTC in milliseconds
MAXIMUM_UNIX_TIMESTAMP = 2145916800000


#######################################################
# Default values for the optional keys of trace payload
#######################################################
DEFAULT_VEHICLE_TYPE = "car"
DEFAULT_VEHICLE_SPEED = 25  # km/hr


########################
# Default values in ping
########################
DEFAULT_LATITUDE = None
DEFAULT_LONGITUDE = None
DEFAULT_ERROR_RADIUS = None
DEFAULT_EVENT_TYPE = None
DEFAULT_FORCE_RETAIN = False


#######################################
# Default values for other keys in ping
#######################################
DEFAULT_UPDATE_STATUS = "unchanged"
DEFAULT_LAST_UPDATED_BY = "never_updated"
DEFAULT_VALUES_FOR_ADDED_KEYS_IN_CLEANED_TRACE = {
    "update_status": "unchanged",
    "last_updated_by": "never_updated",

    "stop_event_status": False,
    "cumulative_stop_event_time": "0 minutes and 0 seconds",
    "representative_stop_event_latitude": None,
    "representative_stop_event_longitude": None,
    "stop_event_sequence_number": -1,

    "time_since_prev_ping": 0,
    "dist_from_prev_ping": 0,

    "cleaned_trace_cumulative_dist": 0,
    "cleaned_trace_cumulative_time": 0
}


###########################################################
# Keys in a ping of trace in trace payload without metadata
###########################################################
CLEAN_TRACE_COLUMNS_WITHOUT_METADATA = [
    "ping_id",
    "input_latitude",
    "input_longitude",
    "timestamp",
    "error_radius",
    "event_type",
    "force_retain",

    "cleaned_latitude",
    "cleaned_longitude",
    "update_status",
    "last_updated_by",

    "stop_event_status",
    "cumulative_stop_event_time",
    "representative_stop_event_latitude",
    "representative_stop_event_longitude",
    "stop_event_sequence_number",

    "time_since_prev_ping",
    "dist_from_prev_ping",

    "cleaned_trace_cumulative_dist",
    "cleaned_trace_cumulative_time"
]


#################################################
# Keys in a ping of trace in output trace payload
#################################################
CLEAN_TRACE_COLUMNS = CLEAN_TRACE_COLUMNS_WITHOUT_METADATA + ["metadata"]
CLEAN_PING_KEYS_COUNT = len(CLEAN_TRACE_COLUMNS)


########################
# List of stop info keys
########################
STOP_INFO_KEYS = [
    "stop_event_status",
    "representative_stop_event_latitude",
    "representative_stop_event_longitude",
    "cumulative_stop_event_time",
    "stop_event_sequence_number"
]


##################################
# List of keys in cleaning summary
##################################
CLEANING_SUMMARY_KEYS = [
    "total_pings_in_input",
    "total_non_null_pings_in_input",
    "total_non_null_pings_in_output",
    "total_trace_time",
    "unchanged_percentage",
    "drop_percentage",
    "updation_percentage",
    "interpolation_percentage",
    "total_execution_time"
]


##################################
# List of keys in distance summary 
##################################
DISTANCE_SUMMARY_KEYS = [
    "cumulative_distance_of_raw_trace",
    "cumulative_distance_of_clean_trace",
    "percent_reduction_in_dist"
]


##############################
# List of keys in stop summary 
##############################
STOP_SUMMARY_KEYS = [
    "stop_events_info",
    "global_stop_events_info"
]


#################################
# List of keys in stop event info 
#################################
STOP_EVENT_INFO_KEYS = [
    "stop_event_sequence_number",
    "start_time",
    "end_time",
    "total_stop_event_time",
    "number_of_pings",
    "representative_latitude",
    "representative_longitude"
]


##################################################
# List of keys in stop event info for entire trace
##################################################
GLOBAL_STOP_EVENTS_INFO_KEYS = [
    "total_trace_time",
    "total_stop_events_time",
    "stop_event_percentage"
]


#################################
# List of keys in cleaning output
#################################
CLEANING_OUTPUT_KEYS = [
    "cleaned_trace",
    "cleaning_summary",
    "distance_summary",
    "stop_summary"
]


#########################
# Interpolation constants
#########################
MIN_TIME_FOR_INTERPOLATED_ROUTE = 1 # seconds
MAX_INTERPOLATION_THRESHOLD_RATIO = 1.5
MIN_SPEED_FOR_INTERPOLATED_ROUTE = 1 # meters per second