class IOErrorCode:
    FILE_READ_ERROR_CODE = 1001
    MANDATORY_COLUMN_ERROR_CODE = 1002


class IOErrorMessage:
    UNABLE_TO_READ_CSV_FILE = "Unable to read input CSV file"
    MANDATORY_COLUMN_MISSING_FROM_CSV = "Expected column: '{}' missing from the input CSV file"


class ValidationErrorCode:
    KEY_ERROR_EXCEPTION_CODE = 4001
    DATA_FORMAT_EXCEPTION_CODE = 4002
    VALUE_EXCEPTION_CODE = 4003
    INVALID_TIME_EXCEPTION_CODE = 4004
    INVALID_COORDS_EXCEPTION_CODE = 4005


class ValidationErrorMessage:
    DATA_IS_NOT_DICT = "{} must be of type Dict but found {}"
    DATA_IS_EMPTY_DICT = "{} cannot be an empty dictionary"
    DATA_IS_NOT_INT = "{} must be of type Int but found {}"
    DATA_IS_NOT_FLOAT = "{} must be of type Float but found {}"
    DATA_IS_NOT_INT_OR_FLOAT = "{} must be of type Int or Float but found {}"
    DATA_IS_NOT_INT_OR_FLOAT_OR_NONE = "{} must be of type Int, Float or None but found {}"
    DATA_IS_NOT_STRING = "{} must be of type String but found {}"
    DATA_IS_NOT_STRING_OR_NONE = "{} must be of type String or None but found {}"
    DATA_IS_NOT_LIST = "{} must be of type List but found {}"
    DATA_IS_AN_EMPTY_LIST = "{} cannot be an empty list"
    DATA_IS_NOT_BOOL = "{} must be of type Bool but found {}"
    DATA_IS_NOT_TUPLE = "{} must be of type Tuple but found {}"
    DATA_IS_ZERO = "{} cannot be zero"
    DATA_IS_NEGATIVE = "{} cannot be negative"
    DATA_IS_NOT_STRICTLY_POSITIVE = "{} cannot be less than or equal to zero"
    KEY_NOT_FOUND_IN_DICT = "Expected key: '{}' missing from the dictionary"

    INVALID_LATITUDE = "{} must be within range [-90 to 90] but found latitude = {}"
    INVALID_LONGITUDE = "{} must be within range [-180 to 180] but found longitude = {}"
    INVALID_TIMESTAMP = "{} must be in milliseconds and unix epoch format within range [0, {}] but found timestamp = {}"

    INVALID_MAX_DELTA_ANGLE_FOR_IMPUTATION = "max_delta_angle can have value only in range 0 to 180, but got {}"
    INVALID_DIST_THRESHOLDS_FOR_INTERPOLATION = "min_dist_from_prev_ping must be less than max_dist_from_prev_ping"
    INVALID_PING_BATCH_SIZE_FOR_MAP_MATCHING = "ping_batch_size cannot be less than 2"
    INVALID_MIN_SIZE_FOR_STOP_EVENTS_DETECTION = "min_size cannot be less than 2"
    INVALID_MAX_DIST_RATIO_FOR_IMPUTATION = "max_dist_ratio can not be less than 0, but got {}"

    FOUND_DUPLICATE_VALUES = "Expected values for '{}' to be unique, but found duplicate values"
    MISSING_MANDATORY_KEY_IN_PING = "Expected key: {} missing from a ping dictionary"
    NON_STRING_KEYS_IN_DICT = "Expected keys of only string type in {} dictionary"
    UNEXPECTED_KEYS_IN_DICT = "Unexpected key provided in {} dictionary"

    ALL_COORDS_NONE = "Trace should have at least one ping with non null latitude and longitude"
    INVALID_PING_ID_LENGTH = "ping_id can not be an empty string"
    AMBIGUOUS_PING_ID = "ping_id must be present in either all of the pings or in none of the pings"
    INCORRECT_INPUT_PINGS_COUNT = "total_non_null_pings_in_input cannot be greater than total_pings_in_input"
    INCORRECT_STATUS_PERCENTAGES_SUM = "Sum of percentages of various update statuses should be at least 99.9"
    INCORRECT_PINGS_COUNT_IN_CLEANING_SUMMARY = "total_pings_in_input in cleaning_summary must be equal to number of pings in input payload"


class OSRMErrorCode:
    CONNECTION_ERROR_CODE = 2001


class OSRMErrorMessage:
    CONNECTION_ERROR = "Can not connect to OSRM server at URL {}"