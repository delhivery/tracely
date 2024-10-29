from .. import constants
from ..exceptions.custom_exceptions import ValidationException
from ..exceptions.error_messages import ValidationErrorMessage, \
                                        ValidationErrorCode


class DataValidationUtils:

    @staticmethod
    def check_dict(data, name="Data"):
        """
        Check if `data` is of type dict.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type dict.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the data variable. Defaults to "Data".
        """

        if not isinstance(data, dict):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_DICT.format(name, type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

    @staticmethod
    def check_empty_dict(data, name="Data"):
        """
        Check if `data` is of type dict and is not empty.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type dict or is empty dict.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the data variable. Defaults to "Data".
        """

        DataValidationUtils.check_dict(data, name)
        if not data:
            raise ValidationException(ValidationErrorMessage.DATA_IS_EMPTY_DICT.format(name,type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

    @staticmethod
    def check_key_in_a_dict(input_dict, key):
        """
        Checks if key is present within the `input_dict` or not.

        Raises:
            ValidationException (OUTPUT_KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the `input_dict`.

        Args:
            input_dict (dict): The dictionary for which we need to check the key.
            key (str): The key which we need to check in the `input_dict`.
        """

        if key not in input_dict:
            raise ValidationException(ValidationErrorMessage.KEY_NOT_FOUND_IN_DICT.format(key),
                                      ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    @staticmethod
    def check_int(data, name="Data"):
        """
        Check if `data` is of type int.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type int.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if not isinstance(data, int):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_INT.format(name,type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

    @staticmethod
    def check_int_or_float(data, name="Data"):
        """
        Check if `data` is of type int or float.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type int or float.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if not isinstance(data, (int, float)):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_INT_OR_FLOAT.format(name,type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

    @staticmethod
    def check_int_or_float_or_none(data, name="Data"):
        """
        Check if `data` is None or of type int or float.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is neither None nor of type int or float.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if (data is not None) and (not isinstance(data, (int, float))):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_INT_OR_FLOAT_OR_NONE.format(name,type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)
        
    @staticmethod
    def check_string(data, name="Data"):
        """
        Check if `data` is of type string.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type string.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if not isinstance(data, str):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_STRING.format(name,type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

    @staticmethod
    def check_string_or_none(data, name="Data"):
        """
        Check if `data` is of type string or None.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type string or None.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if (data is None):
            return
        
        if not isinstance(data, str):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_STRING_OR_NONE.format(name,type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)
        
    @staticmethod
    def check_list(data, name="Data"):
        """
        Check if `data` is of type list.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type list.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if not isinstance(data, list):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_LIST.format(name, type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

    @staticmethod
    def check_empty_list(data, name="Data"):
        """
        Check if `data` is of type list and data is not an empty list.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type list.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `data` is an empty list.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        DataValidationUtils.check_list(data, name)
        if (len(data) == 0):
            raise ValidationException(ValidationErrorMessage.DATA_IS_AN_EMPTY_LIST.format(name),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def check_bool(data, name="Data"):
        """
        Check if `data` is of type bool.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type bool.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        if not isinstance(data, bool):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_BOOL.format(name, type(data)),
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)
                      
    @staticmethod
    def check_strictly_positive_int(data, name="Data"):
        """
        Check if `data` is of type integer and has a value greater than zero.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type int.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `data` is negative or equal to zero.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int(data, name)
        if (data <= 0):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_STRICTLY_POSITIVE.format(name),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def check_strictly_positive_int_or_float(data, name="Data"):
        """
        Check if `data` is of type int or float and has a value greater than zero.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type int or float.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `data` is negative or equal to zero.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int_or_float(data, name)
        if (data <= 0):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NOT_STRICTLY_POSITIVE.format(name),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def check_non_negative_int(data, name="Data"):
        """
        Check if `data` is of type int and has a value greater than or equal to zero.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type int.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `data` is negative.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int(data, name)
        if (data < 0):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NEGATIVE.format(name),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def check_non_negative_int_or_float(data, name="Data"):
        """
        Check if `data` is of type int or float and has a value greater than or equal to zero.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is not of data type int or float.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `data` is negative.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int_or_float(data, name)
        if (data < 0):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NEGATIVE.format(name),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def check_non_negative_int_or_float_or_none(data, name="Data"):
        """
        Check if `data` is of type int, float or None and has a value greater than or equal to zero.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data` is neither None nor of data type int or float.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `data` is negative.

        Args:
            data: Data variable of unknown data type which needs to be validated.
            name (str, optional): The name of the `data` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int_or_float_or_none(data, name)

        if data is None:
            return
        
        if (data < 0):
            raise ValidationException(ValidationErrorMessage.DATA_IS_NEGATIVE.format(name),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)
        
    @staticmethod
    def check_latitude(latitude, name="Data"):
        """
        Check if `latitude` is of type int, float or None and is a valid latitude.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `latitude` is neither None nor of data type int or float.
            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): `latitude` must be within range [-90 to 90].

        Args:
            latitude (float): Latitude to be evaluated.
            name (str, optional): The name of the `latitude` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int_or_float_or_none(latitude, name)

        if latitude is None:
            return

        if ((latitude < -90) or (latitude > 90)):
            raise ValidationException(ValidationErrorMessage.INVALID_LATITUDE.format(name,latitude),
                                      ValidationErrorCode.INVALID_COORDS_EXCEPTION_CODE)

    @staticmethod
    def check_longitude(longitude, name="Data"):
        """
        Check if `longitude` is of type int, float or None and is a valid longitude.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `longitude` is neither None nor of data type int or float.
            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): `longitude` must be within range [-180 to 180].

        Args:
            longitude (float): Longitude to be evaluated.
            name (str, optional): The name of the `longitude` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int_or_float_or_none(longitude, name)

        if longitude is None:
            return
        
        if ((longitude < -180) or (longitude > 180)):
            raise ValidationException(ValidationErrorMessage.INVALID_LONGITUDE.format(name,longitude),
                                      ValidationErrorCode.INVALID_COORDS_EXCEPTION_CODE)

    @staticmethod
    def check_error_radius(error_radius, name="error radius"):
        """
        Check if `error_radius` is of type int, float or None, and is a valid GPS error radius.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `error_radius` is not of data type int, float or None.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `error_radius` is negative.

        Args:
            error_radius(int, float, None) : GPS error radius to be validated
            name (str, optional): The name of the `error_radius` variable. Defaults to "error radius".
        """

        DataValidationUtils.check_non_negative_int_or_float_or_none(error_radius, name)

    @staticmethod
    def check_event_type(event_type, name="Data"):
        """
        Check if `event_type` is of type string or None, and is a valid event type.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `event_type` is not of data type string.

        Args:
            event_type(str) : Event type to be validated
            name (str, optional): The name of the `event_type` variable. Defaults to "Data".
        """

        DataValidationUtils.check_string_or_none(event_type, name)

    @staticmethod
    def check_timestamp(timestamp, name="Data"):
        """
        Check if `timestamp` is of type int and is a valid timestamp.
        A valid time stamp lies between [0, 2145916800000]. The maximum value corresponds
        to 1st Jan 2038 00:00:00 UTC.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `timestamp` is not of data type int.
            ValidationException (INVALID_TIME_EXCEPTION_CODE: 4004): Unix `timestamp` must be within range [0, 2145916800000].

        Args:
            timestamp (int): Unix timestamp to be evaluated.
            name (str, optional): The name of the `timestamp` variable. Defaults to "Data".
        """

        DataValidationUtils.check_int(timestamp, name)
        if ((timestamp < 0) or (timestamp > constants.MAXIMUM_UNIX_TIMESTAMP)):
            raise ValidationException(ValidationErrorMessage.INVALID_TIMESTAMP.format(name,
                                                                                      constants.MAXIMUM_UNIX_TIMESTAMP,
                                                                                      timestamp),
                                      ValidationErrorCode.INVALID_TIME_EXCEPTION_CODE)

    @staticmethod
    def check_ping_id(ping_id, name="ping_id"):
        """
        Check if `ping_id` is of type str and is not an empty string.

        Raises:
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `ping_id` is not of data type str.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `ping_id` is an empty string.

        Args:
            ping_id (float, None): Ping ID to be evaluated.
        """

        DataValidationUtils.check_string(ping_id, name)
        
        if len(ping_id) == 0:
            raise ValidationException(ValidationErrorMessage.INVALID_PING_ID_LENGTH,
                                        ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def check_dict_with_str_keys(data_dict, name="data_dict"):
        """
        Checks if `data_dict` is a dictionary and all keys in the `data_dict` are strings.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `data_dict` is not of data type dict.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If `data_dict` contains key of type other than string.

        Args:
            data_dict (dict): Data dict to be validated
            name (str, optional): The name of the `data_dict` variable. Defaults to "data_dict".
        """

        DataValidationUtils.check_dict(data_dict, name)

        # Iterate over each key in the dictionary
        for key in data_dict.keys():
            
            # Check if the key is not a string
            if not isinstance(key, str):
                raise ValidationException(ValidationErrorMessage.NON_STRING_KEYS_IN_DICT.format(name),
                                          ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    @staticmethod
    def validate_ping(ping_dict, name="ping"):
        """
        Check if ping represented by `ping_dict` is a dictionary, contains mandatory keys and all present keys are valid.

        Raises:
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the ping.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in the ping.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If metadata in ping contains a key of type other than string.

            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping is not of data type dict.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If latitude in ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If longitude in ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If timestamp in ping is not of data type int.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If error_radius in ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If event_type in ping is not of data type string.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If force_retain in ping is not of data type bool.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata in ping is not of data type dict.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id is not of data type str.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If error_radius in ping is negative or equal to zero.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If ping_id is an empty string.

            ValidationException (INVALID_TIME_EXCEPTION_CODE: 4004): If timestamp in ping is not an integer or not in range [0, 2145916800000].

            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If latitude in ping is not in range [-90 to 90].
            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If longitude in ping is not in range [-180 to 180].

        Args:
            ping_dict(dict) : Ping dict to be validated.
        """

        DataValidationUtils.check_empty_dict(ping_dict, name)

        # Check if all mandatory keys are present
        for key in constants.MANDATORY_KEYS_IN_A_PING:
            DataValidationUtils.check_key_in_a_dict(ping_dict, key)

        # Mapping keys to their respective validation functions
        validation_functions = {"latitude": lambda value: DataValidationUtils.check_latitude(value, name="latitude"),
                                "longitude": lambda value: DataValidationUtils.check_longitude(value, name="longitude"),
                                "timestamp": lambda value: DataValidationUtils.check_timestamp(value, name="timestamp"),
                                "ping_id": lambda value: DataValidationUtils.check_ping_id(value),
                                "error_radius": lambda value: DataValidationUtils.check_error_radius(value, name="error_radius"),
                                "event_type": lambda value: DataValidationUtils.check_event_type(value, name="event_type"),
                                "force_retain": lambda value: DataValidationUtils.check_bool(value, name="force_retain"),
                                "metadata": lambda value: DataValidationUtils.check_dict_with_str_keys(value, name="metadata")}

        # Iterate through the pings and validate them
        for key, value in ping_dict.items():
            if key in validation_functions:
                validation_functions[key](value)
            else:
                raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                          ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    @staticmethod
    def validate_all_pings(trace):
        """
        Check if ping represented by `ping_dict` is a dictionary, contains mandatory keys and all present keys are valid.

        Raises:
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from the ping.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in the ping.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If metadata in ping contains a key of type other than string.

            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping is not of data type dict.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If latitude in ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If longitude in ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If timestamp in ping is not of data type int.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If error_radius in ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If event_type in ping is not of data type string.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If force_retain in ping is not of data type bool.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata in ping is not of data type dict.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id is present in some pings but not all.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If error_radius in ping is negative or equal to zero.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If duplicate ping_ids are present.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If ping_id is an empty string.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If latitude and longitude all pings is None.

            ValidationException (INVALID_TIME_EXCEPTION_CODE: 4004): If timestamp in ping is not an integer or not in range [0, 2145916800000].

            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If latitude in ping is not in range [-90 to 90].
            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If longitude in ping is not in range [-180 to 180].

        Args:
            trace(list) : List of pings.
        """
        
        ping_ids = []
        at_least_one_ping_with_not_null_coord = False
        # Validate each ping
        for ping_dict in trace:
            DataValidationUtils.validate_ping(ping_dict)

            if "ping_id" in ping_dict:
                ping_ids.append(ping_dict["ping_id"])

            if isinstance(ping_dict["latitude"], (int, float)) and isinstance(ping_dict["latitude"], (int, float)):
                at_least_one_ping_with_not_null_coord = True

        if not at_least_one_ping_with_not_null_coord:
            raise ValidationException(ValidationErrorMessage.ALL_COORDS_NONE,
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

        # Check if ping_id is strictly present or strictly absent in all of the pings
        if len(ping_ids) == 0:
            return
        elif (len(ping_ids) != len(trace)):
            raise ValidationException(ValidationErrorMessage.AMBIGUOUS_PING_ID,
                                      ValidationErrorCode.DATA_FORMAT_EXCEPTION_CODE)

        # Check if all ping_ids are unique and have valid length
        if len(set(ping_ids)) != len(ping_ids):
            raise ValidationException(ValidationErrorMessage.FOUND_DUPLICATE_VALUES.format("ping_id"),
                                        ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def validate_trace_payload(trace_payload, name="trace payload"):
        """
        Check if a trace payload represented by `trace_payload` is a dictionary, contains mandatory keys and all present keys have valid values.

        Args:
            trace_payload(dict): Trace payload with pings list, vehicle type and vehicle speed information. Has following keys and values:
                trace: A list of dictionaries where each dictionary contains the metadata of a ping.
                    :Each ping dictionary can only have the following keys and values:
                    :ping_id (str, optional) : Unique identifier for the current ping.
                        Note: ping_id must be present in either all of the pings or user can choose to not pass it in any of the pings. We will automatically assign a unique string for each ping.
                    :latitude (int, float, None) : Latitude of the ping in decimal degrees format, must be within range [-90 to 90], or None.
                                                   Example: 27.562381
                    :longitude (int, float, None) : Longitude of the ping in decimal degrees format, must be within range [-180 to 180], or None.  
                                                    Example: 77.52531
                    :timestamp (int): Unix timestamp for the ping in milliseconds.
                    :error_radius (float, None, optional): GPS error radius of the ping in meters.
                    :event_type (str, None, optional): A string denoting the event which occurred at the ping.
                    :force_retain (bool, optional): Should be True if we do not want to drop the corresponding ping during cleaning.
                                                    If it is False then the current ping may or may not be retained.
                    :metadata (dict, optional): A dictionary to store metadata related to the ping. In this dictionary, keys are strings and values can be of any datatype.
                vehicle_type: A string denoting type of vehicle.
                vehicle_speed: A float denoting the average speed of the vehicle in kilometers per hour.

        Raises:
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If mandatory key is missing from the trace payload.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If a mandatory key is missing from a ping.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If an unexpected key is present in a ping.
            ValidationException (KEY_ERROR_EXCEPTION_CODE: 4001): If metadata in a ping contains key of type other than string.

            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If a ping is not of data type dict.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If latitude in a ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If longitude in a ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If timestamp in a ping is not of data type int.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If error_radius in a ping is not of data type int, float or None.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If event_type in a ping is not of data type string.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If force_retain in a ping is not of data type bool.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If metadata in a ping is not of data type dict.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If ping_id is present in some pings but not all.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If error_radius in ping is negative or equal to zero.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If duplicate ping_ids are present.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If ping_id is an empty string.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If latitude and longitude all pings is None.

            ValidationException (INVALID_TIME_EXCEPTION_CODE: 4004): If timestamp in a ping is not in range [0, 2145916800000].

            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If latitude in a ping is not in range [-90 to 90].
            ValidationException (INVALID_COORDS_EXCEPTION_CODE: 4005): If longitude in a ping is not in range [-180 to 180].
        """

        DataValidationUtils.check_empty_dict(trace_payload, name)

        # Check if all mandatory keys are present
        for key in constants.MANDATORY_KEYS_IN_TRACE_PAYLOAD:
            DataValidationUtils.check_key_in_a_dict(trace_payload, key)

        # Check if trace is a non empty list containing at least one point
        DataValidationUtils.check_empty_list(trace_payload["trace"], "trace")

        # Validate each ping
        DataValidationUtils.validate_all_pings(trace_payload.get("trace"))

        # Validate optional keys in trace payload
        trace_payload_keys = list(trace_payload.keys())
        for key in trace_payload_keys:
            if (key == "vehicle_type"):
                DataValidationUtils.check_string(trace_payload[key], name="vehicle_type")

            elif (key == "vehicle_speed"):
                DataValidationUtils.check_strictly_positive_int_or_float(
                    trace_payload[key], name="vehicle_speed")

            # Raise error if unexpected key is found
            elif key not in constants.MANDATORY_KEYS_IN_TRACE_PAYLOAD:
                raise ValidationException(ValidationErrorMessage.UNEXPECTED_KEYS_IN_DICT.format(name),
                                          ValidationErrorCode.KEY_ERROR_EXCEPTION_CODE)

    @staticmethod
    def validate_remove_nearby_pings_parameters(min_dist_bw_consecutive_pings):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `min_dist_bw_consecutive_pings` is not of data type int or float.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `min_dist_bw_consecutive_pings` is negative.
        """

        DataValidationUtils.check_non_negative_int_or_float(min_dist_bw_consecutive_pings, "min_dist_bw_consecutive_pings")

    @staticmethod
    def validate_impute_distorted_pings_with_distance_parameters(max_dist_ratio):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `max_dist_ratio` is not of data type int or float.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_dist_ratio` is less than 1.
        """

        DataValidationUtils.check_int_or_float(max_dist_ratio, "max_dist_ratio")
        
        if max_dist_ratio<1:
            raise ValidationException(ValidationErrorMessage.INVALID_MAX_DIST_RATIO_FOR_IMPUTATION.format(str(max_dist_ratio)),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)
    
    @staticmethod
    def validate_impute_distorted_pings_with_angle_parameters(max_delta_angle):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `max_delta_angle` is not of data type int or float.
            
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_delta_angle` is negative.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_delta_angle` is greater than 180 degrees.
        """

        DataValidationUtils.check_non_negative_int_or_float(max_delta_angle, "max_delta_angle")

        if max_delta_angle>180:
            raise ValidationException(ValidationErrorMessage.INVALID_MAX_DELTA_ANGLE_FOR_IMPUTATION.format(str(max_delta_angle)),
                                      ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def validate_map_match_trace_parameters(osrm_url,
                                            ping_batch_size,
                                            map_matching_radius, 
                                            avg_snap_distance,
                                            max_matched_dist_to_raw_dist_ratio):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `osrm_url` is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `ping_batch_size` is not of data type int.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `map_matching_radius` is not of data type int or float.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `avg_snap_distance`  is not of data type int or float.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `max_matched_dist_to_raw_dist_ratio` is not of data type int or float.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `ping_batch_size` is less than 2.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `map_matching_radius` is negative.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `avg_snap_distance` is negative.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_matched_dist_to_raw_dist_ratio` is negative.
        """

        # Validate parameters
        DataValidationUtils.check_string(osrm_url, "osrm_url")  
        DataValidationUtils.check_non_negative_int_or_float(map_matching_radius, "map_matching_radius")
        DataValidationUtils.check_non_negative_int_or_float(avg_snap_distance, "avg_snap_distance")
        DataValidationUtils.check_non_negative_int_or_float(max_matched_dist_to_raw_dist_ratio, "max_matched_dist_to_raw_dist_ratio")
        DataValidationUtils.check_int(ping_batch_size, "ping_batch_size")

        if ping_batch_size<2:
            raise ValidationException(ValidationErrorMessage.INVALID_PING_BATCH_SIZE_FOR_MAP_MATCHING, ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def validate_interpolate_trace_parameters(osrm_url,
                                              min_dist_from_prev_ping, 
                                              max_dist_from_prev_ping):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `osrm_url` is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `min_dist_from_prev_ping` is not of data type int or float.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `max_dist_from_prev_ping` is not of data type int or float.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `min_dist_from_prev_ping` is not strictly positive.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_dist_from_prev_ping` is not strictly positive.
        """

        # Validate parameters
        DataValidationUtils.check_string(osrm_url, "osrm_url")  
        DataValidationUtils.check_strictly_positive_int_or_float(min_dist_from_prev_ping, "min_dist_from_prev_ping")
        DataValidationUtils.check_strictly_positive_int_or_float(max_dist_from_prev_ping, "max_dist_from_prev_ping")

        # Check if min_dist_from_prev_ping is less than max_dist_from_prev_ping
        if not (min_dist_from_prev_ping < max_dist_from_prev_ping):
            raise ValidationException(ValidationErrorMessage.INVALID_DIST_THRESHOLDS_FOR_INTERPOLATION, ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def validate_add_stop_events_info_parameters(max_dist_bw_consecutive_pings,
                                                 max_dist_for_merging_stop_points,
                                                 min_size, 
                                                 min_staying_time):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `max_dist_bw_consecutive_pings` is not of data type int or float.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `max_dist_for_merging_stop_points` is not of data type int or float.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `min_size` is not of data type int.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `min_staying_time` is not of data type int.
            
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_dist_bw_consecutive_pings` is negative.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `max_dist_for_merging_stop_points` is negative.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `min_size` is less then 2.
            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `min_staying_time` is negative.
        """

        # Validate parameters
        DataValidationUtils.check_strictly_positive_int(min_staying_time, "min_staying_time")
        DataValidationUtils.check_strictly_positive_int_or_float(max_dist_bw_consecutive_pings, "max_dist_bw_consecutive_pings")
        DataValidationUtils.check_strictly_positive_int_or_float(max_dist_for_merging_stop_points, "max_dist_for_merging_stop_points")
        DataValidationUtils.check_int(min_size, "min_size")      

        if min_size<2:
            raise ValidationException(ValidationErrorMessage.INVALID_MIN_SIZE_FOR_STOP_EVENTS_DETECTION, ValidationErrorCode.VALUE_EXCEPTION_CODE)

    @staticmethod
    def validate_convert_csv_to_trace_payload_parameters(csv_file_path,
                                                         vehicle_type,
                                                         vehicle_speed,
                                                         force_retain_event_types):
        """
        Validate parameters.

        Raises:
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `csv_file_path` is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `vehicle_type` is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `vehicle_speed` is not of data type str.
            ValidationException (DATA_FORMAT_EXCEPTION_CODE: 4002): If `force_retain_event_types` is not of data type bool.

            ValidationException (VALUE_EXCEPTION_CODE: 4003): If `vehicle_speed` is zero or negative.
        """

        # Validate parameters
        DataValidationUtils.check_string(csv_file_path, "csv_file_path")
        DataValidationUtils.check_string(vehicle_type, "vehicle_type")
        DataValidationUtils.check_bool(force_retain_event_types, "force_retain_event_types")
        DataValidationUtils.check_strictly_positive_int_or_float(vehicle_speed, "vehicle_speed")
        