import pandas as pd
import numpy as np

from .. import constants
from ..exceptions.custom_exceptions import InputOutputException
from ..exceptions.error_messages import IOErrorMessage, IOErrorCode
from .data_validation_utils import DataValidationUtils


def convert_csv_to_trace_payload(csv_file_path: str,
                                 vehicle_type = constants.DEFAULT_VEHICLE_TYPE,
                                 vehicle_speed = constants.DEFAULT_VEHICLE_SPEED,
                                 force_retain_event_types = True) -> dict:
    """
    Provides trace payload as a dictionary. Created from the CSV file provided at path `csv_file_path`.

    Args:
        csv_file_path (str): Path to the CSV file containing trace data.
        vehicle_type (str, optional): The type of the vehicle used. Defaults to constants.DEFAULT_VEHICLE_TYPE.
        force_retain_event_types (bool, optional): Indicates whether to update force_retain column flag(if present) on the basis of event_type column (if present). Defaults to True.

    Returns:
        Trace payload dictionary with following keys and values:
            :trace: Contains list of pings. Here, each ping will contain mandatory fields required in trace and optional fields provided in the ping.
            :vehicle_type: Denotes the type of the vehicle used.

    Raises:
        All exceptions raised by the following functions:
            "DataValidationUtils.validate_convert_csv_to_trace_payload_parameters" present in data_validation_utils.py.
            "DataValidationUtils.validate_trace_payload" present in data_validation_utils.py.
    """

    # Validate parameters
    DataValidationUtils.validate_convert_csv_to_trace_payload_parameters(csv_file_path=csv_file_path,
                                                                         vehicle_type=vehicle_type,
                                                                         vehicle_speed=vehicle_speed,
                                                                         force_retain_event_types=force_retain_event_types)
    
    # Read the csv file using pandas
    try:
        df = pd.read_csv(csv_file_path)
    except Exception:
        raise InputOutputException(IOErrorMessage.UNABLE_TO_READ_CSV_FILE,
                                   IOErrorCode.FILE_READ_ERROR_CODE)

    # Check if all mandatory columns are present in the CSV file
    for col in constants.MANDATORY_KEYS_IN_A_PING:
        if col not in df.columns:
            raise InputOutputException(IOErrorMessage.MANDATORY_COLUMN_MISSING_FROM_CSV.format(col),
                                       IOErrorCode.MANDATORY_COLUMN_ERROR_CODE)

    # If 'metadata' col already exists then rename it
    df = df.rename(columns={'metadata': 'tracely_internal_old_metadata_col'})

    optional_columns_present_in_csv = list(set(df.columns).intersection(set(list(constants.OPTIONAL_ITEMS_IN_A_PING.keys()))))

    for optional_column, default_value in constants.OPTIONAL_ITEMS_IN_A_PING.items():
        if optional_column in df.columns:
            # If the column is of object type (non-numerical data)
            if df[optional_column].dtype == 'object':
                if default_value is not None:
                    df[optional_column] = df[optional_column].fillna(default_value)
                # If default_value is None, no need to fill NaNs
            else:
                # For numerical columns, use replace to handle NaN -> default_value
                df[optional_column] = df[optional_column].replace(np.nan, default_value)

    df = df.where(df.notna(), None)

    # Get list of columns that contain metadata
    metadata_columns = list(set(df.columns).difference(set(constants.MANDATORY_KEYS_IN_A_PING).
                                            union(set(list(constants.OPTIONAL_ITEMS_IN_A_PING.keys())))))

    trace = []

    # Set update_force_retain_flag flag
    if (("event_type" in df.columns) and
       ("force_retain" not in df.columns) and
       (force_retain_event_types is True)):
        update_force_retain_flag = True
    else:
        update_force_retain_flag = False

    # Create trace from individual pings
    for row in df.to_dict("records"):

        # Add mandatory keys
        ping_dict = {col: (None if pd.isna(row[col]) else row[col]) for col in constants.MANDATORY_KEYS_IN_A_PING}
        ping_dict["metadata"] = {}

        # Add optional columns
        ping_dict.update({col: row[col] for col in optional_columns_present_in_csv})

        # Add force retain
        if (update_force_retain_flag):
            event_type = row["event_type"]
            if ((isinstance(event_type, str)) and (len(event_type) > 0)):
                ping_dict["force_retain"] = True
            else:
                ping_dict["force_retain"] = False

        # Add metadata
        for col in metadata_columns:
            if(col == "tracely_internal_old_metadata_col"):
                ping_dict["metadata"]["metadata"] = row[col]
            else:
                ping_dict["metadata"][col] = row[col]

        # Add row data to trace list
        trace.append(ping_dict)

    # Create trace payload by combining trace, vehicle_type and vehicle_speed
    trace_payload = {
        "trace": trace,
        "vehicle_type": vehicle_type,
        "vehicle_speed": vehicle_speed}

    DataValidationUtils.validate_trace_payload(trace_payload) 

    return trace_payload
