import copy
import pandas as pd
from dataclasses import dataclass, field
from typing import Union, Dict, Any

from . import constants
from .utils.data_validation_utils import DataValidationUtils


@dataclass
class Ping:
    # Mandatory elements
    ping_id: str
    timestamp: int
    latitude: Union[int, float, None]
    longitude: Union[int, float, None]

    # Optional elements
    event_type: Union[str, None] = None
    force_retain: bool = False
    error_radius: Union[int, None] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class CreateTraceData:
    def __init__(self,
                 payload: dict):
        self.input_payload = payload

    def _fill_missing_optional_values_in_input_payload(self) -> dict:
        """
        Fill missing optional fields and their default values within trace data in input payload.

        Returns:
            filled_input_payload (dict): Input payload with filled missing optional values.
        """

        # Create deep copy of input payload
        filled_input_payload = copy.deepcopy(self.input_payload)

        # Fill optional data in trace payload keys
        trace_payload_keys = list(filled_input_payload.keys())
        for key, default_value in constants.OPTIONAL_ITEMS_IN_A_TRACE_PAYLOAD.items():
            if (key not in trace_payload_keys):
                filled_input_payload[key] = default_value

        # Fill optional data in each trace ping dictionary
        for ping_dict in filled_input_payload["trace"]:
            ping_keys = list(ping_dict.keys())
            for key, default_value in constants.OPTIONAL_ITEMS_IN_A_PING.items():
                if (key not in ping_keys):
                    ping_dict[key] = default_value

        return filled_input_payload

    def _create_ping_objects(self,
                             filled_input_payload: dict) -> list:
        """
        Create ping objects from pings provided as dictionaries in the valid input payload.

        Args:
            filled_input_payload (dict): Trace payload with filled missing optional values.

        Returns:
            ping_objects (list): A list containing `Ping` objects, created from pings provided as dictionaries in the valid input payload
        """

        ping_objects = []

        # Sort the list in place based on the "timestamp" key
        filled_input_payload["trace"].sort(key=lambda x: x["timestamp"])

        # Iterate through filled input payload and create Ping objects
        for i, ping_dict in enumerate(filled_input_payload["trace"], 1):
            
            if(ping_dict["ping_id"] is None):
                ping_dict["ping_id"] = str(i)

            ping_object = Ping(ping_id=ping_dict["ping_id"],
                               timestamp=ping_dict["timestamp"],
                               latitude=ping_dict["latitude"],
                               longitude=ping_dict["longitude"],
                               event_type=ping_dict["event_type"],
                               force_retain=ping_dict["force_retain"],
                               error_radius=ping_dict["error_radius"],
                               metadata=ping_dict["metadata"]
                                )
            ping_objects.append(ping_object)

        return ping_objects

    def _create_dataframe_from_ping_objects(self, 
                                            ping_objects: list) -> pd.DataFrame:
        """
        Create dataframe of pings from Ping objects provided in a list. DataFrame does not contain column corresponding to metadata in Pings.

        Args:
            ping_objects (list): List of Ping objects.

        Returns:
            trace_df (pandas.DataFrame): A pandas dataframe containing pings, created from Pings provided as dictionaries in the valid ping objects list.
        """

        # Create dataframe from list of Ping objects
        trace_df = pd.DataFrame([{key: value for key, value in ping.__dict__.items() if key != "metadata"} for ping in ping_objects])

        # Rename latitude, longitude columns and add cleaned_latitude, cleaned_longitude keys.
        trace_df = trace_df.rename(columns={"latitude": "input_latitude",
                                            "longitude": "input_longitude"})

        trace_df["cleaned_latitude"] = copy.deepcopy(trace_df["input_latitude"])
        trace_df["cleaned_longitude"] = copy.deepcopy(trace_df["input_longitude"])

        # Add additional keys in the dataframe
        for key, value in constants.DEFAULT_VALUES_FOR_ADDED_KEYS_IN_CLEANED_TRACE.items():
            trace_df[key] = value

        return trace_df

    def create_trace_data(self) -> dict:
        """
        Create filled input payload of pings from trace payload dictionary. Trace payload is expected to contain at least trace key and valid trace data as its value.

        Returns:
            filled_input_payload (dict): A dictionary contain the list of Ping objects (corresponding to `ping_objects` key), and pings dataframe (corresponding to trace_df key)

        Raises:
            ValidationException: Possible scenarios have been described in the documentation of validate_trace_payload function.
        """
        
        # Validate trace payload
        DataValidationUtils.validate_trace_payload(self.input_payload)

        # Fill missing optional data
        filled_input_payload = self._fill_missing_optional_values_in_input_payload()

        # Create list of Ping objects
        ping_objects = self._create_ping_objects(filled_input_payload)

        # Create pings dataframe from the list of Ping objects
        trace_df = self._create_dataframe_from_ping_objects(ping_objects)

        # Assign created data to respective keys
        filled_input_payload["ping_objects"] = ping_objects
        filled_input_payload["trace_df"] = trace_df

        return filled_input_payload
