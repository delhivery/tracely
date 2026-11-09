import pandas as pd
from dataclasses import dataclass, field
from typing import Union, Dict, Any

from . import constants
from .utils.data_validation_utils import DataValidationUtils
from .utils.utils import fast_deepcopy


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


_OPTIONAL_PING_ITEMS = list(constants.OPTIONAL_ITEMS_IN_A_PING.items())


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

        # Builds an independent working copy and fills in missing optional
        # defaults in the SAME pass, instead of a full generic deep copy
        # followed by a second full pass over every ping to fill defaults.
        # A shallow copy of each ping dict is enough protection for every
        # field except "metadata" (the only field whose value is itself a
        # mutable object a caller could change after the fact) -- every
        # other field is a plain int/float/str/bool/None, which can never
        # be mutated in place, so a shallow copy already fully protects it.
        filled_input_payload = dict(self.input_payload)

        for key, default_value in constants.OPTIONAL_ITEMS_IN_A_TRACE_PAYLOAD.items():
            if key not in filled_input_payload:
                filled_input_payload[key] = default_value

        filled_trace = []
        for ping_dict in self.input_payload["trace"]:
            new_ping_dict = dict(ping_dict)
            for key, default_value in _OPTIONAL_PING_ITEMS:
                if key not in new_ping_dict:
                    new_ping_dict[key] = default_value

            metadata = new_ping_dict["metadata"]
            if metadata is not None:
                new_ping_dict["metadata"] = fast_deepcopy(metadata)

            filled_trace.append(new_ping_dict)

        filled_input_payload["trace"] = filled_trace

        return filled_input_payload

    def _create_ping_objects_and_dataframe(self,
                                           filled_input_payload: dict) -> tuple:
        """
        Create `Ping` objects AND the pings DataFrame together, from pings
        provided as dictionaries in the valid, filled input payload --
        combined into one pass over the (sorted) pings instead of two
        (build every `Ping` object, then separately convert every `Ping`
        object's `__dict__` back into a plain dict just to build the
        DataFrame from that list of dicts).

        Args:
            filled_input_payload (dict): Trace payload with filled missing optional values.

        Returns:
            tuple: (ping_objects, trace_df) -- same shapes/values as the
            previous two-step implementation produced.
        """

        # Sort the list in place based on the "timestamp" key. This mutates
        # filled_input_payload["trace"], which is already our own working
        # copy (built in _fill_missing_optional_values_in_input_payload),
        # never the caller's original list -- same as before.
        filled_input_payload["trace"].sort(key=lambda x: x["timestamp"])

        ping_objects = []
        ping_ids, timestamps = [], []
        latitudes, longitudes = [], []
        event_types, force_retains, error_radii = [], [], []

        for i, ping_dict in enumerate(filled_input_payload["trace"], 1):
            ping_id = ping_dict["ping_id"]
            if ping_id is None:
                ping_id = str(i)
                ping_dict["ping_id"] = ping_id

            timestamp = ping_dict["timestamp"]
            latitude = ping_dict["latitude"]
            longitude = ping_dict["longitude"]
            event_type = ping_dict["event_type"]
            force_retain = ping_dict["force_retain"]
            error_radius = ping_dict["error_radius"]

            ping_objects.append(Ping(ping_id=ping_id,
                                     timestamp=timestamp,
                                     latitude=latitude,
                                     longitude=longitude,
                                     event_type=event_type,
                                     force_retain=force_retain,
                                     error_radius=error_radius,
                                     metadata=ping_dict["metadata"]))

            ping_ids.append(ping_id)
            timestamps.append(timestamp)
            latitudes.append(latitude)
            longitudes.append(longitude)
            event_types.append(event_type)
            force_retains.append(force_retain)
            error_radii.append(error_radius)

        # Builds trace_df directly from the column lists collected above,
        # in the same column order _create_dataframe_from_ping_objects used
        # to produce (ping_id, timestamp, input_latitude, input_longitude,
        # event_type, force_retain, error_radius), instead of going through
        # pd.DataFrame(list_of_dicts) built from Ping.__dict__.
        # Every column -- core + cleaned_latitude/longitude + all default-value
        # columns -- built in ONE pd.DataFrame() call, instead of a 7-column
        # frame followed by 13 separate `trace_df[key] = ...` column inserts.
        # Each of those inserts used to be its own expensive block-manager
        # operation (verified via cProfile: ~13 `managers.insert()` calls per
        # object, the single biggest remaining cost in `__init__`); a dict
        # with every column already has everything pandas needs to lay out
        # the whole block manager once. `list(latitudes)`/`list(longitudes)`
        # (not the same list object passed twice under 2 keys) keep
        # cleaned_latitude/longitude genuinely independent column data from
        # input_latitude/longitude, same as the old `.copy()` did -- pandas
        # would likely still build independent arrays per dict key either
        # way, but this makes that independence explicit rather than implicit.
        all_columns = {
            "ping_id": ping_ids,
            "timestamp": timestamps,
            "input_latitude": latitudes,
            "input_longitude": longitudes,
            "event_type": event_types,
            "force_retain": force_retains,
            "error_radius": error_radii,
            "cleaned_latitude": list(latitudes),
            "cleaned_longitude": list(longitudes),
        }
        all_columns.update(constants.DEFAULT_VALUES_FOR_ADDED_KEYS_IN_CLEANED_TRACE)
        trace_df = pd.DataFrame(all_columns)

        return ping_objects, trace_df

    def create_trace_data(self) -> dict:
        """
        Create filled input payload of pings from trace payload dictionary. Trace payload is expected to contain at least trace key and valid trace data as its value.

        Returns:
            filled_input_payload (dict): The input trace payload (`trace`, `vehicle_type`, `vehicle_speed`) with its optional values filled in, plus two added keys:
                the list of Ping objects (`ping_objects`) and the pings dataframe (`trace_df`).

        Raises:
            ValidationException: Possible scenarios have been described in the documentation of validate_trace_payload function.
        """

        # Validate trace payload
        DataValidationUtils.validate_trace_payload(self.input_payload)

        # Fill missing optional data
        filled_input_payload = self._fill_missing_optional_values_in_input_payload()

        # Create list of Ping objects and the pings dataframe together
        ping_objects, trace_df = self._create_ping_objects_and_dataframe(filled_input_payload)

        # Assign created data to respective keys
        filled_input_payload["ping_objects"] = ping_objects
        filled_input_payload["trace_df"] = trace_df

        return filled_input_payload
