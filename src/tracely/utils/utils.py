import os
import math
import copy
from typing import Union
import datetime
import numpy as np
from haversine import haversine

from .. import constants

# vectorized_haversine uses the `haversine` PyPI package's kernel form
# (`2 * asin(sqrt(d))`, not the equivalent-but-numerically-different
# `2 * atan2(sqrt(d), sqrt(1 - d))` form) together with
# constants.AVG_EARTH_RADIUS_M, so it is bit-for-bit identical to
# get_haversine_distance after rounding.

_JSON_SCALAR_TYPES = (str, int, float, bool, type(None))


def get_haversine_distance(lat_1: Union[int, float],
                           lng_1: Union[int, float],
                           lat_2: Union[int, float],
                           lng_2: Union[int, float]) -> Union[float, None]:
    """
    Provides haversine distance between two geo locations.

    Args:
        lat_1 (int, float): latitude of first point, in decimal degrees.
        lng_1 (int, float): longitude of first point, in decimal degrees.
        lat_2 (int, float): latitude of second point, in decimal degrees.
        lng_2 (int, float): longitude of second point, in decimal degrees.

    Returns:
        Union[float, None]: Returns distance in meters between the given geo locations as a float. In case of any exception, returns None.
    """

    try:
        haversine_distance = haversine((lat_1, lng_1), 
                                       (lat_2, lng_2), 
                                       unit="m")
        
        return round(haversine_distance, 2)
    
    except Exception:
        return None


def vectorized_haversine(lat_1: np.ndarray,
                         lng_1: np.ndarray,
                         lat_2: np.ndarray,
                         lng_2: np.ndarray) -> np.ndarray:
    """
    Array equivalent of `get_haversine_distance`, for pairs of coordinate
    arrays. Uses the exact same formula and earth-radius constant as the
    `haversine` package's scalar kernel, so results are identical (after
    rounding to 2 decimals) to calling `get_haversine_distance` element-wise.
    NaN inputs propagate to NaN outputs (matching `get_haversine_distance`
    called with `float("nan")` arguments, which also returns `nan`, not `None`,
    since `haversine`'s range checks are false for NaN and never raise).

    Args:
        lat_1, lng_1, lat_2, lng_2 (numpy.ndarray): Arrays of coordinates in decimal degrees.

    Returns:
        numpy.ndarray: Distances in meters (rounded to 2 decimal places), dtype float64.
    """

    lat_1_r = np.radians(lat_1)
    lat_2_r = np.radians(lat_2)
    lat_diff = lat_2_r - lat_1_r
    lng_diff = np.radians(lng_2) - np.radians(lng_1)

    d = (np.sin(lat_diff * 0.5) ** 2
         + np.cos(lat_1_r) * np.cos(lat_2_r) * np.sin(lng_diff * 0.5) ** 2)

    distance = constants.AVG_EARTH_RADIUS_M * (2 * np.arcsin(np.sqrt(d)))
    return np.round(distance, 2)


def fast_deepcopy(obj):
    """
    Behaviorally equivalent to `copy.deepcopy(obj)` for the plain
    dict/list/scalar ("JSON-like") structures tracely's input/output
    payloads are made of, but without `copy.deepcopy`'s generic
    type-dispatch and memo-dict bookkeeping. Falls back to real
    `copy.deepcopy` for any value that isn't a dict, list, or JSON scalar
    (str/int/float/bool/None) — e.g. a ping's `metadata` value containing
    some other object type — so it is never less correct than
    `copy.deepcopy`, only faster for the common case.

    Note: unlike `copy.deepcopy`, this does not preserve shared-object
    identity between two references to the same nested dict/list inside
    `obj` (each occurrence is copied independently). This has no effect on
    any computed output or any JSON/dict-equality comparison, only on
    `is`-identity of the copy's internals, which no part of tracely or its
    public API relies on.

    Args:
        obj: A dict, list, or any other (possibly nested) Python object.

    Returns:
        A deep copy of `obj`.
    """

    obj_type = type(obj)
    if obj_type is dict:
        return {key: fast_deepcopy(value) for key, value in obj.items()}
    if obj_type is list:
        return [fast_deepcopy(value) for value in obj]
    if obj_type in _JSON_SCALAR_TYPES:
        return obj
    return copy.deepcopy(obj)


def fast_copy_trace_payload(payload):
    """
    Behaviorally equivalent to `fast_deepcopy(payload)` for a trace
    payload specifically (`{"trace": [ping, ...], "vehicle_type": ...,
    "vehicle_speed": ...}`, each ping a flat dict of scalar fields plus an
    optional nested "metadata" dict/value), but much faster: every ping
    field except "metadata" is a plain int/float/str/bool/None, which can
    never be mutated in place in Python (only reassigned to a different
    value) -- so a shallow copy of each ping dict already fully protects
    every one of those fields from later mutation of the original payload
    or any of its ping dicts. Only "metadata" (the one field whose value
    is itself a mutable object) needs real, recursive protection.

    Falls back to `fast_deepcopy` for a ping's `metadata` value exactly
    as `fast_deepcopy` itself would, so protection is never weaker, only
    faster for the common (flat, scalar-only ping) case.

    Args:
        payload (dict): A trace payload, as passed to `CleanTrace.__init__`
            or `CreateTraceData.__init__`.

    Returns:
        dict: An independent copy of `payload`, safe from later mutation
        of the original payload (including each ping's `metadata`, if any).
    """

    # This runs before `payload` has been validated -- it must tolerate
    # any malformed shape (not a dict, no "trace" key, "trace" not a list
    # of dicts, ...) by falling back to the fully generic fast_deepcopy,
    # so that validation (which runs right after) still gets a chance to
    # raise its own specific, correct exception instead of this function
    # crashing first with an unrelated KeyError/TypeError/AttributeError.
    if type(payload) is not dict or not isinstance(payload.get("trace"), list):
        return fast_deepcopy(payload)

    new_payload = dict(payload)
    new_trace = []
    for ping_dict in payload["trace"]:
        if type(ping_dict) is not dict:
            new_trace.append(fast_deepcopy(ping_dict))
            continue
        new_ping_dict = dict(ping_dict)
        metadata = new_ping_dict.get("metadata")
        if metadata is not None:
            new_ping_dict["metadata"] = fast_deepcopy(metadata)
        new_trace.append(new_ping_dict)
    new_payload["trace"] = new_trace
    return new_payload


def calculate_trace_distance(trace):
    """
    This function gives the total distance calculated from a trace, using haversine method.

    Args:
        trace (list): List of geo locations where each geo locations is in format [latitude, longitude]

    Returns :
        trace_distance (float): Total distance of the given trace.
    """

    trace_distance = 0
    for i in range(len(trace) - 1):
        lat1, lng1, lat2, lng2 = trace[i][0], trace[i][1], trace[i+1][0], trace[i+1][1]
        trace_distance += get_haversine_distance(lat1,
                                                 lng1, 
                                                 lat2,
                                                 lng2)
    return trace_distance


def calculate_initial_compass_bearing(lat_1: Union[int, float],
                                      lng_1: Union[int, float],
                                      lat_2: Union[int, float],
                                      lng_2: Union[int, float]) -> Union[float, None]:
    """
    Provides compass bearing angle between two geo locations.

    Args:
        lat_1 (Union[int, float]): latitude of first point, in decimal degrees.
        lng_1 (Union[int, float]): longitude of first point, in decimal degrees.
        lat_2 (Union[int, float]): latitude of second point, in decimal degrees.
        lng_2 (Union[int, float]): longitude of second point, in decimal degrees.

    Returns:
        Union[float, None]: Returns compass bearing angle between the given geo locations as a float. In case of any exception, returns None.
    """

    try:
        delta_lng = math.radians(lng_2 - lng_1)
        lat_1 = math.radians(lat_1)
        lat_2 = math.radians(lat_2)

        x = math.sin(delta_lng) * math.cos(lat_2)
        y = math.cos(lat_1) * math.sin(lat_2) - (math.sin(lat_1) * math.cos(lat_2) * math.cos(delta_lng))

        initial_bearing = math.atan2(x, y)
        initial_bearing = math.degrees(initial_bearing)
        bearing = (initial_bearing + 360) % 360  # Normalize to 0-360 degrees

        return bearing
    except Exception:
        return None


def vectorized_change_in_direction(prev_lat: np.ndarray,
                                   prev_lng: np.ndarray,
                                   curr_lat: np.ndarray,
                                   curr_lng: np.ndarray,
                                   next_lat: np.ndarray,
                                   next_lng: np.ndarray) -> np.ndarray:
    """
    Array equivalent of `calculate_change_in_direction(P1, P2, P3)` for
    arrays of (P1, P2, P3) triples, where P1=(prev_lat, prev_lng),
    P2=(curr_lat, curr_lng), P3=(next_lat, next_lng). Uses the exact same
    bearing formula (`atan2`-based, matching `calculate_initial_compass_bearing`)
    so results are identical to calling `calculate_change_in_direction`
    element-wise.

    Args:
        prev_lat, prev_lng, curr_lat, curr_lng, next_lat, next_lng (numpy.ndarray):
            Arrays of coordinates in decimal degrees.

    Returns:
        numpy.ndarray: Change in direction in degrees (0-180), dtype float64.
    """

    def bearing(lat_1, lng_1, lat_2, lng_2):
        """
        Array equivalent of `calculate_initial_compass_bearing`, for
        arrays of coordinate pairs. Uses the exact same `atan2`-based
        formula, so results are identical to calling
        `calculate_initial_compass_bearing` element-wise.

        Args:
            lat_1, lng_1, lat_2, lng_2 (numpy.ndarray): Arrays of coordinates in decimal degrees.

        Returns:
            numpy.ndarray: Compass bearing in degrees (0-360), dtype float64.
        """

        delta_lng = np.radians(lng_2 - lng_1)
        lat_1_r = np.radians(lat_1)
        lat_2_r = np.radians(lat_2)

        x = np.sin(delta_lng) * np.cos(lat_2_r)
        y = np.cos(lat_1_r) * np.sin(lat_2_r) - (np.sin(lat_1_r) * np.cos(lat_2_r) * np.cos(delta_lng))

        initial_bearing = np.degrees(np.arctan2(x, y))
        return (initial_bearing + 360) % 360

    bearing_curr_prev = bearing(curr_lat, curr_lng, prev_lat, prev_lng)
    bearing_curr_next = bearing(curr_lat, curr_lng, next_lat, next_lng)

    angle = (bearing_curr_next - bearing_curr_prev + 360) % 360
    return np.where(angle < 180, 180 - angle, angle - 180)


def calculate_change_in_direction(P1: tuple,
                                  P2: tuple, 
                                  P3: tuple) -> Union[float, None]:
    """
    Provides change in direction between three geographic points, in range 0-180 degrees, independent of clockwise or anti-clockwise change in direction among the points.

    Args:
        P1 (tuple): Tuple of latitude and longitude(both in decimal degrees) of first point in format (latitude, longitude).
        P2 (tuple): Tuple of latitude and longitude(both in decimal degrees) of second point in format (latitude, longitude).
        P3 (tuple): Tuple of latitude and longitude(both in decimal degrees) of third point in format (latitude, longitude).

    Returns:
        Union[float, None]: Returns change in direction in degrees as a float. In case of any exception, returns None.
    """

    try:
        # Calculate bearings from P2 to P1 and from P2 to P3
        bearing_P2_P1 = calculate_initial_compass_bearing(
            P2[0], P2[1], P1[0], P1[1])
        bearing_P2_P3 = calculate_initial_compass_bearing(
            P2[0], P2[1], P3[0], P3[1])

        # Calculate the angle at point P2
        angle = (bearing_P2_P3 - bearing_P2_P1 + 360) % 360
        return (180 - angle) if angle < 180 else (angle - 180)
    except Exception:
        return None


def convert_unix_timestamp_to_human_readable(timestamp: int) -> Union[str, None]:
    """Converts unix timestamp in seconds to human readable format as a string.

    Args:
        timestamp (int): Unix timestamp in seconds

    Returns:
        Union[str, None]: Returns timestamp as a string of YYYY-MM-DD HH:MM:SS format. In case of any exception, return None.
    """

    try:
        # Create a timezone-aware datetime object in IST (constants.timezone_offset)
        dt = datetime.datetime.fromtimestamp(timestamp,
                                             datetime.timezone(constants.timezone_offset))
        
        # Format the datetime object as a string
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def convert_time_interval_to_human_readable(time: int, 
                                            format="hms") -> str:
    """
    Converts a time duration given in seconds into a human-readable format displaying hours, minutes, and seconds.

    Args:
        time (int): The time duration in seconds. Must be a non-negative integer.
        format (str, optional): Format of output time string. Defaults to "hms" and can have following values:
                                "hms" : include hour, minute, second in output time string.
                                "ms" : include minute, second in output time string.
                                "s" : include only second in output time string.

    Returns:
        str | None: The time duration formatted per `format` — "{hours} hours, {minutes} minutes and {seconds} seconds" for "hms",
             "{minutes} minutes and {seconds} seconds" for "ms", or "{seconds} seconds" for "s". `None` if an exception occurs
             (e.g. `time`/`format` of an unexpected type).
    """

    try:
        if format == "hms":
            # Calculate hours, minutes, and seconds
            hours = time // (60 * 60)
            time %= (60 * 60)
            minutes = time // (60)
            time %= (60)
            seconds = time

            # Format the result
            return f"{int(hours)} hours, {int(minutes)} minutes and {int(seconds)} seconds"

        elif format == "ms":
            # Calculate minutes, and seconds
            minutes = time // (60)
            time %= (60)
            seconds = time

            # Format the result
            return f"{int(minutes)} minutes and {int(seconds)} seconds"

        elif format == "s":
            # Calculate seconds
            seconds = time

            # Format the result
            return f"{int(seconds)} seconds"
    except Exception:
        return None


def is_filename(input_path):
    """
    Check if the input_path is a filename.

    Args:
        input_path (string): Input path.

    Returns
        bool: True if input_path is a filename else False.
    """

    return os.path.isfile(input_path)


def create_path(file_path):
    """
    Creates all required directories for the input path "file_path".

    Args:
        file_path (string): A file path.

    Returns:
        None. Creates the directories as a side effect; does not report success/failure.
    """

    if (not is_filename(file_path)):
        if (not os.path.exists(file_path)):
            os.makedirs(os.path.dirname(file_path),
                        exist_ok=True)
