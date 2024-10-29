import os
import math
from typing import Union
import datetime
from haversine import haversine

from .. import constants


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
        # Create a timezone-aware datetime object in UTC
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
        time_ms (int): The time duration in seconds. Must be a non-negative integer.
        format (str, optional): Format of output time string. Defaults to "hms" and can have following values:
                                "hms" : include hour, minute, second in output time string.
                                "ms" : include minute, second in output time string.
                                "s" : include only second in output time string.

    Returns:
        str: A string representing the time duration in hours, minutes, and seconds. The format is "{hours} hours, {minutes} minutes, {seconds} seconds".
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
        bool: True if file_path already exists or if file_path was successfully created. Else False.
    """

    if (not is_filename(file_path)):
        if (not os.path.exists(file_path)):
            os.makedirs(os.path.dirname(file_path),
                        exist_ok=True)
