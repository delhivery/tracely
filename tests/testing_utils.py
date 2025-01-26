import os
import json
import random
import time

from src.tracely import constants


def generate_dummy_trace(num_points):
    """
    Generate a trace of coordinates starting from a random point.

    Args:
        num_points (int): Number of coordinate points to generate.

    Returns:
        list: List of coordinates with [latitude, longitude, timestamp].
    """
    
    coordinates = []
    current_time = int(time.time() * 1000)  # Get current timestamp in milliseconds

    # Start with a random latitude and longitude
    latitude = round(random.uniform(-90.0, 90.0), 6)
    longitude = round(random.uniform(-180.0, 180.0), 6)

    for _ in range(num_points):
        coordinates.append([latitude, longitude, current_time])

        # Increment latitude and longitude slightly for the next point
        latitude = latitude + 0.001
        longitude = longitude + 0.001

        # Increment timestamp by 1 second (1000 milliseconds)
        current_time += 1000

    return coordinates


def dump_payload_to_json(payload: dict, json_file_path) -> None:
    """
    Dumps `payload` to JSON file in write mode at input file path in `json_file_path` argument.

    Args:
        payload (dict): Payload tp be dumped in JSON file
        json_file_path (str): File path where `payload` is to be dumped.
    """

    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, indent=4)


def load_trace_payload(payload_name):
    """
    Load saved trace payload saved as a JSON file.

    Args:
        payload_name (dict): Name of payload JSON file.
    """

    test_payloads_base_path = constants.TEST_PAYLOADS_BASE_PATH
    payload_path = test_payloads_base_path + payload_name + ".json"
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload


def load_calculate_trace_similarity_payloads(payload_name):
    """
    Load saved traces for similarity calculation saved as a JSON file.

    Args:
        payload_name (dict): Name of payload JSON file.
    """

    trace_sim_cal_payloads_path = os.path.join(constants.TEST_PAYLOADS_BASE_PATH,
                                        "similarity_calculation_payloads", "")
    payload_path = trace_sim_cal_payloads_path + payload_name + ".json"

    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload