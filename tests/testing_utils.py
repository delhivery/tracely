import json

from src.tracely import constants


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
