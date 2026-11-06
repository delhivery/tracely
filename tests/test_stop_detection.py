from src.tracely.clean_trace import CleanTrace
from tests.testing_utils import load_trace_payload


# Golden values captured from the real `infostop`-backed implementation,
# before it was removed -- see docs/remove_infostop_dependency/02_implementation_plan.md
# for how and why. Any future change to stop-detection logic must keep
# these exact values, or update them with an explicit, reviewed reason.
EXPECTED_STOP_EVENTS_INFO = [
    {
        "stop_event_sequence_number": 1,
        "start_time": "2023-11-15 03:43:20",
        "end_time": "2023-11-15 03:46:50",
        "total_stop_event_time": "0 hours, 3 minutes and 30 seconds",
        "number_of_pings": 15,
    },
    {
        "stop_event_sequence_number": 2,
        "start_time": "2023-11-15 03:48:45",
        "end_time": "2023-11-15 03:50:57",
        "total_stop_event_time": "0 hours, 2 minutes and 12 seconds",
        "number_of_pings": 12,
    },
]


def test_add_stop_events_info_detects_real_stop_events():
    """Test add_stop_events_info produces the expected stop events on a trace that has real stops."""

    payload = load_trace_payload("stop_event_fixture_payload")
    trace_data_obj = CleanTrace(payload)
    trace_data_obj.add_stop_events_info(max_dist_bw_consecutive_pings=10,
                                        max_dist_for_merging_stop_points=0.001,
                                        min_size=2,
                                        min_staying_time=120)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    stop_events_info = clean_output["stop_summary"]["stop_events_info"]
    assert len(stop_events_info) == len(EXPECTED_STOP_EVENTS_INFO)

    for actual, expected in zip(stop_events_info, EXPECTED_STOP_EVENTS_INFO):
        assert actual["stop_event_sequence_number"] == expected["stop_event_sequence_number"]
        assert actual["start_time"] == expected["start_time"]
        assert actual["end_time"] == expected["end_time"]
        assert actual["total_stop_event_time"] == expected["total_stop_event_time"]
        assert actual["number_of_pings"] == expected["number_of_pings"]

    stopping_pings = [p for p in clean_output["cleaned_trace"] if p["stop_event_status"]]
    assert len(stopping_pings) == 15 + 12


def test_add_stop_events_info_no_stop_found_returns_defaults():
    """Test add_stop_events_info falls back to all-default stop fields when no stop qualifies."""

    payload = load_trace_payload("dummy_trace_input_payload")
    trace_data_obj = CleanTrace(payload)
    # min_staying_time far larger than this fixture's trace duration -- guaranteed no stop qualifies.
    trace_data_obj.add_stop_events_info(min_staying_time=10 ** 9)
    clean_output = trace_data_obj.get_trace_cleaning_output()

    for ping in clean_output["cleaned_trace"]:
        assert ping["stop_event_status"] == False
        assert ping["stop_event_sequence_number"] == -1
