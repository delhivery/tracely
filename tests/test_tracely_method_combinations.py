import itertools

from src.tracely.clean_trace import CleanTrace    
from tests.testing_utils import load_trace_payload


# Function to test run combinations of trace cleaning methods
def test_tracely_method_combinations():
    methods = ["remove_nearby_pings", "impute_distorted_pings_with_distance", "impute_distorted_pings_with_angle", "map_match_trace", "interpolate_trace", "add_stop_events_info"]
    
    # Loop through combinations of methods
    for i in range(1, 4):  # i = number of methods to apply (from 1 to 3)
        for combination in itertools.permutations(methods, i):
            print(f"Testing combination: {combination}")

            # Load the payload and create an object of CleanTrace
            payload = load_trace_payload("dummy_trace_input_payload")
            payload["trace"] = payload["trace"][:100]
            trace_data_obj = CleanTrace(payload)

            # Call the methods in the specified order
            for method_name in combination:
                getattr(trace_data_obj, method_name)()  # Call methods

            trace_cleaning_output = trace_data_obj.get_trace_cleaning_output()

