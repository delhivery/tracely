import json

from tests.testing_utils import load_trace_payload
from src.tracely.clean_trace import CleanTrace
from src.tracely import constants
from src.tracely.utils.utils import create_path


# Example usage of CleanTrace for cleaning trace
if __name__ == "__main__":

    # Load existing payload
    input_trace_payload = load_trace_payload("dummy_trace_input_payload")

    # Initialize the CleanTrace class with the input payload
    clean_trace_obj = CleanTrace(input_trace_payload)

    # Get raw trace without any cleaning
    raw_trace_output = clean_trace_obj.get_trace_cleaning_output()
    raw_trace = raw_trace_output['cleaned_trace']

    # Remove nearby pings that are too close to each other
    clean_trace_obj.remove_nearby_pings(min_dist_bw_consecutive_pings=10)

    # Impute distorted pings using distance and angle criteria
    clean_trace_obj.impute_distorted_pings_with_distance(max_dist_ratio=3)
    clean_trace_obj.impute_distorted_pings_with_angle(max_delta_angle=120)

    # Perform map matching (use only if an OSRM server is running)
    # clean_trace_obj.map_match_trace(
    #     osrm_url="http://127.0.0.1:5000/match/v1/driving/",
    #     ping_batch_size=5,
    #     map_matching_radius=20,
    #     avg_snap_distance=12,
    #     max_matched_dist_to_raw_dist_ratio=1.3)

    # Interpolate pings (use only if an OSRM server is running)
    # clean_trace_obj.interpolate_trace(
    #     osrm_url="http://127.0.0.1:5000/route/v1/driving/",
    #     min_dist_from_prev_ping=10,
    #     max_dist_from_prev_ping=250)

    # Get the cleaned trace and other summary information
    clean_trace_output = clean_trace_obj.get_trace_cleaning_output()

    # Extract data from output
    # Extract trace
    clean_trace = clean_trace_output['cleaned_trace']
    # Extract summaries
    cleaning_summary = clean_trace_output['cleaning_summary']
    distance_summary = clean_trace_output['distance_summary']

    # Plot map to compare raw vs clean
    print("Creating map ...")
    cleaning_comparison_map = clean_trace_obj.plot_cleaning_comparison_map(former_trace=raw_trace,
                                                                           latter_trace=clean_trace)

    # Define paths
    results_base_path = constants.BASE_PATH + "example_results/"
    create_path(results_base_path)
    clean_trace_payload_path = results_base_path + "clean_trace_payload.json"
    cleaning_comparison_map_path = results_base_path + "cleaning_comparison_map.html"

    print(f"Saving results at {results_base_path}")

    # Dump output trace as json
    dump_json_file_path = clean_trace_payload_path
    with open(dump_json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(clean_trace_output, json_file, indent=4)

    # Save map
    cleaning_comparison_map.save(cleaning_comparison_map_path)
