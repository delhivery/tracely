import json

from tests.testing_utils import load_trace_payload
from src.tracely.clean_trace import CleanTrace
from src.tracely import constants
from src.tracely.utils.utils import create_path


# Example usage of CleanTrace to add stop information
if __name__ == "__main__":

    # Load existing payload
    input_trace_payload = load_trace_payload("dummy_trace_input_payload")

    # Initialize the CleanTrace class with the input payload
    clean_trace_obj = CleanTrace(input_trace_payload)

    # Example usage for adding stop event information
    clean_trace_obj.add_stop_events_info(max_dist_bw_consecutive_pings=15,
                                         min_size=2,
                                         min_staying_time=60)

    # Get the cleaned trace and other summary information
    clean_trace_output = clean_trace_obj.get_trace_cleaning_output()

    # Extract stop summary
    stop_summary = clean_trace_output['stop_summary']

    # Plot comparison map between raw and stop event pings
    print("Creating map ...")
    raw_vs_stop_map = clean_trace_obj.plot_raw_vs_stop_comparison_map()

    # Define paths
    results_base_path = constants.BASE_PATH + "example_results/"
    create_path(results_base_path)
    stop_summary_path = results_base_path + "stop_summary.json"
    raw_vs_stop_map_path = results_base_path + "raw_vs_stop_map.html"

    print(f"Saving result at {results_base_path}")

    # Dump stop summary as json
    dump_json_file_path = stop_summary_path
    with open(dump_json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(stop_summary, json_file, indent=4)
        
    # Save map
    raw_vs_stop_map.save(raw_vs_stop_map_path)
