import json

from src.tracely.clean_trace import CleanTrace
from src.tracely import constants
from src.tracely.utils.utils import create_path


# Example usage of CleanTrace for trace similarity calculation
if __name__ == "__main__":

    # Load existing traces
    traces_path = constants.BASE_PATH + "/tests/similarity_calculation_payloads/large_trace_pair.json"

    with open(traces_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    
    trace_1 = payload["trace_1"]
    trace_2 = payload["trace_2"]

    similarity_result = CleanTrace.calculate_trace_similarity(trace_1, trace_2, distance_threshold = 100, time_threshold = 10000, plot_map = True)

    # Get similarity_percentage and metadata from result
    similarity_result_stats = {"similarity_percentage": similarity_result["similarity_percentage"],
                               "metadata": similarity_result["metadata"]}

    # Get map plot from result
    similarity_result_plot = similarity_result["plot"]

    # Define paths
    results_base_path = constants.BASE_PATH + "example_results/"
    similarity_result_path = results_base_path + "similarity_result.json"
    similarity_map_path = results_base_path + "similarity_map.html"
    create_path(results_base_path)

    print(f"Saving results at {results_base_path}")

    # Dump similarity result statistics as json
    dump_json_file_path = similarity_result_path
    with open(dump_json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(similarity_result_stats, json_file, indent=4)

    # Save map
    similarity_result_plot.save(similarity_map_path)