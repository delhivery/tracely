import copy
import pandas as pd
from scipy.spatial import KDTree

from .utils.data_validation_utils import DataValidationUtils
from .utils.utils import get_haversine_distance
from .utils.plotting_utils import plot_trace_overlap_map
from .utils.output_validation_utils import validate_trace_similarity_output


def _calculate_trace_similarity(trace_1, trace_2, distance_threshold, time_threshold):
    """
    Calculates the spatial and temporal similarity between two traces based on specified haversine distance and time thresholds.

    This function determines the similarity of `trace_1` with `trace_2` by identifying the pings in `trace_1` that have a point in `trace_2` 
    within the given distance and time thresholds. Overlapping pings are paired by their indices, and the similarity percentage is the percentage of pings in `trace_1` that are overlapping.

    Args:
        trace_1 (np.array): The first trace as numpy array, where each inner array represents [latitude, longitude, timestamp].
                            Here, latitude and longitude are in decimal degrees, and timestamp is in milliseconds.
        trace_2 (np.array): The second trace as numpy array, where each inner array represents [latitude, longitude, timestamp].
                            Here, latitude and longitude are in decimal degrees, and timestamp is in milliseconds.
        distance_threshold (float): The maximum allowable haversine distance (in meters) for two pings to be considered overlapping.
        time_threshold (int): The maximum allowable time difference in milliseconds for two pings to be considered overlapping.

    Returns:
        dict: 
            A dictionary containing:
                similarity_percentage (float): The percentage of pings in `trace_1` that have overlapping pings in `trace_2`.
                overlapping_pings_indices (list): A list of pairs of indices, where each pair represents the indices of overlapping pings in `trace_1` and `trace_2`.
    """

    time_pings_2 = trace_2[:, 2].reshape(-1, 1)
    time_tree_2 = KDTree(time_pings_2)

    matched_pings_counts = 0
    overlapping_pings_pairs = []

    for i, (lat1, lng1, timestamp_1) in enumerate(trace_1):
        candidates = time_tree_2.query_ball_point(timestamp_1, r=time_threshold)

        if not candidates:
            continue

        min_distance = float('inf')
        closest_candidate_index = -1

        for candidate_index in candidates:
            lat2, lng2, _ = trace_2[candidate_index]
            haversine_distance = get_haversine_distance(lat1, lng1, lat2, lng2)

            if haversine_distance < min_distance:
                min_distance = haversine_distance
                closest_candidate_index = candidate_index

        if min_distance <= distance_threshold:
            matched_pings_counts += 1
            overlapping_pings_pairs.append([i, closest_candidate_index])

    similarity_percentage = (matched_pings_counts / len(trace_1)) * 100
    similarity_percentage = round(similarity_percentage, 5)

    return {
        "similarity_percentage": similarity_percentage,
        "overlapping_pings_indices": overlapping_pings_pairs
    }


def calculate_trace_similarity(trace_1, trace_2, distance_threshold, time_threshold, plot_map=False):
    """
    Calculates the similarity between two traces based on spatial and temporal thresholds.
    This function determines the overlap between `trace_1` and `trace_2` by identifying pairs of pings that are within the specified haversine distance and time thresholds. 
    It computes the similarity percentage for both traces and optionally generates a map visualization of the traces. Pings with null latitude or longitude values in the input traces are ignored for similarity calculation.

    Args:
        trace_1 (list): The first trace as list of list, where each inner list represents [latitude, longitude, timestamp].
                        Here, latitude and longitude are in decimal degrees, and timestamp is in milliseconds.
        trace_2 (list): The second trace as list of list, where each inner list represents [latitude, longitude, timestamp].
                        Here, latitude and longitude are in decimal degrees, and timestamp is in milliseconds.
        distance_threshold (float): The maximum allowable haversine distance (in meters) for two pings to be considered overlapping.
        time_threshold (int): The maximum allowable time difference in milliseconds for two pings to be considered overlapping.
        plot_map (bool, optional): A flag indicating whether to generate a folium map visualization of the traces. Defaults to `False`.

    Returns:
        dict: 
            A dictionary containing:
                max_similarity_percentage (float): Maximum of similarity percentages calculated between the two traces with respect to each other.
                metadata (dict): Detailed similarity information including:
                    similarity_info_trace_1_to_2 (dict): Similarity of `trace_1` with respect to `trace_2`, containing:
                        similarity_percentage (float): Percentage of pings in `trace_1` overlapping with `trace_2`.
                        overlapping_pings_indices (list): Pairs of indices representing overlapping pings in `trace_1` and `trace_2`.
                    similarity_info_trace_2_to_1 (dict): Similarity of `trace_2` with respect to `trace_1`, containing:
                        similarity_percentage (float): Percentage of pings in `trace_2` overlapping with `trace_1`.
                        overlapping_pings_indices (list): Pairs of indices representing overlapping pings in `trace_2` and `trace_1`.
                plot (folium.plugins.DualMap or None): A folium map visualization of the traces, if plot_map is True. Otherwise, None.

    Raises:
        All exceptions raised by the following functions:
            "DataValidationUtils.validate_calculate_trace_similarity_parameters" present in data_validation_utils.py.
            "validate_trace_similarity_output" present in output_validation_utils.py.
    """

    # Validate input data
    DataValidationUtils.validate_calculate_trace_similarity_parameters(trace_1, trace_2, distance_threshold, time_threshold, plot_map)

    # Create DataFrames and store original indices
    trace_1_df = pd.DataFrame(trace_1, columns=["latitude", "longitude", "timestamp"])
    trace_2_df = pd.DataFrame(trace_2, columns=["latitude", "longitude", "timestamp"])

    # Add original_index before removing pings with null coordinates.
    trace_1_df['original_index'] = trace_1_df.index  
    trace_2_df['original_index'] = trace_2_df.index

    # Remove pings with null coordinates.
    trace_1_df.dropna(subset=["latitude", "longitude"], inplace=True)
    trace_2_df.dropna(subset=["latitude", "longitude"], inplace=True)

    # SOrt traces by timestamp
    trace_1_df.sort_values(by=["timestamp"], inplace=True)
    trace_2_df.sort_values(by=["timestamp"], inplace=True)

    trace_1_np = trace_1_df[["latitude", "longitude", "timestamp"]].values
    trace_2_np = trace_2_df[["latitude", "longitude", "timestamp"]].values

    # Get similarity information
    similarity_info_trace_1_to_2 = _calculate_trace_similarity(trace_1_np, trace_2_np, distance_threshold, time_threshold)
    similarity_info_trace_2_to_1 = _calculate_trace_similarity(trace_2_np, trace_1_np, distance_threshold, time_threshold)

    # Indices are according to cleaned traces with no ping with null coordinates, map these back according to original input traces
    # For first trace
    overlapping_pings_indices_1_to_2 = []
    for idx_1, idx_2 in similarity_info_trace_1_to_2["overlapping_pings_indices"]:
        original_idx_1 = trace_1_df.iloc[idx_1]['original_index']
        original_idx_2 = trace_2_df.iloc[idx_2]['original_index']
        overlapping_pings_indices_1_to_2.append([original_idx_1, original_idx_2])

    similarity_info_trace_1_to_2["overlapping_pings_indices"] = overlapping_pings_indices_1_to_2

    # For second trace
    overlapping_pings_indices_2_to_1 = []
    for idx_1, idx_2 in similarity_info_trace_2_to_1["overlapping_pings_indices"]:
        original_idx_1 = trace_2_df.iloc[idx_1]['original_index']
        original_idx_2 = trace_1_df.iloc[idx_2]['original_index']
        overlapping_pings_indices_2_to_1.append([original_idx_1, original_idx_2])

    similarity_info_trace_2_to_1["overlapping_pings_indices"] = overlapping_pings_indices_2_to_1

    # Take higher of individual similarity percentages as overall similarity.
    max_similarity_percentage = max(similarity_info_trace_1_to_2["similarity_percentage"], similarity_info_trace_2_to_1["similarity_percentage"])
    
    # Assign similarity information of individual traces in metadata dict
    metadata = {
        "similarity_info_trace_1_to_2": similarity_info_trace_1_to_2,
        "similarity_info_trace_2_to_1": similarity_info_trace_2_to_1
    }

    # Plot map according to plot_map argument
    if plot_map:
        similarity_percentage_trace_1_to_2 = round(similarity_info_trace_1_to_2["similarity_percentage"], 2)
        similarity_percentage_trace_2_to_1 = round(similarity_info_trace_2_to_1["similarity_percentage"], 2)

        plot = plot_trace_overlap_map(trace_1_np,
                                      trace_2_np, 
                                      similarity_percentage_trace_1_to_2,
                                      similarity_percentage_trace_2_to_1)
    else:
        plot = None

    # Create output dictionary
    similarity_result = {"max_similarity_percentage": max_similarity_percentage, 
                         "metadata": metadata, 
                         "plot": plot}

    # Validate output
    validate_trace_similarity_output(similarity_result, 
                                     trace_1_df.values.tolist(), 
                                     trace_2_df.values.tolist(),
                                     )

    return similarity_result

