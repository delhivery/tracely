import copy
import time
import pandas as pd
import numpy as np
import folium as fl
from infostop import Infostop

from . import constants
from .create_trace_data import CreateTraceData
from .utils.data_validation_utils import DataValidationUtils
from .utils.utils import get_haversine_distance, \
                         calculate_change_in_direction, \
                         convert_unix_timestamp_to_human_readable, \
                         convert_time_interval_to_human_readable, \
                         calculate_trace_distance

from .utils.plotting_utils import plot_raw_trace_from_trace_output, \
                                  plot_clean_trace_from_trace_output, \
                                  plot_cleaning_comparison_map_bw_two_traces, \
                                  plot_stop_comparison_map

from .utils.output_validation_utils import validate_cleaned_trace, \
                                           validate_clean_trace_output

from .utils.osrm_utils import create_segments, \
                              process_trace_segments, \
                              get_osrm_route


class CleanTrace():
    def __init__(self,
                 input_trace_payload: dict) -> None:
        """
        It validates and processes input trace payload provided in `input_trace_payload` argument.
        Creates the following attributes for CleanTrace class:
            :input_trace_payload (dict): Contains input trace payload.
            :trace_data (dict): Contains trace data will filled missing values. Created from input trace payload.
            :vehicle_type (str): Contains vehicle type.
            :vehicle_speed (int, float): Contains vehicle speed.
            :trace_df (pandas.DataFrame): Contains dataframe representation of pings.
            :input_pings_count (int): Contains number of pings in `trace_data`.
            :raw_trace (list): Contains raw trace data.
            :elapsed_time_record (list): Contains record of time spend to run each trace cleaning function.

        Args:
            trace_payload (dict): Trace payload with pings list, vehicle type and vehicle speed information. Has following keys and values:
                trace (list): A list of dictionaries where each dictionary contains the metadata of a ping.
                    Each ping can only have the following keys and values:
                        :ping_id (str, optional) : Unique identifier for the current ping.
                            Note: ping_id must be present in either all of the pings or user can choose to not pass it in any of the pings. We will automatically assign a unique string for each ping.
                        :latitude (int, float, None): Latitude of the ping in decimal degrees format.
                                                      `latitude` must be within range [-90 to 90], or None.
                        :longitude (int, float, None): Longitude of the ping in decimal degrees format.
                                                       `longitude` must be within range [-180 to 180], or None.
                        :timestamp (int): Unix timestamp for the ping in milliseconds.
                        :error_radius (float, None, optional): GPS error radius of the ping in meters.
                        :event_type (str, None, optional): A string denoting the event which occurred for the ping.
                        :force_retain (bool, optional): Should be True if we do not want to drop the corresponding ping during cleaning.
                                                        If False then the current ping may or may not be retained.
                        :metadata (dict, optional): A dictionary to store metadata related to the ping. In this dictionary, keys are strings and values can be of any datatype.
                vehicle_type (str, optional): A string denoting the type of vehicle.
                vehicle_speed (int, float, optional): A number denoting the average speed of the vehicle in kilometers per hour.

        Raises:
            All exceptions raised by the following functions:
                "DataValidationUtils.validate_trace_payload" present in data_validation_utils.py.
        """
        
        start_time = time.time()
        self.input_trace_payload = copy.deepcopy(input_trace_payload)
        self.trace_data = CreateTraceData(input_trace_payload).create_trace_data()
        self.vehicle_type = self.trace_data["vehicle_type"]
        self.vehicle_speed = self.trace_data["vehicle_speed"]  # km/hr
        self.trace_df = copy.deepcopy(self.trace_data["trace_df"])
        self.raw_trace = self.trace_df.to_dict(orient="records")
        self.input_pings_count = len(self.trace_df)
        self.elapsed_time_record = []
        self._add_runtime_info(function_name="object_initialization",
                               time_taken=(time.time() - start_time))

    def _add_runtime_info(self,
                          function_name,
                          time_taken) -> None:
        """
        Records the execution time of a specified function. This internal method appends the name of the function and the time it took to execute
        to an elapsed time record. This can be useful for performance monitoring and debugging.

        Args:
            function_name (str): The name of the function whose runtime is being recorded.
            time_taken (float): The time taken for the function to execute, measured in seconds.
        """

        self.elapsed_time_record.append({"function_name": function_name,
                                         "time_taken": time_taken})

    def _merge_dataframes(self, 
                          df1: pd.DataFrame,
                          df2: pd.DataFrame,
                          match_key: str,
                          common_keys: list) -> pd.DataFrame:
        """
        Performs left join on two DataFrames `df1` and `df2` on a specified key (`match_key`).
        For each column specified in `common_keys`, the values in `df1` will be updated with those from `df2` where available.

        Args:
            df1 (pandas.DataFrame): The primary dataframe to which data from df2 will be merged.
            df2 (pandas.DataFrame): The secondary dataframe containing additional data to merge with df1.
            match_key (str): The column name on which to merge the two dataframes.
            common_keys (list): A list of column names that should be updated.

        Returns:
            pandas.DataFrame: A new DataFrame with updated values, based on the match_key and common_keys provided.
        """

        df1_copy = df1.reset_index(drop=True)
        df2_copy = df2.reset_index(drop=True)

        # Merge the DataFrames on match_key with a left join
        merged_df = df1.merge(df2_copy,
                              on=match_key,
                              how="left",
                              suffixes=("", "_new"))
        
        # For each common key, update the values in df1 with values from df2
        for key in common_keys:
            if (key in merged_df.columns) and (f"{key}_new" in merged_df.columns):
                df1_copy[key] = merged_df[f"{key}_new"].combine_first(df1_copy[key])

        return df1_copy

    def _merge_and_update_trace_df(self,
                                   df_to_match: pd.DataFrame,
                                   match_key: str,
                                   common_keys: list) -> pd.DataFrame:
        """
        Perform a left join of the instance's trace_df with `df_to_match` on the specified `match_key`.
        The merging process ensures that all rows from `trace_df` are retained, and only the matching rows from `df_to_match` are used for updating.

        Args:
            df_to_match (pandas.DataFrame): A dataframe containing additional data that will be merged into the instance's trace_df. 
            match_key (str): The column name on which to merge the two dataframes.
            common_keys (list): A list of column names that should be updated in trace_df.

        Returns:
            pandas.DataFrame: The instance's trace_df with updated values from `df_to_match`, based on the match_key and common_keys provided.
        """
        trace_df = self.trace_df.reset_index(drop=True)

        # Merge the DataFrames on match_key with a left join
        merged_df = trace_df.merge(df_to_match.reset_index(drop=True),
                                   on=match_key,
                                   how="left",
                                   suffixes=("", "_new"))

        # For each common key, update the values in self.trace_df with values from df_to_match
        for key in common_keys:
            if (key in merged_df.columns) and (f"{key}_new" in merged_df.columns):
                self.trace_df[key] = merged_df[f"{key}_new"].combine_first(self.trace_df[key])

    def _insert_points_between_coords(self, 
                                      coord1: tuple, 
                                      coord2: tuple, 
                                      n: int):
        """
        Inserts n points between two latitude-longitude pairs treating them as lying on a straight line.

        Args:
            coord1 (tuple): The first coordinate as (lat, lng).
            coord2 (tuple): The second coordinate as (lat, lng).
            n (int): Number of points to insert between coord1 and coord2.

        Returns:
            List (tuple): List of coordinates including the original two points and n inserted points.
        """

        # Initialize the list of points with the starting coordinate
        points = [coord1]

        # Calculate the increments in latitude and longitude
        lat_increment = (coord2[0] - coord1[0]) / (n + 1)
        lng_increment = (coord2[1] - coord1[1]) / (n + 1)

        # Insert n points at equal intervals
        for i in range(1, n + 1):
            new_point = (coord1[0] + i * lat_increment,
                         coord1[1] + i * lng_increment)
            points.append(new_point)

        # Append the final coordinate
        points.append(coord2)

        return points

    def remove_nearby_pings(self, 
                            min_dist_bw_consecutive_pings=5) -> None:
        """
        Function to remove pings that are too close to their previous ping.
        Iterates through pings and check if the haversine distance between current and previous ping (in meters) is less then `min_dist_bw_consecutive_pings`
        If distance is less then minimum threshold and force_retain for current ping is False, then the current ping is removed.
        For a removed ping, its "cleaned_latitude" and "cleaned_longitude" are set to None, update_status is set to "dropped" and last_updated_by is set to "remove_nearby_pings".
        Does not drop an interpolated ping (ping added by "interpolate_trace" method).

        Args:
            min_dist_bw_consecutive_pings (int, float, optional): The minimum threshold for distance between consecutive pings, in meters. Defaults to 5.

        Raises:
            All exceptions raised by the following functions:
                "DataValidationUtils.validate_remove_nearby_pings_parameters" present in data_validation_utils.py.
        """

        start_time = time.time()

        # Validate parameter
        DataValidationUtils.validate_remove_nearby_pings_parameters(min_dist_bw_consecutive_pings=min_dist_bw_consecutive_pings)

        # Get the first ping as the starting reference
        prev_ping = (self.trace_df.iloc[0]["cleaned_latitude"],self.trace_df.iloc[0]["cleaned_longitude"])

        # Create a mask to store which rows need to be updated
        mask = np.zeros(len(self.trace_df), dtype=bool)

        for i, row in enumerate(self.trace_df.to_dict("records")[1:], 1):

            current_ping = (row["cleaned_latitude"], row["cleaned_longitude"])

            if ((prev_ping[0] is None) or
                (prev_ping[1] is None) or
                (pd.isna(prev_ping[0])) or
                (pd.isna(prev_ping[1]))):

                prev_ping = current_ping
                continue

            if ((current_ping[0] is None) or
                (current_ping[1] is None) or
                (pd.isna(current_ping[0])) or
                (pd.isna(current_ping[1]))):
                continue

            # Do not update current ping if it is an interpolated ping
            if row["update_status"] == "interpolated":
                continue

            # Calculate the distance between the current ping and the previous ping
            distance = get_haversine_distance(prev_ping[0],
                                              prev_ping[1],
                                              current_ping[0],
                                              current_ping[1])

            if distance is None:
                continue

            if (distance < min_dist_bw_consecutive_pings) and not (row["force_retain"]):
                mask[i] = True
                continue

            # Update the previous ping if the current one is kept
            prev_ping = current_ping  

        self.trace_df.loc[mask, ["cleaned_latitude", "cleaned_longitude", "update_status", "last_updated_by"]] = [None, None, "dropped", "remove_nearby_pings"]

        self.trace_df["cleaned_latitude"].replace({np.nan: None}, inplace=True)
        self.trace_df["cleaned_longitude"].replace({np.nan: None}, inplace=True)

        self._add_runtime_info(function_name="remove_nearby_pings", time_taken=((time.time() - start_time)))

    def _impute_distorted_pings_with_distance(self,
                                              max_dist_ratio=3, 
                                              side_win_len=1) -> None:
        """
        Imputes distorted pings based on distance analysis.
        This internal function examines each ping within a specified window length and checks if the sum of the haversine distances from the current ping to its adjacent pings
        exceeds a defined threshold ratio (`max_dist_ratio`) compared to the direct distance between the adjacent pings.
        If the condition is met, it replaces the current ping with interpolated pings between the adjacent pings.

        Args:
            max_dist_ratio (int, optional): The maximum allowable ratio for the distance comparison. If the ratio of the sum of distances from the adjacent
                                            pings to the distance between them exceeds this value, imputation occurs. Defaults to 3.

            side_win_len (int, optional): The length of the side window used to consider adjacent pings. A value of 1 means the function will look one ping back and one ping forward. 
                                          Defaults to 1.
        """

        trace_df = copy.deepcopy(self.trace_df[(~self.trace_df["cleaned_latitude"].isnull()) & 
                                               (~self.trace_df["cleaned_longitude"].isnull())])
        
        trace_df = trace_df.reset_index(drop=True)

        # Extract latitude, longitude and update_status data as NumPy array for faster access
        latitudes = trace_df["cleaned_latitude"].values
        longitudes = trace_df["cleaned_longitude"].values
        update_statuses = trace_df["update_status"].values

        for i in range(side_win_len, len(trace_df)-side_win_len):

            # Do not update current ping if it is an interpolated ping
            if update_statuses[i] == "interpolated":
                continue

            # Get current and its adjacent pings
            prev_ping = (latitudes[i - side_win_len], longitudes[i - side_win_len])
            current_ping = (latitudes[i], longitudes[i])
            next_ping = (latitudes[i + side_win_len], longitudes[i + side_win_len])

            # Get distance between ping pairs
            haversine_distance_dist_bw_prev_and_current = get_haversine_distance(prev_ping[0],
                                                                                 prev_ping[1],
                                                                                 current_ping[0], 
                                                                                 current_ping[1])
            
            haversine_distance_dist_bw_current_and_next = get_haversine_distance(current_ping[0], 
                                                                                 current_ping[1], 
                                                                                 next_ping[0], 
                                                                                 next_ping[1])
            
            haversine_distance_dist_bw_prev_and_next = get_haversine_distance(prev_ping[0], 
                                                                              prev_ping[1], 
                                                                              next_ping[0], 
                                                                              next_ping[1])

            # Get distance covered by covered by current ping
            dist_cov_by_current_ping = haversine_distance_dist_bw_prev_and_current + haversine_distance_dist_bw_current_and_next

            # If distance covered by current ping breaches limit, impute it
            if dist_cov_by_current_ping > (max_dist_ratio * haversine_distance_dist_bw_prev_and_next):
                pings = self._insert_points_between_coords(prev_ping,
                                                           next_ping, 
                                                           (2 * side_win_len) - 1)
                pings = pings[1:-1]

                for j, ping in enumerate(pings):
                    idx = i - side_win_len + 1 + j
                    if idx < len(
                            trace_df):  # Check to avoid index out of bounds
                        trace_df.at[idx,"last_updated_by"] = "impute_distorted_pings_with_distance"
                        trace_df.at[idx, "cleaned_latitude"] = ping[0]
                        trace_df.at[idx, "cleaned_longitude"] = ping[1]
                        trace_df.at[idx, "update_status"] = "updated"

        self._merge_and_update_trace_df(trace_df,
                                        match_key="ping_id",
                                        common_keys=["cleaned_latitude", "cleaned_longitude", "last_updated_by", "update_status"])

    def impute_distorted_pings_with_distance(self, 
                                             max_dist_ratio=3) -> None:
        """
        Imputes distorted pings using distance-based criteria.
        This method iterates through pings and calculates the pairwise haversine distance between the current ping and its adjacent pings.
        If the ratio of the sum of distances from the current to the adjacent pings, compared to the distance between adjacent pings, exceeds `max_dist_ratio`,
        the current ping is imputed between the adjacent pings.
        This function does not impute pings marked as "interpolated" (pings added by the interpolate_trace method).

        Args:
            max_dist_ratio (int or float, optional): The maximum allowable ratio for the distance comparison. Defaults to 3.

        Raises:
            All exceptions raised by the following functions:
                :"DataValidationUtils.validate_impute_distorted_pings_with_distance_parameters" present in data_validation_utils.py.
        """

        start_time = time.time()

        # Validate parameter
        DataValidationUtils.validate_impute_distorted_pings_with_distance_parameters(max_dist_ratio=max_dist_ratio)

        self._impute_distorted_pings_with_distance(max_dist_ratio, side_win_len=1)
        self._impute_distorted_pings_with_distance(max_dist_ratio=4, side_win_len=2)
        self._add_runtime_info(function_name="impute_distorted_pings_with_distance",
                               time_taken=(time.time() - start_time))

    def impute_distorted_pings_with_angle(self, 
                                          max_delta_angle=120):
        """
        Imputes distorted pings on the basis of angle (change in direction).
        Iterates through consecutive pings and calculate the change in direction at current ping w.r.t. previous and next ping (in decimal degrees, <= 180 degrees).
        If the change in direction is larger than `max_delta_angle` argument then current ping is imputed with mean of previous and next ping.
        This function does not impute pings marked as "interpolated" (pings added by the `interpolate_trace` method).

        Args:
            max_delta_angle (int, float, optional): Threshold for maximum change in direction at a ping. Defaults to 120 degrees.

        Raises:
            All exceptions raised by the following functions:
                :"DataValidationUtils.validate_impute_distorted_pings_with_angle_parameters" present in data_validation_utils.py.
        """

        start_time = time.time()

        # Validate parameter
        DataValidationUtils.validate_impute_distorted_pings_with_angle_parameters(max_delta_angle=max_delta_angle)

        trace_df = copy.deepcopy(self.trace_df[(~self.trace_df["cleaned_latitude"].isnull()) & 
                                               (~self.trace_df["cleaned_longitude"].isnull())])
        trace_df = trace_df.reset_index(drop=True)

        # Convert DataFrame to NumPy array for faster access
        latitudes = trace_df["cleaned_latitude"].values
        longitudes = trace_df["cleaned_longitude"].values
        update_statuses = trace_df["update_status"].values
        
        # Iterate over the DataFrame starting from the second row
        for i in range(2, len(trace_df) - 2):
            # Do not update current ping if it is an interpolated ping
            if update_statuses[i] == "interpolated":
                continue

            # Get current and its adjacent pings
            prev_ping = (latitudes[i - 1], longitudes[i - 1])
            current_ping = (latitudes[i], longitudes[i])
            next_ping = (latitudes[i + 1], longitudes[i + 1])

            # Get change in direction at current ping
            delta_direction_at_current_ping = calculate_change_in_direction(
                prev_ping, current_ping, next_ping)

            # If change in direction at current ping breaches limit, impute it
            if delta_direction_at_current_ping > max_delta_angle:
                # Set last_updated_by field to function name
                trace_df.at[i, "last_updated_by"] = "impute_distorted_pings_with_angle"
                
                # Impute lat
                trace_df.at[i, "cleaned_latitude"] = (prev_ping[0] + next_ping[0]) / 2
                
                # Impute lon
                trace_df.at[i, "cleaned_longitude"] = (prev_ping[1] + next_ping[1]) / 2
                
                # Set the update status as "updated"
                trace_df.at[i, "update_status"] = "updated"

        self._merge_and_update_trace_df(trace_df,
                                        match_key="ping_id",
                                        common_keys=["cleaned_latitude", "cleaned_longitude", "last_updated_by", "update_status"])
        
        self._add_runtime_info(function_name="impute_distorted_pings_with_angle",
                               time_taken=(time.time() - start_time))

    def map_match_trace(self,
                     osrm_url="http://127.0.0.1:5000/match/v1/driving/",
                     ping_batch_size=5,
                     map_matching_radius=20,
                     avg_snap_distance=12,
                     max_matched_dist_to_raw_dist_ratio=1.3):
        """
        This function matches pings to the road network using the OSRM map matching service.
        The pings are processed in batches of size defined by the `ping_batch_size` parameter. It is important that `ping_batch_size` does not exceed 100 unless 
        the OSRM server is configured to handle larger requests. For a matched ping, its cleaned_latitude and cleaned_longitude are updated, update_status is set to "updated"
        and last_updated_by is set to "map_match_trace".

        Args:
            osrm_url (str, optional): The URL of the OSRM server for map matching.
                                      Defaults to "http://127.0.0.1:5000/match/v1/driving/", which expects the OSRM server
                                      to be running locally on port 5000.
            ping_batch_size (int, optional): The size of each segment for map matching pings. Defaults to 5.
            map_matching_radius (int, float, optional): The radius in meters for map matching. A location is map matched only 
                                                        if there is a road within the map matching radius. Defaults to 20 meters.
            avg_snap_distance (int, float, optional): Average snap distance in meters for all points in a segment. Defaults to 12 meters.
            max_matched_dist_to_raw_dist_ratio (int, float, optional): For two consecutive pings, it is the maximum ratio between (distance between point after map matching)
                                                                       and (distance between point before map matching). Defaults to 1.3.
            
        Raises:
            All exceptions raised by the following functions:
                "DataValidationUtils.validate_map_match_trace_parameters" present in data_validation_utils.py.
                "get_osrm_match" present in osrm_utils.py.
        """

        start_time = time.time()

        # Validate parameters
        DataValidationUtils.validate_map_match_trace_parameters(osrm_url=osrm_url,
                                                             ping_batch_size=ping_batch_size,
                                                             map_matching_radius=map_matching_radius, 
                                                             avg_snap_distance=avg_snap_distance,
                                                             max_matched_dist_to_raw_dist_ratio=max_matched_dist_to_raw_dist_ratio)

        trace_df = copy.deepcopy(self.trace_df)
        trace_segments = create_segments(trace_df, ping_batch_size)
        matched_segments = process_trace_segments(segments=trace_segments,
                                                  osrm_url=osrm_url,
                                                  map_matching_radius=map_matching_radius,
                                                  avg_snap_distance=avg_snap_distance,
                                                  max_matched_dist_to_raw_dist_ratio=max_matched_dist_to_raw_dist_ratio)

        matched_data_frame = pd.DataFrame(matched_segments,
                                          columns=["cleaned_latitude",
                                                   "cleaned_longitude",
                                                   "ping_id",
                                                   "last_updated_by",
                                                   "map_matching_status"])

        unique_ping_id_df = matched_data_frame.drop_duplicates(subset="ping_id", keep="first")
        unique_ping_id_df = unique_ping_id_df.drop(columns = ["last_updated_by"])

        # Merge the two DataFrames on "ping_id"
        merged_df = trace_df.merge(unique_ping_id_df,
                                   on="ping_id",
                                   how="left",
                                   suffixes=("", "_new"))

        # update flags for matched pings
        update_mask = merged_df["map_matching_status"] == 2
        trace_df.loc[update_mask, ["update_status", "last_updated_by"]] = ["updated", "map_match_trace"]

        # Use where to conditionally update the values
        trace_df["cleaned_latitude"] = np.where(merged_df["cleaned_latitude_new"].notna(),
                                                merged_df["cleaned_latitude_new"],
                                                trace_df["cleaned_latitude"])
        
        trace_df["cleaned_longitude"] = np.where(merged_df["cleaned_longitude_new"].notna(),
                                                 merged_df["cleaned_longitude_new"],
                                                 trace_df["cleaned_longitude"])

        del merged_df
        self.trace_df = trace_df
        
        self._add_runtime_info(
            function_name="map_match_trace",
            time_taken=(time.time() - start_time))
        
    def interpolate_trace(self,
                          osrm_url="http://127.0.0.1:5000/route/v1/driving/",
                          min_dist_from_prev_ping=10,
                          max_dist_from_prev_ping=250):
        """
        Interpolates pings between consecutive pings based on distance criteria.
        This function iterates through pings and checks the distance between consecutive pings that are map matched to road. If the distance between these pings lies in user specified range, 
        then the function retrieves the route geometry from the OSRM routing service and generates interpolated pings between the two consecutive pings.

        Args:
            osrm_url (str, optional): A URL specifying the endpoint for accessing the OSRM route service.
                                      Defaults to "http://127.0.0.1:5000/route/v1/driving/", which expects the OSRM server to be running locally on port 5000.
            min_dist_from_prev_ping (int, optional): Minimum distance in meters required between consecutive trace pings for interpolation. Defaults to 20 meters.
            max_dist_from_prev_ping (int, optional): Maximum distance in meters allowed between consecutive trace pings for interpolation. Defaults to 200 meters.
        
        Raises:
            All exceptions raised by the following functions:
                "DataValidationUtils.validate_interpolate_trace_parameters" present in data_validation_utils.py.
                "get_osrm_route" present in osrm_utils.py.
        """ 

        start_time = time.time()

        # Validate parameters
        DataValidationUtils.validate_interpolate_trace_parameters(osrm_url=osrm_url,
                                                                  min_dist_from_prev_ping=min_dist_from_prev_ping,
                                                                  max_dist_from_prev_ping=max_dist_from_prev_ping)
        
        # Collect all pings along with the interpolated pings
        interpolated_pings = []
        trace_data = self.trace_df.to_dict(orient="records")
        for i in range(1, len(trace_data)):
            interpolated_pings.append(trace_data[i-1])

            # Two consecutive point should be map matched
            if trace_data[i - 1]["last_updated_by"] == "map_match_trace" and trace_data[i]["last_updated_by"] == "map_match_trace":

                prev_lat, prev_lng, prev_time = trace_data[i-1]["cleaned_latitude"], trace_data[i-1]["cleaned_longitude"], trace_data[i-1]["timestamp"]

                curr_lat, curr_lng, curr_time = trace_data[i]["cleaned_latitude"], trace_data[i]["cleaned_longitude"], trace_data[i]["timestamp"]

                dist_from_prev_ping = get_haversine_distance(prev_lat, prev_lng, 
                                                             curr_lat, curr_lng)
                
                # Check if distance between the pings is in specified range
                if ((dist_from_prev_ping < min_dist_from_prev_ping) or
                    (dist_from_prev_ping > max_dist_from_prev_ping)):
                    continue

                # Get OSRM route between map matched pings
                geo_coords = ((prev_lat, prev_lng), (curr_lat, curr_lng))
                
                route_geometry, \
                    route_trace_distance, _ = get_osrm_route(geo_coords, osrm_url)

                total_interpolated_route_time = (curr_time - prev_time) / 1000

                # Check if interpolated route is usable
                if (route_geometry is None):
                    continue

                starting_snap_distance = \
                    get_haversine_distance(prev_lat, prev_lng, 
                                           route_geometry[0][0], route_geometry[0][1])
                
                ending_snap_distance = \
                    get_haversine_distance(curr_lat, curr_lng, 
                                           route_geometry[-1][0], route_geometry[-1][1])

                total_interpolated_trace_distance = \
                    starting_snap_distance + route_trace_distance + ending_snap_distance


                if ((total_interpolated_route_time <= constants.MIN_TIME_FOR_INTERPOLATED_ROUTE) or \
                    (total_interpolated_trace_distance > constants.MAX_INTERPOLATION_THRESHOLD_RATIO * dist_from_prev_ping)):
                    continue

                route_speed = total_interpolated_trace_distance / total_interpolated_route_time
                if (route_speed <= constants.MIN_SPEED_FOR_INTERPOLATED_ROUTE):
                    continue
                
                ping_id_count = 0
                prev_interpolated_ping_time = prev_time
                prev_ping_lat, prev_ping_lng = prev_lat, prev_lng

                # Since first and last point of route geometry are just snapped points
                if(len(route_geometry)<=2):
                    continue

                # Skipping first and last point of route_geometry
                route_geometry = route_geometry[1:-1]
                    
                for ping in range(len(route_geometry)):
                    segment_distance = get_haversine_distance(route_geometry[ping][0], route_geometry[ping][1],
                                                              prev_ping_lat, prev_ping_lng)
                    new_ping = trace_data[i-1].copy()
                    curr_ping_id = trace_data[i-1]["ping_id"]
                    ping_id_count += 1
                    new_ping_id = f"{curr_ping_id}_{ping_id_count}"
                    new_ping_lat, new_ping_lng = route_geometry[ping][0], route_geometry[ping][1]

                    interpolated_ping_time = min(
                        int(prev_interpolated_ping_time + (segment_distance / route_speed) * 1000),
                        curr_time)
                    
                    updates = {
                        "ping_id": new_ping_id,
                        "error_radius": None,
                        "timestamp": interpolated_ping_time,
                        "event_type": None,
                        "force_retain": False,
                        "input_latitude": None,
                        "input_longitude": None,
                        "cleaned_latitude": new_ping_lat,
                        "cleaned_longitude": new_ping_lng,
                        "update_status": "interpolated",
                        "last_updated_by": "interpolate_trace"}
                    
                    new_ping.update(updates)
                    prev_lat, prev_lng, prev_interpolated_ping_time = new_ping_lat, new_ping_lng, interpolated_ping_time
                    interpolated_pings.append(new_ping)

        interpolated_pings.append(trace_data[i])
        interpolation_result = pd.DataFrame(interpolated_pings)

        self.trace_df = interpolation_result
        self._add_runtime_info(function_name="interpolate_trace",
                               time_taken=(time.time() - start_time))

    def _get_stop_labels(self,
                         trace_list: list,
                         max_dist_bw_consecutive_pings,
                         max_dist_for_merging_stop_points,
                         min_size,
                         min_staying_time):
        """
        Provides labels for pings to indicate if they are stopping pings or not. For non stopping pings, label is -1.
        For stopping pings, label is a non negative integer according to sequence of stop event due to which ping was marked as stopping.

        Args:
            trace_list (list): List of pings. Each ping is a list in format [latitude, longitude, timestamp]. Timestamp should be unix epoch in seconds.
            max_dist_bw_consecutive_pings (int, float): Max distance between time-consecutive pings to label them as stopping.
            max_dist_for_merging_stop_points (int, float): Max distance between stopping pings to form an edge (merge two stopping pings).
            min_size (int): Minimum size of group to consider it stopping.
            min_staying_time (int): The shortest duration that can constitute a stop, in seconds.

        Returns:
            list: List of labels of size same as number of pings in input trace_list
        """

        # Infostop raises error if no stop ping is detected, we handle this error by returning all labels (in label list) as -1
        # This does not impact data because label for all non stopping pings is also -1 by default.
        try:
            # Convert trace list into numpy array
            trace_list = np.array(trace_list)

            # Create instance of label prediction model
            model = Infostop(r1=max_dist_bw_consecutive_pings, 
                             r2=max_dist_for_merging_stop_points, 
                             min_size=min_size,
                             min_staying_time=min_staying_time)
            
            # Predict label
            labels = model.fit_predict(trace_list)
            return labels
        
        except Exception:
            return [-1 for _ in range(len(trace_list))]

    def _get_stop_info(self,
                       trace_df: pd.DataFrame,
                       labels: list) -> pd.DataFrame:
        """
        Provides relevant stop information for stopping points.

        Args:
            labels (list): Labels denoting stopping pings information.

        Returns:
            pandas.DataFrame: Pandas dataframe with ping_id, stop_event_status, representative_stop_event_latitude, representative_stop_event_longitude,
                          cumulative_stop_event_time, stop_event_sequence_number columns in the order they are written.
        """
        
        trace_df_copy = copy.deepcopy(trace_df)

        # Create label key
        trace_df_copy["label_key"] = labels

        # Adding status key on the basis of labels in trace_df_copy
        trace_df_copy["stop_event_status"] = trace_df_copy["label_key"].apply(lambda x: True if x >= 0 else False)

        # Adding representative ping for each batch of stopping ping
        stopping_pings_df = trace_df_copy[trace_df_copy["label_key"] != -1]

        # Grouping by "label_key" and calculating mean for "cleaned_latitude" and "cleaned_longitude"
        grouped = stopping_pings_df.groupby("label_key").agg({"cleaned_latitude": "mean",
                                                             "cleaned_longitude": "mean"
                                                        }).reset_index()

        # Renaming columns to "representative_stop_event_latitude" and "representative_stop_event_longitude"
        grouped.rename(columns={"cleaned_latitude": "representative_stop_event_latitude",
                                "cleaned_longitude": "representative_stop_event_longitude"},
                       inplace=True)

        # Merging the representative values back into the original trace_df_copy
        trace_df_copy = self._merge_dataframes(df1=trace_df_copy,
                                               df2=grouped,
                                               match_key="label_key",
                                               common_keys=list(grouped.columns))

        # Filtering out the group with label_key = -1
        stopping_pings_df = trace_df_copy.loc[trace_df_copy["label_key"] != -1].copy()

        # Assigning a unique sequence number for each unique "label_key" group
        stopping_pings_df["stop_event_sequence_number"] = (stopping_pings_df["label_key"] != stopping_pings_df["label_key"].shift()).cumsum().copy()

        # Calculate time gap between pings in each stopping group, as "time_since_prev_ping"
        stopping_pings_df["time_since_prev_ping"] = stopping_pings_df.groupby("stop_event_sequence_number")["timestamp"].diff()
        
        # Calculate cumulative sum of "time_since_prev_ping" within each group defined by "stop_event_sequence_number"      
        # Set start time for each stopping group as 0
        stopping_pings_df["cumulative_stop_event_time"] = stopping_pings_df.groupby("stop_event_sequence_number")\
                                                                            ["time_since_prev_ping"].cumsum().replace({np.nan: 0})
    
        # Merge the sequence back into the original trace_df_copy
        trace_df_copy = self._merge_dataframes(df1=trace_df_copy,
                                               df2=stopping_pings_df[["ping_id","cumulative_stop_event_time","stop_event_sequence_number"]],
                                               match_key="ping_id",
                                               common_keys=["ping_id","cumulative_stop_event_time","stop_event_sequence_number"])

        trace_df_copy["cumulative_stop_event_time"].replace({np.nan: 0}, inplace=True)
        trace_df_copy["cumulative_stop_event_time"] = trace_df_copy.apply(lambda row: convert_time_interval_to_human_readable(row["cumulative_stop_event_time"], 
                                                                                                                              format="ms"),
                                                                    axis=1)

        return trace_df_copy[["ping_id", "stop_event_status", "representative_stop_event_latitude",
                              "representative_stop_event_longitude", "cumulative_stop_event_time", "stop_event_sequence_number"]]

    def add_stop_events_info(self,
                             max_dist_bw_consecutive_pings=10,
                             max_dist_for_merging_stop_points=0.001,
                             min_size=2,
                             min_staying_time=120) -> None:
        """
        Updates fields in each ping to add stop event information about the ping using cleaned_latitude, cleaned_longitude key. For non-stopping pings, fields are not updated.
        Following columns are updated and denote as described:
            :stop_event_status (bool): Denotes stop status of ping. Defaults to False, only updated when add_stop_events_info is called. Remains False if the ping is not a stop ping.
            :stop_event_sequence_number (int): Denotes the sequence number of stop event, if the current ping is part of a stop event.
                                               Defaults to -1, only updated when add_stop_events_info is called. Remains -1 if the ping is not an stop ping.
            :cumulative_stop_event_time (float): Denotes cumulative stop time in the stop event till that ping. Defaults to "0 minutes and 0 seconds", 
                                                 updated when add_stop_events_info is called. Remains "0 minutes and 0 seconds" if the ping is not a stop ping.
            :representative_stop_event_latitude (float, None): Denotes the representative latitude of the stop event, if current ping is its part.
                                                              Defaults to None, updated when add_stop_events_info is called. Remains None if the ping is not a stop ping.
            :representative_stop_event_longitude (float, None): Denotes the representative longitude of the stop event, if current ping is its part.
                                                               Defaults to None, updated when add_stop_events_info is called. Remains None if the ping is not a stop ping.

        Args:
            max_dist_bw_consecutive_pings (int, float, optional): Max distance between time-consecutive pings to label them as part of a stop event. Defaults to 10.
            max_dist_for_merging_stop_points (int, float, optional): Max distance between two representative stop event locations to form an edge (merge two stop events). Defaults to 0.001.
            min_size (int, optional): Minimum size of the group of pings that can constitute a stop event. Defaults to 2.
            min_staying_time (int, optional): The shortest duration in seconds that can constitute a stop event. Defaults to 120.

        Raises:
            All exceptions raised by the following functions:
                :"DataValidationUtils.validate_add_stop_events_info_parameters" present in data_validation_utils.py.
        """

        # Validate parameters
        DataValidationUtils.validate_add_stop_events_info_parameters(max_dist_bw_consecutive_pings=max_dist_bw_consecutive_pings, 
                                                                     max_dist_for_merging_stop_points=max_dist_for_merging_stop_points,
                                                                     min_size=min_size, 
                                                                     min_staying_time=min_staying_time)

        trace_df = copy.deepcopy(self.trace_df[(~self.trace_df["cleaned_latitude"].isnull()) & 
                                               (~self.trace_df["cleaned_longitude"].isnull()) & 
                                               (~self.trace_df["timestamp"].isnull())])
        
        trace_df["timestamp"] = trace_df["timestamp"] // 1000
        trace_list = trace_df[["cleaned_latitude","cleaned_longitude", "timestamp"]].values.tolist()

        labels = self._get_stop_labels(trace_list,
                                       max_dist_bw_consecutive_pings,
                                       max_dist_for_merging_stop_points,
                                       min_size,
                                       min_staying_time)

        stop_info_df = self._get_stop_info(trace_df,labels)

        stop_info_df = self._merge_dataframes(copy.deepcopy(self.trace_df), 
                                              stop_info_df, "ping_id", 
                                              common_keys=constants.STOP_INFO_KEYS)
        
        stop_info_df["representative_stop_event_latitude"].replace({np.nan: None}, inplace=True)
        stop_info_df["representative_stop_event_longitude"].replace({np.nan: None}, inplace=True)
        stop_info_df["stop_event_sequence_number"] = stop_info_df["stop_event_sequence_number"].astype(int)
        self.trace_df = stop_info_df

    def _add_time_interval_bw_pings(self, 
                                    trace_df: pd.DataFrame) -> pd.Series:
        """
        Provides difference in time between consecutive pings, on the basis of `timestamp` key.
        Timestamp should be unix timestamp in milliseconds. Since timestamp is in milliseconds, difference in time between consecutive pings will also be in milliseconds.

        Args:
            trace_df (pandas.DataFrame): Dataframe containing pings.

        Returns:
            pandas.Series: Series of time difference between consecutive pings.
        """

        timestamp_series = copy.deepcopy(trace_df[["timestamp"]])
        return timestamp_series["timestamp"].diff()

    def _add_distance_bw_pings(self, 
                               input_trace_df: pd.DataFrame) -> pd.Series:
        """
        Provides difference in distance between consecutive pings, on the basis of "cleaned_latitude", "cleaned_longitude" key.

        Args:
            input_trace_df (pandas.DataFrame): Dataframe containing pings.

        Returns:
            pandas.Series: Series of difference in distance between consecutive pings.
        """

        trace_df = copy.deepcopy(input_trace_df[["cleaned_latitude", "cleaned_longitude"]])

        # Step 1: Shift the "cleaned_latitude", "cleaned_longitude" column to create the initial "previous_latitude", "previous_longitude" column
        trace_df["previous_latitude"] = trace_df["cleaned_latitude"].shift(1)
        trace_df["previous_longitude"] = trace_df["cleaned_longitude"].shift(1)

        # Step 2: Forward fill "previous_latitude", "previous_longitude" only where it is not NaN
        trace_df["previous_latitude"] = trace_df["previous_latitude"].ffill()
        trace_df["previous_longitude"] = trace_df["previous_longitude"].ffill()

        # Step 3: Ensure "previous_latitude", "previous_longitude" are NaN where "cleaned_latitude", "cleaned_longitude" is NaN
        trace_df["previous_latitude"] = trace_df.apply(lambda row: row["previous_latitude"] if pd.notna(row["cleaned_latitude"]) else np.nan, axis=1)
        trace_df["previous_longitude"] = trace_df.apply(lambda row: row["previous_longitude"] if pd.notna(row["cleaned_longitude"]) else np.nan, axis=1)

        return trace_df.apply(lambda row: get_haversine_distance(row["previous_latitude"],
                                                                 row["previous_longitude"],
                                                                 row["cleaned_latitude"],
                                                                 row["cleaned_longitude"]),
                        axis=1)

    def _match_metadata(self, 
                        ping_list: list) -> list:
        """
        Matches metadata of each ping provided in `ping_list`.
        Metadata for each ping is kept as attribute in corresponding ping object, and is matched using ping_id.

        Args:
            ping_list (list): List of pings (dict).

        Returns:
            list: List of pings, with added metadata.
        """

        # Get list of Ping objects.
        ping_objects = self.trace_data["ping_objects"]

        # Create a lookup dictionary for ping_objects by their ping_id
        ping_objects_by_ping_id = {obj.ping_id: obj for obj in ping_objects}

        #  Add meta data corresponding to the ping_id or add an empty dictionary
        for ping in ping_list:
            unique_id = ping["ping_id"]
            if unique_id in ping_objects_by_ping_id:
                # If there"s a matching object, add its extra_attribute to the dictionary
                ping["metadata"] = ping_objects_by_ping_id[unique_id].metadata
            else:
                # If no matching object is found, add an empty dictionary
                ping["metadata"] = {}

        return ping_list

    def _create_output_trace(self) -> list:
        """
        Provide output trace (list of pings) from latest state of instance's trace_df attribute.

        Returns:
            list: List of pings.
        """

        # Add dist_from_prev_ping to instance's trace dataframe
        self.trace_df["dist_from_prev_ping"] = self._add_distance_bw_pings(self.trace_df)
        self.trace_df["dist_from_prev_ping"].replace({np.nan: 0}, inplace=True)

        # Add cleaned_trace_cumulative_dist to instance's trace dataframe
        self.trace_df["cleaned_trace_cumulative_dist"] = self.trace_df["dist_from_prev_ping"].cumsum()
        self.trace_df["cleaned_trace_cumulative_dist"].replace({np.nan: 0}, inplace=True)

        # Add time_since_prev_ping to instance's trace dataframe
        self.trace_df["time_since_prev_ping"] = self._add_time_interval_bw_pings(self.trace_df)
        self.trace_df["time_since_prev_ping"].replace({np.nan: 0}, inplace=True)

        # Add cleaned_trace_cumulative_time to instance's trace dataframe
        self.trace_df["cleaned_trace_cumulative_time"] = self.trace_df["time_since_prev_ping"].cumsum()
        self.trace_df["cleaned_trace_cumulative_time"].replace({np.nan: 0}, inplace=True)

        output_trace = self._match_metadata(self.trace_df[constants.CLEAN_TRACE_COLUMNS_WITHOUT_METADATA].to_dict(orient="records"))
        output_trace_df = pd.DataFrame(output_trace)

        for column in output_trace_df.columns:
            output_trace_df[column].replace({np.nan: None}, inplace=True)

        output_trace = output_trace_df.to_dict(orient="records")

        return output_trace

    def _create_output_cleaning_summary(self) -> dict:
        """
        Create output cleaning summary for trace data. Cleaning summary contains various metrics about trace cleaning operations and their effect.

        Returns:
            dict: A dictionary summarizing the results of trace cleaning. The dictionary has the following keys and values:
                  :total_pings_in_input (int): Denotes total number of pings in the input.
                  :total_non_null_pings_in_input (int): Denotes total number of non null pings in the input.
                  :total_pings_in_output (int): Denotes total number of non null pings in the output.
                  :unchanged_percentage (float): The percentage of pings from the input trace whose location is unchanged in the cleaned trace.
                                                 The percentage will be w.r.t the number of non null pings in the input.
                  :drop_percentage (float): The percentage of pings from the input trace which were dropped in cleaned trace w.r.t the number of non null pings in the input.
                  :updation_percentage (float): The percentage of updated pings  w.r.t the number of non null pings in the input.
                  :interpolation_percentage (float): The percentage of pings which are interpolated  w.r.t the number of non null pings in the input.
                  :total_execution_time (float): Total time taken in seconds for creating cleanable trace object and applying trace cleaning functions (CleanTrace methods)
        """

        # Get count of pings in input and output
        total_pings_in_input = len(self.raw_trace)

        total_non_null_pings_in_input = self.trace_df[self.trace_df["input_latitude"].notnull() & self.trace_df["input_longitude"].notnull()].shape[0]
        total_non_null_pings_in_output = self.trace_df[self.trace_df["cleaned_latitude"].notnull() & self.trace_df["cleaned_longitude"].notnull()].shape[0]

        # Get count of total unchanged, dropped and updated pings in output
        update_status_counts_for_existing_points = (self.trace_df[self.trace_df["input_latitude"].notnull() & self.trace_df["input_longitude"].notnull()].update_status).value_counts()
        
        # Access individual counts
        total_unchanged_pings_in_output = update_status_counts_for_existing_points.get("unchanged", 0)
        total_dropped_pings_in_output = update_status_counts_for_existing_points.get("dropped", 0)
        total_updated_pings_in_output = update_status_counts_for_existing_points.get("updated", 0)
        total_interpolated_pings_in_output = ((self.trace_df["cleaned_latitude"].notnull() & self.trace_df["cleaned_longitude"].notnull()) &
                                              (self.trace_df["update_status"] == "interpolated")).sum()
        
        # Get total execution time for trace cleaning operations
        total_execution_time = round(sum(item.get("time_taken", 0) for item in self.elapsed_time_record), 5)

        # Create output clean summary dictionary
        output_clean_summary = {
            "total_pings_in_input": total_pings_in_input,
            "total_non_null_pings_in_input": total_non_null_pings_in_input,
            "total_non_null_pings_in_output": total_non_null_pings_in_output,
            "total_trace_time":  convert_time_interval_to_human_readable(((self.trace_df["timestamp"].max()//1000) - (self.trace_df["timestamp"].min()//1000))),
            "unchanged_percentage": 0.0 if total_non_null_pings_in_input == 0 else round((total_unchanged_pings_in_output / total_non_null_pings_in_input) * 100, 2),
            "drop_percentage": 0.0 if total_non_null_pings_in_input == 0 else round((total_dropped_pings_in_output / total_non_null_pings_in_input) * 100, 2),
            "updation_percentage": 0.0 if total_non_null_pings_in_input == 0 else round((total_updated_pings_in_output / total_non_null_pings_in_input) * 100, 2),
            "interpolation_percentage": 0.0 if total_non_null_pings_in_input == 0 else round((total_interpolated_pings_in_output / total_non_null_pings_in_input) * 100, 2),
            "total_execution_time": total_execution_time }

        return output_clean_summary

    def _create_output_distance_summary(self) -> dict:
        """
        Create output distance summary for trace data. Distance summary contains various metrics about distance in clean and raw trace.

        Returns:
            dict: A dictionary summarizing distance metrics. The dictionary has the following keys and values:
                  :cumulative_distance_of_raw_trace (float): Denotes total distance covered in meters in raw trace, ignoring pings with null input_latitude and input_longitude.
                  :cumulative_distance_of_clean_trace (float): Denotes total distance covered in meters in cleaned trace, ignoring pings with null cleaned_latitude and cleaned_longitude.
                  :percent_reduction_in_dist (float): The percentage of reduction in distance in cumulative_distance_of_clean_trace w.r.t. cumulative_distance_of_raw_trace.
        """

        # Create copy of dataframe (only input_latitude, input_longitude columns) with only not null pings.
        trace_lat_lng_df = copy.deepcopy(self.trace_df[["input_latitude", "input_longitude"]])
        trace_lat_lng_df.dropna(inplace=True)

        # Add previous ping for each ping
        trace_lat_lng_df["previous_latitude"] = trace_lat_lng_df["input_latitude"].shift(1)
        trace_lat_lng_df["previous_longitude"] = trace_lat_lng_df["input_longitude"].shift(1)

        # Add distance of each ping from its previous ping
        trace_lat_lng_df["dist_from_prev_ping"] = trace_lat_lng_df.apply(lambda row: get_haversine_distance(row["previous_latitude"],
                                                                                                             row["previous_longitude"],
                                                                                                             row["input_latitude"],
                                                                                                             row["input_longitude"]),
                                                                        axis=1)

        # Calculate distance of raw and clean trace
        # Use float type conversion to avoid numpy.int64 or numpy.float64 data type
        distance_of_raw_trace = round(float((trace_lat_lng_df["dist_from_prev_ping"].sum())), 2)
        distance_of_clean_trace = round(float((self.trace_df["dist_from_prev_ping"].sum())), 2)

        # Calculate percentage reduction in distance. Minimum reduction is 0 in case distance increases (due to interpolation)
        percent_reduction_in_dist = 0 if ((distance_of_raw_trace == 0) or (distance_of_raw_trace <= distance_of_clean_trace)) else (((distance_of_raw_trace - distance_of_clean_trace) / distance_of_raw_trace) * 100)
        percent_reduction_in_dist = round(percent_reduction_in_dist, 2)

        # Create output distance summary
        output_distance_summary = {"cumulative_distance_of_raw_trace": distance_of_raw_trace,
                                   "cumulative_distance_of_clean_trace": distance_of_clean_trace,
                                   "percent_reduction_in_dist": percent_reduction_in_dist}

        return output_distance_summary

    def _create_output_stop_summary(self) -> dict:
        """
        Create output stop summary for trace data. Stop summary contains various metrics about stopping in clean and raw trace.

        Returns:
            dict: A dictionary summarizing stop events metrics. The dictionary has the following keys and values:
                :stop_events_info (list): A list of dictionaries, where each dictionary describes an stop event and has the following keys and values:
                    :stop_event_sequence_number (int): Integer sequence number of the stop event. Each stop event is given a unique non negative integer value.
                    :start_time (str): Timestamp when stop event started. Provided in human readable format (YYYY-MM-DD HH:MM:SS).
                    :end_time (str): Timestamp when stop event ended. Provided in human readable format (YYYY-MM-DD HH:MM:SS).
                    :total_stop_event_time (str): Denotes total time spent in stopping, within the stop event. Provided in human readable format.
                    :number_of_pings (int): Number of pings in the stop event.
                    :representative_latitude (float): Representative latitude of the stop event.
                    :representative_longitude (float): Representative longitude of the stop event.

                :global_stop_events_info (dict): A dictionary describing global stop information and has the following keys and values.
                    :total_trace_time (str): Denotes total time taken in raw trace. Provided in human readable format.
                    :total_stop_events_time (str): Denotes total time spent in stopping, in the entire trace. Provided in human readable format.
                    :stop_events_percentage (float): Percentage of total_stop_events_time w.r.t. total_trace_time.
        """

        # Initialize summary
        summary = {
            "stop_events_info": [],
            "global_stop_events_info": {}}

        df = copy.deepcopy(self.trace_df)

        # Variables to track global stopping metrics
        total_trace_time = df["timestamp"].max()//1000 - df["timestamp"].min()//1000
        total_stop_time, total_stop_pings = 0, 0

        # Get unique stopping sequences (excluding -1 which are non-stopping pings)
        stop_sequences = df[df["stop_event_sequence_number"]!= -1]["stop_event_sequence_number"].unique()

        # Loop through each stop sequence and calculate stop event summary
        for seq in stop_sequences:
            event_data = df[df["stop_event_sequence_number"] == int(seq)]

            # Get start and end time of sequence
            start_time_ms = event_data["timestamp"].min()
            end_time_ms = event_data["timestamp"].max()

            # Get total stop time
            total_event_stop_time = end_time_ms//1000 - start_time_ms//1000
            total_event_stop_time_hr = convert_time_interval_to_human_readable(total_event_stop_time)
            
            # Number of pings in stop event
            num_pings = len(event_data)

            # Get representative latitude, longitude of stop event
            representative_latitude = event_data["representative_stop_event_latitude"].median()
            representative_longitude = event_data["representative_stop_event_longitude"].median()

            # Convert start and end times to human-readable format (UTC)
            start_time_human_hr = convert_unix_timestamp_to_human_readable(start_time_ms // 1000)
            end_time_human_hr = convert_unix_timestamp_to_human_readable(end_time_ms // 1000)

            # Add event level summary to stop events list
            summary["stop_events_info"].append({"stop_event_sequence_number": int(seq),
                                                "start_time": start_time_human_hr,
                                                "end_time": end_time_human_hr,
                                                "total_stop_event_time": total_event_stop_time_hr,
                                                "number_of_pings": num_pings,
                                                "representative_latitude": representative_latitude,
                                                "representative_longitude": representative_longitude})

            # Accumulate global stopping data
            total_stop_time += total_event_stop_time if total_event_stop_time is not None else 0
            total_stop_pings += num_pings if num_pings is not None else 0

        # Global stopping summary
        total_stop_time_hr = convert_time_interval_to_human_readable(total_stop_time)
        summary["global_stop_events_info"] = {"total_trace_time": convert_time_interval_to_human_readable(total_trace_time),
                                              "total_stop_events_time": total_stop_time_hr,
                                              "stop_event_percentage": 0 if total_trace_time == 0 else ((total_stop_time / total_trace_time) * 100) }
        
        return summary

    def get_trace_cleaning_output(self) -> dict:
        """
        Create output with trace data, summary of trace cleaning, summary of distance, summary of time and summary of stopping.

        Returns:
            dict: A dictionary with following keys and values:
                :cleaned_trace (list):
                    A list of dictionaries where each dictionary is a ping in the cleaned trace. Each dictionary in cleaned trace will have the following keys and values:
                        :ping_id (str, optional) : Unique identifier for the current ping.
                            Note: ping_id must be present in either all of the pings or user can choose to not pass it in any of the pings. We will automatically assign a unique string for each ping.
                        :input_latitude (float, None): Original latitude of the ping as provided in the input. Will be None in case the ping is an interpolated ping.
                        :input_longitude (float, None): Original longitude of the ping as provided in the input. Will be None in case the ping is an interpolated ping.
                        :timestamp (int): Unix timestamp for the ping as provided in the input. If the current ping is an interpolated ping then
                                          the timestamp for the current ping will also be between the timestamps of the surrounding two input pings.
                        :error_radius (float, None): Original GPS error radius of the input ping. If the ping has been interpolated then the error_radius value will be None.
                        :event_type (str, None): A string denoting the type of event which occurred at the ping as provided in the input. If no event_type was provided in the input for the current ping or
                                                 if the current ping is interpolated then the value in event_type will be None.
                        :force_retain (bool): Same as in input. If the ping is interpolated then the value will be False.
                        :cleaned_latitude (float, None): Cleaned latitude. If the input ping has been dropped in cleaned trace then the cleaned latitude will be None.
                        :cleaned_longitude (float, None): Cleaned longitude. If the input ping has been dropped in cleaned trace then the cleaned longitude will be None.
                        :update_status (str): A string denoting the updation status of the current ping.
                        :last_updated_by (str): A string denoting the name of function which last updated the ping.
                                                If a user is running a couple of functions over the ping then they can know which function has impacted the ping most recently.
                        :stop_status (bool): Denotes stop status of ping. Defaults to False, updated when add_stop_events_info is called. Remains False if the ping is not a stop ping.
                        :stop_event_sequence_number (int): Denotes the sequence number of stop event, if the current ping is its part.
                                                           Defaults to -1, updated when add_stop_events_info is called. Remains -1 if the ping is not an stop ping.
                        :cumulative_stop_event_time (str): Denotes cumulative stop time in the stop event
                                                             till that ping. Defaults to "0 minutes and 0 seconds", updated when add_stop_events_info is called.
                        :representative_stop_event_latitude (float, None): Denotes the representative latitude of the stop event, if current ping is its part.
                                                                           Defaults to None, updated when add_stop_events_info is called. Remains None if the ping is not a stop ping.
                        :representative_stop_event_longitude (float, None): Denotes the representative longitude of the stop event, if current ping is its part.
                                                                            Defaults to None, updated when add_stop_events_info is called. Remains None if the ping is not a stop ping.
                        :time_since_prev_ping (float): Time passed in milliseconds since previous ping.
                        :dist_from_prev_ping (float): Distance from previous pings in meters.
                        :cleaned_trace_cumulative_dist (float): Distance accumulated till current ping in meters.
                        :cleaned_trace_cumulative_time (float): Time passed in milliseconds till current ping.

                :cleaning_summary (dict):
                    A dictionary summarizing the results of trace cleaning. The dictionary has the following keys and values:
                        :total_pings_in_input (int): Denotes total number of pings in the input.
                        :total_non_null_pings_in_input (int): Denotes total number of non null pings in the input.
                        :total_pings_in_output (int): Denotes total number of non null pings in the output.
                        :total_trace_time (str): Denotes total time of trace in human readable string.
                        :unchanged_percentage (float): The percentage of pings from the input trace whose location is unchanged in the cleaned trace.
                                                       The percentage will be w.r.t the number of non null pings in the input.
                        :drop_percentage (float): The percentage of pings from the input trace which were dropped in cleaned trace.
                        :updation_percentage (float): The percentage of updated pings w.r.t the number of input pings.
                        :interpolation_percentage (float): The percentage of pings which are interpolated w.r.t the number of non null pings in input.
                        :total_execution_time (float): Total time taken in seconds for creating TraceClean object and applying trace cleaning functions (TraceClean methods)

                :distance_summary (dict):
                    A dictionary summarizing distance metrics. The dictionary has the following keys and values:
                        :cumulative_distance_of_raw_trace (float): Denotes total distance covered in raw trace, ignoring pings with Nonetype input_latitude and input_longitude. Unit is meters.
                        :cumulative_distance_of_clean_trace (float): Denotes total distance covered in cleaned trace, ignoring pings with Nonetype cleaned_latitude and cleaned_longitude. Unit is meters.
                        :percent_reduction_in_dist (float): The percentage of reduction in distance in cumulative_distance_of_clean_trace w.r.t. cumulative_distance_of_raw_trace.

                :stop_summary (dict):
                    A dictionary summarizing stop event metrics. The dictionary has the following keys and values:
                        :stop_events_info (list): A list of dictionaries, where each dictionary describes an stop event and has the following keys and values:
                            :stop_event_sequence_number (int): Integer sequence number of the stop event. Each stop event is given a unique non negative integer value.
                            :start_time (str): Timestamp when stop event started. Provided in human readable format (YYYY-MM-DD HH:MM:SS).
                            :end_time (str): Timestamp when stop event ended. Provided in human readable format (YYYY-MM-DD HH:MM:SS).
                            :total_stop_event_time (str): Denotes total time spent in stopping, within the stop event. Provided in human readable format.
                            :number_of_pings (int): Number of pings in the stop event.
                            :representative_latitude (float): Representative latitude of the stop event.
                            :representative_longitude (float): Representative longitude of the stop event.

                        :global_stops_event_info (dict): A dictionary describing global stop information and has the following keys and values.
                            :total_trace_time (str): Denotes total time taken in raw trace. Provided in human readable format.
                            :total_stop_event_time (str): Denotes total time spent in stopping, in the entire trace. Provided in human readable format.
                            :stop_events_percentage (float): Percentage of total_stop_event_time w.r.t. total_trace_time.
        
        Raises:
            All exceptions raised by the following functions:
                :"validate_clean_trace_output" present in output_validation_utils.py.
        """

        trace_cleaning_output = {
            "cleaned_trace": self._create_output_trace(),
            "cleaning_summary": self._create_output_cleaning_summary(),
            "distance_summary": self._create_output_distance_summary(),
            "stop_summary": self._create_output_stop_summary()}
        
        # Validate trace cleaning output
        validate_clean_trace_output(trace_cleaning_output, raw_trace_length=len(self.raw_trace))
        return trace_cleaning_output

    def plot_raw_trace(self) -> fl.Map:
        """
        Provides plot of raw trace.

        Returns:
            folium.Map: Map containing plot of raw trace.
        """

        # Create map
        mean_lat = self.trace_df["input_latitude"].mean()
        mean_lng = self.trace_df["input_longitude"].mean()
        start_coords = [mean_lat, mean_lng]

        map_object = fl.Map(location=start_coords,
                            zoom_start=12,
                            control_scale=True,
                            max_zoom=29)

        # Get trace from output data
        trace_output = self.get_trace_cleaning_output()
        trace = trace_output["cleaned_trace"]

        # Create and return map plot
        return plot_raw_trace_from_trace_output(trace, map_object)

    def plot_clean_trace(self):
        """
        Provides plot of clean trace.

        Returns:
            folium.Map: Map containing plot of clean trace.
        """

        # Create map
        mean_lat = self.trace_df["input_latitude"].mean()
        mean_lng = self.trace_df["input_longitude"].mean()
        start_coords = [mean_lat, mean_lng]

        map_object = fl.Map(location=start_coords,
                            zoom_start=12,
                            control_scale=True,
                            max_zoom=29)

        # Get trace from output data
        trace_output = self.get_trace_cleaning_output()
        trace = trace_output["cleaned_trace"]

        # Create and return map plot
        return plot_clean_trace_from_trace_output(trace, map_object)

    def plot_cleaning_comparison_map(self, 
                                     former_trace, 
                                     latter_trace) -> fl.plugins.DualMap:
        """
        Provides comparison plot of traces. Trace of smaller length (in terms of cleaned_latitude and cleaned_longitude) is plotted on left side of map, whereas the other trace is plotted on right side of map.
        If length of both traces are same then `former_trace` is plotted on left side of map and `latter_trace` is plotted on right side of map.

        Args:
            former_trace (list): First trace to be added in comparison map. It must be of same format as trace data corresponding to "cleaned_trace" key in output trace data obtained using get_trace_cleaning_output method.
            latter_trace (list): Second trace to be added in comparison map. It must be of same format as trace data corresponding to "cleaned_trace" key in output trace data obtained using get_trace_cleaning_output method.

        Raises:
            All exceptions raised by the following functions:
                :"validate_clean_trace_output" present in output_validation_utils.py, while validating data corresponding to to "cleaned_trace" key in trace cleaning output.

        Returns:
            folium.plugins.DualMap: Map containing plot of both traces.
        """

        # validate input traces
        validate_cleaned_trace(former_trace)
        validate_cleaned_trace(latter_trace)

        # Create map
        mean_lat = self.trace_df["input_latitude"].mean()
        mean_lng = self.trace_df["input_longitude"].mean()
        start_coords = [mean_lat, mean_lng]
        map_object = fl.plugins.DualMap(location=start_coords,
                                        zoom_start=15,
                                        control_scale=True,
                                        max_zoom=29)

        # Create and return map plot
        return plot_cleaning_comparison_map_bw_two_traces(
                                        former_trace,
                                        latter_trace,
                                        map_object)

    def plot_raw_vs_stop_comparison_map(self) -> fl.plugins.DualMap:
        """
        Provides comparison plot of raw trace and stopping pings. Raw trace is plotted on left hand side of the map, where as trace formed by stopping points is
        plotted on right hand side of the map.

        Returns:
            folium.plugins.DualMap: Map containing plot of both traces.
        """

        # Create map
        mean_lat = self.trace_df["input_latitude"].mean()
        mean_lng = self.trace_df["input_longitude"].mean()
        start_coords = [mean_lat, mean_lng]
        map_object = fl.plugins.DualMap(location=start_coords,
                                        zoom_start=15,
                                        control_scale=True,
                                        max_zoom=29)

        # Create and return map plot
        trace_output = self.get_trace_cleaning_output()
        trace = trace_output["cleaned_trace"]
        return plot_stop_comparison_map(copy.deepcopy(self.raw_trace), trace, map_object)
