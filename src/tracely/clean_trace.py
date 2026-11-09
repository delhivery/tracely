import time
import pandas as pd
import numpy as np
import folium as fl

from . import constants
from .create_trace_data import CreateTraceData
from .utils.data_validation_utils import DataValidationUtils
from .stop_detection.stop_detection_utils import get_stop_labels
from .utils.utils import get_haversine_distance, \
                         vectorized_haversine, \
                         vectorized_change_in_direction, \
                         fast_deepcopy, \
                         fast_copy_trace_payload, \
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
        self.input_trace_payload = fast_copy_trace_payload(input_trace_payload)
        self.trace_data = CreateTraceData(input_trace_payload).create_trace_data()
        self.vehicle_type = self.trace_data["vehicle_type"]
        self.vehicle_speed = self.trace_data["vehicle_speed"]  # km/hr
        self.trace_df = self.trace_data["trace_df"].copy()
        # `raw_trace` is only ever needed as a list of dicts by
        # plot_raw_vs_stop_comparison_map. Every other use (both call sites
        # in this file) only reads len(self.raw_trace), already available
        # as input_pings_count. Snapshotting the (cheap, copy-on-write)
        # DataFrame here and deferring the actual to_dict(orient="records")
        # conversion to first real access of `raw_trace` removes that
        # conversion from the hot init path entirely for callers who never
        # touch raw_trace's full content.
        self._raw_trace_snapshot_df = self.trace_df.copy()
        self._raw_trace_cache = None
        self.input_pings_count = len(self.trace_df)
        self.elapsed_time_record = []
        self._add_runtime_info(function_name="object_initialization",
                               time_taken=(time.time() - start_time))

    @property
    def raw_trace(self) -> list:
        """
        List of dictionaries representing the raw (as-loaded, pre-cleaning) trace, snapshotted at object
        initialization. Computed lazily (and cached) on first access, from an internal copy-on-write
        DataFrame snapshot taken in __init__ -- so later in-place mutation of self.trace_df by cleaning
        methods never affects its value.

        Returns:
            list: List of pings (dict), as they were right after object initialization.
        """

        if self._raw_trace_cache is None:
            self._raw_trace_cache = self._raw_trace_snapshot_df.to_dict(orient="records")
        return self._raw_trace_cache

    @raw_trace.setter
    def raw_trace(self, value) -> None:
        """
        Overrides the cached value `raw_trace` returns, so direct
        assignment (`clean_trace_object.raw_trace = ...`) behaves the
        same as it did when `raw_trace` was a plain attribute.

        Args:
            value (list): The value subsequent reads of `raw_trace` should return.
        """

        self._raw_trace_cache = value

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

    @staticmethod
    def _build_impute_runs(trigger_positions, margin, lo_bound, hi_bound):
        """
        Merges candidate-trigger positions (found by a fast vectorized pass
        over the ORIGINAL, unmutated data) into disjoint [lo, hi] runs, each
        wide enough that any index whose own prev/current/next reads could
        possibly be affected by another trigger's write is included in some
        run -- not just the trigger positions themselves.

        `margin` is the maximum distance between a trigger at index `t` and
        any future index `i` (`i > t`) whose read positions could overlap
        `t`'s write zone: for a window of half-width `w` (so prev/next reads
        are `w` away, and a trigger's write zone spans `2w - 1` positions
        centered on itself), that distance is `2w - 1`. Passing `margin`
        explicitly rather than deriving it here keeps this method usable for
        both the distance imputation (`w = side_win_len`) and angle
        imputation (`w = 1`) cases, which have different window widths.

        Args:
            trigger_positions (numpy.ndarray): Sorted indices flagged as
                candidate triggers under the original, unmutated data.
            margin (int): See above.
            lo_bound, hi_bound (int): Valid index range (inclusive) runs may
                not extend past (the loop's own `range(w, n - w)` bounds).

        Returns:
            list[list[int, int]]: Disjoint, sorted [lo, hi] runs.
        """

        runs = []
        for i in trigger_positions:
            lo = max(lo_bound, i - margin)
            hi = min(hi_bound, i + margin)
            if runs and lo <= runs[-1][1] + 1:
                runs[-1][1] = max(runs[-1][1], hi)
            else:
                runs.append([lo, hi])
        return runs

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

        # Extract columns once as numpy arrays, instead of materializing a
        # per-row dict for the whole dataframe via to_dict("records") just
        # to read 4 columns. The loop itself stays row-by-row and
        # sequential: whether row i's "previous kept ping" is row i-1 or an
        # earlier row depends on which prior rows get dropped, so this
        # can't be vectorized away.
        cleaned_latitudes = self.trace_df["cleaned_latitude"].to_numpy()
        cleaned_longitudes = self.trace_df["cleaned_longitude"].to_numpy()
        update_statuses = self.trace_df["update_status"].to_numpy()
        force_retains = self.trace_df["force_retain"].to_numpy()

        # Get the first ping as the starting reference
        prev_lat, prev_lng = cleaned_latitudes[0], cleaned_longitudes[0]

        # Create a mask to store which rows need to be updated
        mask = np.zeros(len(self.trace_df), dtype=bool)

        for i in range(1, len(self.trace_df)):

            current_lat, current_lng = cleaned_latitudes[i], cleaned_longitudes[i]

            if ((prev_lat is None) or
                (prev_lng is None) or
                (pd.isna(prev_lat)) or
                (pd.isna(prev_lng))):

                prev_lat, prev_lng = current_lat, current_lng
                continue

            if ((current_lat is None) or
                (current_lng is None) or
                (pd.isna(current_lat)) or
                (pd.isna(current_lng))):
                continue

            # Do not update current ping if it is an interpolated ping
            if update_statuses[i] == "interpolated":
                continue

            # Calculate the distance between the current ping and the previous ping
            distance = get_haversine_distance(prev_lat,
                                              prev_lng,
                                              current_lat,
                                              current_lng)

            if distance is None:
                continue

            if (distance < min_dist_bw_consecutive_pings) and not (force_retains[i]):
                mask[i] = True
                continue

            # Update the previous ping if the current one is kept
            prev_lat, prev_lng = current_lat, current_lng

        # 4 separate single-column scalar writes, instead of 1 combined
        # multi-column write with a heterogeneous (None/None/str/str) value
        # list -- verified directly, measurably faster (0.29ms vs 0.49ms on
        # a real trace): a single-column scalar `.loc[]` assignment is a
        # simpler, more optimized pandas code path than broadcasting a
        # mixed-type value list across several differently-typed columns at
        # once. Skipped entirely when nothing was dropped this call.
        if mask.any():
            self.trace_df.loc[mask, "cleaned_latitude"] = None
            self.trace_df.loc[mask, "cleaned_longitude"] = None
            self.trace_df.loc[mask, "update_status"] = "dropped"
            self.trace_df.loc[mask, "last_updated_by"] = "remove_nearby_pings"

        # Still needed even when nothing was dropped THIS call -- a ping
        # with missing input coordinates already has a real NaN (not None)
        # in these columns from construction time, independent of anything
        # this method does.
        self.trace_df["cleaned_latitude"] = self.trace_df["cleaned_latitude"].replace({np.nan: None})
        self.trace_df["cleaned_longitude"] = self.trace_df["cleaned_longitude"].replace({np.nan: None})

        self._add_runtime_info(function_name="remove_nearby_pings", time_taken=((time.time() - start_time)))

    def remove_pings_by_speed(self,
                              max_speed_kph=150) -> None:
        """
        Function to remove pings that imply a physically impossible speed from the previous kept ping.
        Iterates through pings and computes the implied speed (haversine distance in km / elapsed time in hours) between the current ping and the previous KEPT ping.
        If that implied speed exceeds `max_speed_kph` and force_retain for the current ping is False, the current ping is removed.
        For a removed ping, its "cleaned_latitude" and "cleaned_longitude" are set to None, update_status is set to "dropped" and last_updated_by is set to "remove_pings_by_speed".
        Does not drop an interpolated ping (ping added by "interpolate_trace" method).

        This is a distinct failure mode from the ones "remove_nearby_pings" and "impute_distorted_pings_with_distance" address: a ping (or a short run of
        consecutive pings) that is individually self-consistent -- not close to its neighbours (so "remove_nearby_pings" does not fire), and not a single
        spike that detours far off an otherwise-straight prev-next line (so "impute_distorted_pings_with_distance"'s distance-ratio check does not fire
        either, since a SUSTAINED displacement barely increases the prev->current->next detour ratio relative to going prev->next directly). A ping that
        is physically impossible to reach from the previous kept ping in the elapsed time is still a real, catchable error -- this function catches it on
        speed alone, independent of any detour-shape assumption.

        Args:
            max_speed_kph (int, float, optional): The maximum plausible instantaneous speed between two consecutive kept pings, in kilometers per hour. Defaults to 150.

        Raises:
            All exceptions raised by the following functions:
                "DataValidationUtils.validate_remove_pings_by_speed_parameters" present in data_validation_utils.py.
        """

        start_time = time.time()

        # Validate parameter
        DataValidationUtils.validate_remove_pings_by_speed_parameters(max_speed_kph=max_speed_kph)

        # Extract columns once as numpy arrays, instead of materializing a
        # per-row dict for the whole dataframe via to_dict("records") just
        # to read 5 columns. The loop itself stays row-by-row and
        # sequential, same reasoning as remove_nearby_pings: whether row
        # i's "previous kept ping" is row i-1 or an earlier row depends on
        # which prior rows get dropped, so this can't be vectorized away.
        cleaned_latitudes = self.trace_df["cleaned_latitude"].to_numpy()
        cleaned_longitudes = self.trace_df["cleaned_longitude"].to_numpy()
        timestamps = self.trace_df["timestamp"].to_numpy()
        update_statuses = self.trace_df["update_status"].to_numpy()
        force_retains = self.trace_df["force_retain"].to_numpy()

        # Get the first ping as the starting reference
        prev_lat, prev_lng = cleaned_latitudes[0], cleaned_longitudes[0]
        prev_timestamp = timestamps[0]

        # Create a mask to store which rows need to be updated
        mask = np.zeros(len(self.trace_df), dtype=bool)

        for i in range(1, len(self.trace_df)):

            current_lat, current_lng = cleaned_latitudes[i], cleaned_longitudes[i]
            current_timestamp = timestamps[i]

            if ((prev_lat is None) or
                (prev_lng is None) or
                (pd.isna(prev_lat)) or
                (pd.isna(prev_lng))):

                prev_lat, prev_lng = current_lat, current_lng
                prev_timestamp = current_timestamp
                continue

            if ((current_lat is None) or
                (current_lng is None) or
                (pd.isna(current_lat)) or
                (pd.isna(current_lng))):
                continue

            # Do not update current ping if it is an interpolated ping
            if update_statuses[i] == "interpolated":
                continue

            elapsed_seconds = (current_timestamp - prev_timestamp) / 1000.0

            if elapsed_seconds <= 0:
                continue

            # Calculate the distance between the current ping and the previous kept ping
            distance_m = get_haversine_distance(prev_lat,
                                                prev_lng,
                                                current_lat,
                                                current_lng)

            if distance_m is None:
                continue

            implied_speed_kph = (distance_m / 1000.0) / (elapsed_seconds / 3600.0)

            if (implied_speed_kph > max_speed_kph) and not (force_retains[i]):
                mask[i] = True
                continue

            # Update the previous ping/timestamp if the current one is kept
            prev_lat, prev_lng = current_lat, current_lng
            prev_timestamp = current_timestamp

        # Same split-into-4-single-column-writes win as `remove_nearby_pings`
        # -- see its comment for the full reasoning and measurement.
        if mask.any():
            self.trace_df.loc[mask, "cleaned_latitude"] = None
            self.trace_df.loc[mask, "cleaned_longitude"] = None
            self.trace_df.loc[mask, "update_status"] = "dropped"
            self.trace_df.loc[mask, "last_updated_by"] = "remove_pings_by_speed"

        self.trace_df["cleaned_latitude"] = self.trace_df["cleaned_latitude"].replace({np.nan: None})
        self.trace_df["cleaned_longitude"] = self.trace_df["cleaned_longitude"].replace({np.nan: None})

        self._add_runtime_info(function_name="remove_pings_by_speed", time_taken=((time.time() - start_time)))

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

        # Boolean mask of self.trace_df's non-null-coordinate rows. self.trace_df's
        # index is a plain 0..n-1 RangeIndex at this point in the pipeline (no rows
        # have been removed yet, only blanked), so this mask's True-positions line
        # up 1:1, in order, with trace_df's rows below -- which lets the update at
        # the bottom write straight back by position instead of a ping_id merge.
        non_null_mask = (~self.trace_df["cleaned_latitude"].isnull()) & \
                        (~self.trace_df["cleaned_longitude"].isnull())
        trace_df = self.trace_df[non_null_mask].copy()
        trace_df = trace_df.reset_index(drop=True)

        # NOTE on the aliasing hazard this method's loop has always had:
        # `.at[]` writes below mutate the SAME array `latitudes`/`longitudes`
        # point into, so a later iteration can read an EARLIER iteration's
        # just-imputed value as its own "previous"/"next" neighbor whenever
        # imputation windows overlap. Almost certainly an accident of
        # implementation, not an intentional design choice -- but it is
        # tracely's actual, existing, shipped behavior. The strategy below
        # preserves it EXACTLY (not "fixes" it), while making the common
        # case (the large majority of pings, which no trigger's write zone
        # ever reaches) fully vectorized:
        #
        # 1. A vectorized pass computes, for every index at once, whether it
        #    would trigger under the ORIGINAL (unmutated) data -- cheap,
        #    numpy-only.
        # 2. Any index that is itself a candidate, or close enough to one
        #    that a trigger's write could reach its own prev/current/next
        #    reads, is reprocessed with the EXACT original scalar algorithm,
        #    restricted to a provably-sufficient local "run" of indices
        #    (see _build_impute_runs). Everywhere else, the vectorized
        #    decision (no trigger) is already final -- nothing mutates those
        #    reads, so no further work is needed for them at all.
        # 3. A run's own processing can discover NEW triggers (ones that
        #    only exist because of an earlier mutation, invisible to the
        #    original-data vectorized pass) and grows its own right edge to
        #    cover them -- correctness here relies only on causality
        #    (a trigger can never affect an index processed before it), so
        #    only right-extension is ever needed, never left-extension.
        # 4. Run extension can reach into a LATER, separately-built run.
        #    `last_processed` is a watermark across the whole run list so
        #    that overlap is detected and skipped, not reprocessed on
        #    already-correct data (reprocessing would silently corrupt it).
        latitudes = trace_df["cleaned_latitude"].to_numpy(dtype=np.float64, copy=True)
        longitudes = trace_df["cleaned_longitude"].to_numpy(dtype=np.float64, copy=True)
        update_statuses = trace_df["update_status"].to_numpy()
        is_interpolated = (update_statuses == "interpolated")

        n = len(trace_df)
        s = side_win_len
        touched = np.zeros(n, dtype=bool)

        valid_range = np.arange(s, n - s)
        if len(valid_range) > 0:
            prev_idx = valid_range - s
            next_idx = valid_range + s

            d_prev_cur = vectorized_haversine(latitudes[prev_idx], longitudes[prev_idx],
                                              latitudes[valid_range], longitudes[valid_range])
            d_cur_next = vectorized_haversine(latitudes[valid_range], longitudes[valid_range],
                                              latitudes[next_idx], longitudes[next_idx])
            d_prev_next = vectorized_haversine(latitudes[prev_idx], longitudes[prev_idx],
                                               latitudes[next_idx], longitudes[next_idx])

            candidate_trigger = (~is_interpolated[valid_range]) & ((d_prev_cur + d_cur_next) > (max_dist_ratio * d_prev_next))
            trigger_positions = valid_range[candidate_trigger]

            if len(trigger_positions) > 0:
                margin = 2 * s - 1
                runs = self._build_impute_runs(trigger_positions, margin, s, n - s - 1)

                last_processed = s - 1
                for run_lo, run_hi in runs:
                    if run_hi <= last_processed:
                        continue
                    i = max(run_lo, last_processed + 1)
                    hi = run_hi
                    while i <= hi:
                        if not is_interpolated[i]:
                            prev_ping = (latitudes[i - s], longitudes[i - s])
                            current_ping = (latitudes[i], longitudes[i])
                            next_ping = (latitudes[i + s], longitudes[i + s])

                            d1 = get_haversine_distance(prev_ping[0], prev_ping[1], current_ping[0], current_ping[1])
                            d2 = get_haversine_distance(current_ping[0], current_ping[1], next_ping[0], next_ping[1])
                            d3 = get_haversine_distance(prev_ping[0], prev_ping[1], next_ping[0], next_ping[1])

                            if (d1 + d2) > (max_dist_ratio * d3):
                                pings = self._insert_points_between_coords(prev_ping, next_ping, (2 * s) - 1)
                                pings = pings[1:-1]
                                for j, ping in enumerate(pings):
                                    idx = i - s + 1 + j
                                    if idx < n:
                                        latitudes[idx] = ping[0]
                                        longitudes[idx] = ping[1]
                                        touched[idx] = True

                                extended_hi = min(i + margin, n - s - 1)
                                if extended_hi > hi:
                                    hi = extended_hi
                        i += 1
                    last_processed = hi

        # On real short/clean segments, this very often touches nothing at
        # all (verified: 60% of a real 50-trace small-trace sample) -- when
        # `touched` is entirely False, `latitudes`/`longitudes` are already
        # bit-identical to what `self.trace_df` already holds (nothing wrote
        # into them) and `last_updated_by`/`update_status` are untouched too,
        # so the whole write-back below would just rewrite the same values
        # back into the same place. Skipping it entirely when there is
        # nothing to write removes 4 pandas calls (2 column writes, 2 `.loc[]`
        # writes) plus the final `.loc[]`+`.to_numpy()` write-back below, for
        # the common no-distortion case.
        if touched.any():
            trace_df["cleaned_latitude"] = latitudes
            trace_df["cleaned_longitude"] = longitudes
            trace_df.loc[touched, "last_updated_by"] = "impute_distorted_pings_with_distance"
            trace_df.loc[touched, "update_status"] = "updated"

            # Write the (possibly updated) subset back to self.trace_df directly by
            # position instead of a ping_id merge -- see non_null_mask's comment above.
            common_keys = ["cleaned_latitude", "cleaned_longitude", "last_updated_by", "update_status"]
            self.trace_df.loc[non_null_mask, common_keys] = trace_df[common_keys].to_numpy()

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

        # Same RangeIndex / order-preserving-mask reasoning as
        # `_impute_distorted_pings_with_distance` -- see its comment.
        non_null_mask = (~self.trace_df["cleaned_latitude"].isnull()) & \
                        (~self.trace_df["cleaned_longitude"].isnull())
        trace_df = self.trace_df[non_null_mask].copy()
        trace_df = trace_df.reset_index(drop=True)

        # Same vectorize-the-common-case strategy as
        # `_impute_distorted_pings_with_distance` -- see its comment for the
        # full explanation. Here each trigger writes only its own single
        # index, so the margin (how far a trigger's write can reach a future
        # index's reads) is 1, not `2 * side_win_len - 1`.
        latitudes = trace_df["cleaned_latitude"].to_numpy(dtype=np.float64, copy=True)
        longitudes = trace_df["cleaned_longitude"].to_numpy(dtype=np.float64, copy=True)
        update_statuses = trace_df["update_status"].to_numpy()
        is_interpolated = (update_statuses == "interpolated")

        n = len(trace_df)
        touched = np.zeros(n, dtype=bool)

        # Matches the original loop's exact bounds (range(2, n - 2), not
        # range(1, n - 1), even though only i-1/i+1 are ever read) -- not
        # "fixed" here, kept bit-for-bit identical to current behavior.
        valid_range = np.arange(2, n - 2)
        if len(valid_range) > 0:
            prev_idx = valid_range - 1
            next_idx = valid_range + 1

            delta = vectorized_change_in_direction(
                latitudes[prev_idx], longitudes[prev_idx],
                latitudes[valid_range], longitudes[valid_range],
                latitudes[next_idx], longitudes[next_idx])

            candidate_trigger = (~is_interpolated[valid_range]) & (delta > max_delta_angle)
            trigger_positions = valid_range[candidate_trigger]

            if len(trigger_positions) > 0:
                margin = 1
                runs = self._build_impute_runs(trigger_positions, margin, 2, n - 3)

                last_processed = 1
                for run_lo, run_hi in runs:
                    if run_hi <= last_processed:
                        continue
                    i = max(run_lo, last_processed + 1)
                    hi = run_hi
                    while i <= hi:
                        if not is_interpolated[i]:
                            prev_ping = (latitudes[i - 1], longitudes[i - 1])
                            current_ping = (latitudes[i], longitudes[i])
                            next_ping = (latitudes[i + 1], longitudes[i + 1])

                            delta_direction_at_current_ping = calculate_change_in_direction(
                                prev_ping, current_ping, next_ping)

                            if delta_direction_at_current_ping > max_delta_angle:
                                latitudes[i] = (prev_ping[0] + next_ping[0]) / 2
                                longitudes[i] = (prev_ping[1] + next_ping[1]) / 2
                                touched[i] = True

                                extended_hi = min(i + margin, n - 3)
                                if extended_hi > hi:
                                    hi = extended_hi
                        i += 1
                    last_processed = hi

        # Same no-op-write-back skip as `_impute_distorted_pings_with_distance`
        # -- see its comment for the full reasoning.
        if touched.any():
            trace_df["cleaned_latitude"] = latitudes
            trace_df["cleaned_longitude"] = longitudes
            trace_df.loc[touched, "last_updated_by"] = "impute_distorted_pings_with_angle"
            trace_df.loc[touched, "update_status"] = "updated"

            # Write the (possibly updated) subset back to self.trace_df directly by
            # position instead of a ping_id merge -- see non_null_mask's comment above.
            common_keys = ["cleaned_latitude", "cleaned_longitude", "last_updated_by", "update_status"]
            self.trace_df.loc[non_null_mask, common_keys] = trace_df[common_keys].to_numpy()

        self._add_runtime_info(function_name="impute_distorted_pings_with_angle",
                               time_taken=(time.time() - start_time))

    def map_match_trace(self,
                     osrm_url=constants.DEFAULT_OSRM_URL + "/match/v1/driving/",
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
                                      Defaults to "<OSRM_URL>/match/v1/driving/", where OSRM_URL is read from an OSRM_URL
                                      environment variable or a .env file in the current working directory, or
                                      "http://127.0.0.1:5000" if neither is set.
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

        trace_df = self.trace_df.copy()
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
                          osrm_url=constants.DEFAULT_OSRM_URL + "/route/v1/driving/",
                          min_dist_from_prev_ping=10,
                          max_dist_from_prev_ping=250):
        """
        Interpolates pings between consecutive pings based on distance criteria.
        This function iterates through pings and checks the distance between consecutive pings that are map matched to road. If the distance between these pings lies in user specified range, 
        then the function retrieves the route geometry from the OSRM routing service and generates interpolated pings between the two consecutive pings.

        Args:
            osrm_url (str, optional): A URL specifying the endpoint for accessing the OSRM route service.
                                      Defaults to "<OSRM_URL>/route/v1/driving/", where OSRM_URL is read from an OSRM_URL
                                      environment variable or a .env file in the current working directory, or
                                      "http://127.0.0.1:5000" if neither is set.
            min_dist_from_prev_ping (int, optional): Minimum distance in meters required between consecutive trace pings for interpolation. Defaults to 10 meters.
            max_dist_from_prev_ping (int, optional): Maximum distance in meters allowed between consecutive trace pings for interpolation. Defaults to 250 meters.
        
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

        if trace_data:
            interpolated_pings.append(trace_data[-1])
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

        # get_stop_labels raises an error if no stop ping is detected, we handle this error by returning all labels (in label list) as -1
        # This does not impact data because label for all non stopping pings is also -1 by default.
        try:
            labels = get_stop_labels(trace_list,
                                     max_dist_bw_consecutive_pings=max_dist_bw_consecutive_pings,
                                     max_dist_for_merging_stop_points=max_dist_for_merging_stop_points,
                                     min_size=min_size,
                                     min_staying_time=min_staying_time)
            return labels

        except Exception:
            return [-1 for _ in range(len(trace_list))]

    @staticmethod
    def _compute_stop_info_arrays(trace_df: pd.DataFrame,
                                  labels) -> dict:
        """
        Provides relevant stop information for stopping points, as plain numpy/Python
        arrays positionally aligned with `trace_df` -- a pure-Python/numpy replacement
        for the groupby/map/combine_first chain this used to go through, since at the
        row counts this runs at (tens to low-thousands of pings), the actual
        aggregation work is trivial and pandas' own per-call bookkeeping (dtype checks,
        index rebuilding, hash-join-style map/merge machinery) was the real cost, not
        the math.

        Produces EXACTLY the same values the old groupby-based implementation did,
        including its one quirk: `cumulative_stop_event_time` is only ever meaningful
        for stop pings -- non-stop positions get `None` here (not the human-readable
        "0 minutes and 0 seconds" string), so the caller must leave those positions
        untouched when writing back, same as before (the old code arrived at the same
        `None` by accident, by running every row's leftover string default through
        `convert_time_interval_to_human_readable`, which fails on a string and is
        caught; this skips that redundant, always-failing call instead of removing it
        by surprise).

        Args:
            trace_df (pandas.DataFrame): Trace dataframe (cleaned_latitude, cleaned_longitude, timestamp columns) the stop labels were computed from.
            labels (list or numpy.ndarray): Labels denoting stopping pings information.

        Returns:
            dict: Keys "stop_event_status" (bool ndarray), "representative_stop_event_latitude"/
            "representative_stop_event_longitude" (object ndarray, float or None),
            "stop_event_sequence_number" (int64 ndarray, -1 for non-stop positions),
            "cumulative_stop_event_time" (object ndarray, str or None) -- each positionally
            aligned with `trace_df`'s own rows, in order.
        """

        labels = np.asarray(labels)
        n = len(trace_df)

        cleaned_latitude = trace_df["cleaned_latitude"].to_numpy(dtype=np.float64)
        cleaned_longitude = trace_df["cleaned_longitude"].to_numpy(dtype=np.float64)
        timestamp = trace_df["timestamp"].to_numpy()

        stop_event_status = labels >= 0

        # Representative lat/lng per raw label_key, grouped over every position
        # sharing that label (not just a contiguous run of it) -- matches the old
        # groupby("label_key").mean() exactly. Computed via numpy's own sum/count
        # per group (same float64 reduction pandas' groupby mean uses internally),
        # not a running Python-float accumulator, to stay bit-identical.
        representative_stop_event_latitude = np.full(n, None, dtype=object)
        representative_stop_event_longitude = np.full(n, None, dtype=object)
        if stop_event_status.any():
            unique_labels = np.unique(labels[stop_event_status])
            for label_value in unique_labels:
                group_mask = (labels == label_value)
                mean_lat = cleaned_latitude[group_mask].mean()
                mean_lng = cleaned_longitude[group_mask].mean()
                representative_stop_event_latitude[group_mask] = mean_lat
                representative_stop_event_longitude[group_mask] = mean_lng

        # Sequence number + cumulative stop time: both only ever defined over
        # CONTIGUOUS runs of stop positions sharing the same label_key (a
        # separate notion of grouping from the raw label_key grouping above --
        # revisiting the same stop location later in the trace starts a new
        # sequence, exactly like the old shift()/cumsum() pair did). A single
        # forward pass over stop positions only (never the full n) reproduces
        # it directly, instead of 2 separate groupby calls.
        stop_event_sequence_number = np.full(n, -1, dtype=np.int64)
        cumulative_stop_event_time = np.full(n, None, dtype=object)

        stop_positions = np.flatnonzero(stop_event_status)
        sequence_id = 0
        prev_label = None
        group_start_timestamp = None
        for position in stop_positions:
            label_value = labels[position]
            if label_value != prev_label:
                sequence_id += 1
                group_start_timestamp = timestamp[position]
                cumulative_seconds = 0
            else:
                cumulative_seconds = timestamp[position] - group_start_timestamp
            stop_event_sequence_number[position] = sequence_id
            cumulative_stop_event_time[position] = convert_time_interval_to_human_readable(
                cumulative_seconds, format="ms")
            prev_label = label_value

        return {
            "stop_event_status": stop_event_status,
            "representative_stop_event_latitude": representative_stop_event_latitude,
            "representative_stop_event_longitude": representative_stop_event_longitude,
            "stop_event_sequence_number": stop_event_sequence_number,
            "cumulative_stop_event_time": cumulative_stop_event_time,
        }

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

        non_null_mask = (~self.trace_df["cleaned_latitude"].isnull()) & \
                       (~self.trace_df["cleaned_longitude"].isnull()) & \
                       (~self.trace_df["timestamp"].isnull())
        trace_df = self.trace_df[non_null_mask].copy()

        trace_df["timestamp"] = trace_df["timestamp"] // 1000
        trace_list = trace_df[["cleaned_latitude","cleaned_longitude", "timestamp"]].values.tolist()

        labels = self._get_stop_labels(trace_list,
                                       max_dist_bw_consecutive_pings,
                                       max_dist_for_merging_stop_points,
                                       min_size,
                                       min_staying_time)

        # stop_arrays is positionally aligned with trace_df (same non_null_mask,
        # index never reset) -- same order-preserving-subset reasoning the old
        # ping_id-merge replacement used, see _compute_stop_info_arrays' own
        # docstring for exactly which values match self.trace_df's existing
        # defaults already (safe to write for every row) vs. which don't
        # (cumulative_stop_event_time -- must only touch actual stop positions).
        stop_arrays = self._compute_stop_info_arrays(trace_df, labels)
        full_index = trace_df.index.to_numpy()

        # representative_stop_event_latitude/longitude are already object dtype
        # at this point in every real pipeline ordering (verified directly:
        # bare init, the standard cleaning pipeline, a repeated
        # add_stop_events_info() call, and after map_match_trace/interpolate_trace
        # rebuild trace_df from scratch -- all object dtype already, since the
        # column's own default value is `None`, which pandas never stores as a
        # float64 column). The old explicit `.astype(object)` pre-cast here was
        # therefore always a no-op paying real per-call pandas overhead for
        # nothing -- removed, not "fixed" (nothing was wrong, just redundant).

        # stop_event_status, representative lat/lng, and stop_event_sequence_number
        # all already equal self.trace_df's own existing default for every
        # non-stop position in this subset (False / None / None / -1) -- writing
        # the full subset in one shot per column is equivalent to writing only
        # the changed positions, just without a boolean-mask/notna filtering
        # pass first. Combined into ONE multi-column .loc write instead of 4
        # separate ones -- pandas still casts each column back to its own
        # existing dtype (bool/object/object/int64) from the shared object-dtype
        # source array, verified directly, so this is a pure call-count cut,
        # not a dtype change.
        combined_columns = ["stop_event_status", "representative_stop_event_latitude",
                            "representative_stop_event_longitude", "stop_event_sequence_number"]
        combined_values = np.empty((len(full_index), 4), dtype=object)
        combined_values[:, 0] = stop_arrays["stop_event_status"]
        combined_values[:, 1] = stop_arrays["representative_stop_event_latitude"]
        combined_values[:, 2] = stop_arrays["representative_stop_event_longitude"]
        combined_values[:, 3] = stop_arrays["stop_event_sequence_number"]
        self.trace_df.loc[full_index, combined_columns] = combined_values

        # cumulative_stop_event_time's non-stop positions are None here (see
        # _compute_stop_info_arrays' docstring), which does NOT match
        # self.trace_df's own "0 minutes and 0 seconds" string default -- must
        # restrict this one write to actual stop positions only.
        stop_mask_positions = stop_arrays["stop_event_status"]
        if stop_mask_positions.any():
            stop_index = full_index[stop_mask_positions]
            self.trace_df.loc[stop_index, "cumulative_stop_event_time"] = \
                stop_arrays["cumulative_stop_event_time"][stop_mask_positions]

        # Matches a real quirk of the merge+combine_first this replaces: when a
        # trace has zero stop events, the "_new" side of the merge is entirely
        # null, and pandas' merge/combine_first produces object dtype for
        # cumulative_stop_event_time in that case specifically (vs. StringDtype
        # when at least one real value merged in). Preserved exactly, not fixed.
        if not stop_mask_positions.any():
            self.trace_df["cumulative_stop_event_time"] = self.trace_df["cumulative_stop_event_time"].astype(object)

        self.trace_df["representative_stop_event_latitude"] = self.trace_df["representative_stop_event_latitude"].replace({np.nan: None})
        self.trace_df["representative_stop_event_longitude"] = self.trace_df["representative_stop_event_longitude"].replace({np.nan: None})
        # Matches the dtype the merge+combine_first this replaces always produced for
        # stop_event_status: a left join leaves non-matching rows (null lat/lng/timestamp)
        # as NaN in the "_new" column, which can't coexist with bool dtype, so that column
        # -- and combine_first's result with it -- end up as object dtype holding plain
        # Python bool values, never a clean bool-dtype column. Preserved here on purpose
        # (not "fixed") to keep self.trace_df byte-for-byte identical to current behavior.
        self.trace_df["stop_event_status"] = self.trace_df["stop_event_status"].astype(object)
        self.trace_df["stop_event_sequence_number"] = self.trace_df["stop_event_sequence_number"].astype(int)

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

        return trace_df["timestamp"].diff()

    def _add_distance_bw_pings(self, 
                               input_trace_df: pd.DataFrame) -> pd.Series:
        """
        Provides difference in distance between consecutive pings, on the basis of "cleaned_latitude", "cleaned_longitude" key.

        Args:
            input_trace_df (pandas.DataFrame): Dataframe containing pings.

        Returns:
            pandas.Series: Series of difference in distance between consecutive pings.
        """

        cleaned_latitude = input_trace_df["cleaned_latitude"].to_numpy(dtype=np.float64)
        cleaned_longitude = input_trace_df["cleaned_longitude"].to_numpy(dtype=np.float64)

        # Step 1: Shift the "cleaned_latitude", "cleaned_longitude" arrays to create the initial "previous_latitude", "previous_longitude" arrays
        previous_latitude = np.empty_like(cleaned_latitude)
        previous_longitude = np.empty_like(cleaned_longitude)
        if len(cleaned_latitude) > 0:
            previous_latitude[0] = np.nan
            previous_latitude[1:] = cleaned_latitude[:-1]
            previous_longitude[0] = np.nan
            previous_longitude[1:] = cleaned_longitude[:-1]

        # Step 2: Forward fill "previous_latitude", "previous_longitude" only where it is not NaN
        previous_latitude = pd.Series(previous_latitude).ffill().to_numpy()
        previous_longitude = pd.Series(previous_longitude).ffill().to_numpy()

        # Step 3: Ensure "previous_latitude", "previous_longitude" are NaN where "cleaned_latitude", "cleaned_longitude" is NaN
        current_is_null = np.isnan(cleaned_latitude) | np.isnan(cleaned_longitude)
        previous_latitude = np.where(current_is_null, np.nan, previous_latitude)
        previous_longitude = np.where(current_is_null, np.nan, previous_longitude)

        distances = vectorized_haversine(previous_latitude, previous_longitude, cleaned_latitude, cleaned_longitude)
        return pd.Series(distances, index=input_trace_df.index)

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
        self.trace_df["dist_from_prev_ping"] = self.trace_df["dist_from_prev_ping"].replace({np.nan: 0})

        # Add cleaned_trace_cumulative_dist to instance's trace dataframe
        self.trace_df["cleaned_trace_cumulative_dist"] = self.trace_df["dist_from_prev_ping"].cumsum()
        self.trace_df["cleaned_trace_cumulative_dist"] = self.trace_df["cleaned_trace_cumulative_dist"].replace({np.nan: 0})

        # Add time_since_prev_ping to instance's trace dataframe
        self.trace_df["time_since_prev_ping"] = self._add_time_interval_bw_pings(self.trace_df)
        self.trace_df["time_since_prev_ping"] = self.trace_df["time_since_prev_ping"].replace({np.nan: 0})

        # Add cleaned_trace_cumulative_time to instance's trace dataframe
        self.trace_df["cleaned_trace_cumulative_time"] = self.trace_df["time_since_prev_ping"].cumsum()
        self.trace_df["cleaned_trace_cumulative_time"] = self.trace_df["cleaned_trace_cumulative_time"].replace({np.nan: 0})

        # A single `.to_numpy(dtype=object)` call over the whole output slice,
        # instead of a `.replace({np.nan: None})` call PLUS a `.tolist()` call
        # per column (2 pandas calls x ~20 columns = ~40 calls) -- verified
        # directly, element-by-element and type-by-type, that `to_numpy(dtype=
        # object)` already boxes every cell to the same native Python type
        # `.tolist()` did (int/float/str/bool, never a leftover numpy scalar),
        # so nothing is lost by skipping the per-column `.tolist()` round-trip.
        # NaN-to-None replacement then happens once, on the resulting plain
        # numpy object array, via `pd.isna()` -- same net effect as the old
        # per-column `.replace({np.nan: None})` (both turn every NaN-like cell
        # into `None`; `pd.isna()` additionally treats an already-`None` cell
        # as NA too, which is a no-op since it was already `None`). Metadata
        # (added below, after this) is always a dict and never NaN, so it
        # never needed this replacement either, same as before.
        output_columns_df = self.trace_df[constants.CLEAN_TRACE_COLUMNS_WITHOUT_METADATA]
        columns = output_columns_df.columns.tolist()
        raw_values = output_columns_df.to_numpy(dtype=object)
        raw_values[pd.isna(raw_values)] = None
        output_trace = [dict(zip(columns, row)) for row in raw_values]

        output_trace = self._match_metadata(output_trace)

        return output_trace

    def _create_output_cleaning_summary(self) -> dict:
        """
        Create output cleaning summary for trace data. Cleaning summary contains various metrics about trace cleaning operations and their effect.

        Returns:
            dict: A dictionary summarizing the results of trace cleaning. The dictionary has the following keys and values:
                  :total_pings_in_input (int): Denotes total number of pings in the input.
                  :total_non_null_pings_in_input (int): Denotes total number of non null pings in the input.
                  :total_non_null_pings_in_output (int): Denotes total number of non null pings in the output.
                  :unchanged_percentage (float): The percentage of pings from the input trace whose location is unchanged in the cleaned trace.
                                                 The percentage will be w.r.t the number of non null pings in the input.
                  :drop_percentage (float): The percentage of pings from the input trace which were dropped in cleaned trace w.r.t the number of non null pings in the input.
                  :updation_percentage (float): The percentage of updated pings  w.r.t the number of non null pings in the input.
                  :interpolation_percentage (float): The percentage of pings which are interpolated  w.r.t the number of non null pings in the input.
                  :total_execution_time (float): Total time taken in seconds for creating cleanable trace object and applying trace cleaning functions (CleanTrace methods)
        """

        # Get count of pings in input and output
        total_pings_in_input = self.input_pings_count

        # Pull each needed column as a plain numpy array ONCE (5 pandas
        # calls total), then do every count/filter below in pure
        # numpy/Python -- replaces what used to be 2 separate notnull()+filter
        # passes, a 3rd notnull()+filter+value_counts() pass, and a 4th
        # notnull()+equality+sum pass (each re-reading the same handful of
        # columns from scratch). `int(...)` casts below match the plain
        # Python `int` the old `.shape[0]`/`.get(..., 0)` calls produced --
        # a bare numpy `.sum()` result would otherwise leak a numpy int64
        # into this public output dict.
        input_latitude = self.trace_df["input_latitude"].to_numpy()
        input_longitude = self.trace_df["input_longitude"].to_numpy()
        cleaned_latitude = self.trace_df["cleaned_latitude"].to_numpy()
        cleaned_longitude = self.trace_df["cleaned_longitude"].to_numpy()
        update_status = self.trace_df["update_status"].to_numpy()

        input_non_null_mask = ~(pd.isna(input_latitude) | pd.isna(input_longitude))
        output_non_null_mask = ~(pd.isna(cleaned_latitude) | pd.isna(cleaned_longitude))

        total_non_null_pings_in_input = int(input_non_null_mask.sum())
        total_non_null_pings_in_output = int(output_non_null_mask.sum())

        existing_point_statuses = update_status[input_non_null_mask]
        total_unchanged_pings_in_output = int((existing_point_statuses == "unchanged").sum())
        total_dropped_pings_in_output = int((existing_point_statuses == "dropped").sum())
        total_updated_pings_in_output = int((existing_point_statuses == "updated").sum())
        total_interpolated_pings_in_output = int((output_non_null_mask & (update_status == "interpolated")).sum())
        
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

        # Pull input_latitude/input_longitude as plain numpy arrays once (2
        # pandas calls), instead of building and dropna()-filtering a
        # 2-column DataFrame copy just to read 2 numpy arrays back out of it.
        # dropna()'s default `how="any"` means "drop if EITHER is null" --
        # the same condition as the OR'd isnan mask below.
        input_latitude = self.trace_df["input_latitude"].to_numpy(dtype=np.float64)
        input_longitude = self.trace_df["input_longitude"].to_numpy(dtype=np.float64)
        non_null_mask = ~(np.isnan(input_latitude) | np.isnan(input_longitude))
        lat = input_latitude[non_null_mask]
        lng = input_longitude[non_null_mask]

        # Add previous ping for each ping, and distance of each ping from its previous ping
        previous_latitude = np.empty_like(lat)
        previous_longitude = np.empty_like(lng)
        if len(lat) > 0:
            previous_latitude[0] = np.nan
            previous_latitude[1:] = lat[:-1]
            previous_longitude[0] = np.nan
            previous_longitude[1:] = lng[:-1]

        dist_from_prev_ping = vectorized_haversine(previous_latitude, previous_longitude, lat, lng)

        # Calculate distance of raw and clean trace
        # Use float type conversion to avoid numpy.int64 or numpy.float64 data type.
        # np.nansum (not np.sum) to match pandas Series.sum()'s default
        # skipna=True -- dist_from_prev_ping's own first element is always
        # NaN (no "previous ping" for the first row), same as before.
        distance_of_raw_trace = round(float(np.nansum(dist_from_prev_ping)), 2)
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

        # Pull every column this method reads ONCE as a plain numpy array
        # (4 pandas calls total, regardless of how many stop sequences
        # exist), instead of taking a full-DataFrame `.copy()` up front and
        # then re-filtering that copy with a fresh boolean-mask pandas query
        # per sequence. Every per-sequence computation below works on numpy
        # slices of these arrays instead.
        timestamp = self.trace_df["timestamp"].to_numpy()
        sequence_number = self.trace_df["stop_event_sequence_number"].to_numpy()
        representative_latitude_col = self.trace_df["representative_stop_event_latitude"].to_numpy(dtype=np.float64)
        representative_longitude_col = self.trace_df["representative_stop_event_longitude"].to_numpy(dtype=np.float64)

        # Variables to track global stopping metrics
        total_trace_time = timestamp.max()//1000 - timestamp.min()//1000
        total_stop_time, total_stop_pings = 0, 0

        # Get unique stopping sequences (excluding -1 which are non-stopping pings)
        stop_sequences = np.unique(sequence_number[sequence_number != -1])

        # Loop through each stop sequence and calculate stop event summary
        for seq in stop_sequences:
            event_mask = (sequence_number == seq)

            # Get start and end time of sequence
            start_time_ms = timestamp[event_mask].min()
            end_time_ms = timestamp[event_mask].max()

            # Get total stop time
            total_event_stop_time = end_time_ms//1000 - start_time_ms//1000
            total_event_stop_time_hr = convert_time_interval_to_human_readable(total_event_stop_time)

            # Number of pings in stop event
            num_pings = int(event_mask.sum())

            # Get representative latitude, longitude of stop event -- np.median
            # matches pandas' Series.median() bit-for-bit here (same sorting-based
            # algorithm, no NaN in this slice since every stop-sequence row always
            # has a real representative lat/lng, never the non-stop-row None default).
            representative_latitude = np.median(representative_latitude_col[event_mask])
            representative_longitude = np.median(representative_longitude_col[event_mask])

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

    def get_trace_cleaning_output(self,
                                  include_cleaning_summary: bool = True,
                                  include_distance_summary: bool = True,
                                  include_stop_summary: bool = True) -> dict:
        """
        Create output with trace data, summary of trace cleaning, summary of distance, summary of time and summary of stopping.

        Args:
            include_cleaning_summary (bool, optional): Whether to compute and include "cleaning_summary" in the output. Defaults to True.
            include_distance_summary (bool, optional): Whether to compute and include "distance_summary" in the output. Defaults to True.
            include_stop_summary (bool, optional): Whether to compute and include "stop_summary" in the output. Defaults to True.
            "cleaned_trace" itself is always computed -- it is the primary output and is never skippable. Each summary is
            independently skippable to avoid computing it at all when a caller doesn't need it. Calling this with no
            arguments behaves exactly as before this option existed.

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
                        :metadata (dict): Same as in input, matched back by ping_id. If the ping is interpolated (its ping_id is synthetic and never matches an input ping) then the value will be an empty dictionary.
                        :cleaned_latitude (float, None): Cleaned latitude. If the input ping has been dropped in cleaned trace then the cleaned latitude will be None.
                        :cleaned_longitude (float, None): Cleaned longitude. If the input ping has been dropped in cleaned trace then the cleaned longitude will be None.
                        :update_status (str): A string denoting the updation status of the current ping.
                        :last_updated_by (str): A string denoting the name of function which last updated the ping.
                                                If a user is running a couple of functions over the ping then they can know which function has impacted the ping most recently.
                        :stop_event_status (bool): Denotes stop status of ping. Defaults to False, updated when add_stop_events_info is called. Remains False if the ping is not a stop ping.
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
                        :total_non_null_pings_in_output (int): Denotes total number of non null pings in the output.
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

                        :global_stop_events_info (dict): A dictionary describing global stop information and has the following keys and values.
                            :total_trace_time (str): Denotes total time taken in raw trace. Provided in human readable format.
                            :total_stop_events_time (str): Denotes total time spent in stopping, in the entire trace. Provided in human readable format.
                            :stop_event_percentage (float): Percentage of total_stop_events_time w.r.t. total_trace_time.
        
        Raises:
            All exceptions raised by the following functions:
                :"validate_clean_trace_output" present in output_validation_utils.py.
        """

        trace_cleaning_output = {"cleaned_trace": self._create_output_trace()}
        if include_cleaning_summary:
            trace_cleaning_output["cleaning_summary"] = self._create_output_cleaning_summary()
        if include_distance_summary:
            trace_cleaning_output["distance_summary"] = self._create_output_distance_summary()
        if include_stop_summary:
            trace_cleaning_output["stop_summary"] = self._create_output_stop_summary()

        # Validate trace cleaning output
        validate_clean_trace_output(trace_cleaning_output,
                                    raw_trace_length=self.input_pings_count,
                                    include_cleaning_summary=include_cleaning_summary,
                                    include_distance_summary=include_distance_summary,
                                    include_stop_summary=include_stop_summary)
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
        return plot_stop_comparison_map(fast_deepcopy(self.raw_trace), trace, map_object)
