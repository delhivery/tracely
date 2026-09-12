# Functions in Tracely

Scoped to `CleanTrace`'s own methods. For the trace payload shape these
methods operate on, see `INPUT_REFERENCE.md`; for `get_trace_cleaning_output`'s
return shape, see `OUTPUT_REFERENCE.md`; for every error code/message, see
`ERROR_REFERENCE.md`.

## 1. Tracely has following functions for cleaning trace data:

### 1.1 Remove nearby pings

#### Description
The `remove_nearby_pings` function removes pings that are too close to their previous ping based on a specified minimum distance threshold. It iterates through the trace pings and checks if the haversine distance between the current and previous trace ping is less than this threshold. If the distance is below the threshold and the `force_retain` flag is `False`, the current trace ping is marked for removal. The function also updates specific fields for removed pings.

#### Parameters
- **min_dist_bw_consecutive_pings** (`int`, `float`, optional): The minimum distance threshold (in meters) between consecutive pings. Defaults to `5`.

#### Behavior
- If current ping is removed then:
  - The `cleaned_latitude` and `cleaned_longitude` of the current ping are set to `None`.
  - The `update_status` is set to `dropped`.
  - The `last_updated_by` is set to `"remove_nearby_pings"`.
- Interpolated pings (pings added by the `interpolate_trace` method) are not dropped.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_object.remove_nearby_pings(min_dist_bw_consecutive_pings=10)
```

### 1.2 Remove pings by speed

#### Description
The `remove_pings_by_speed` function removes pings that imply a physically impossible speed from the previous KEPT ping. It iterates through pings and computes the implied speed (haversine distance in km / elapsed time in hours) between the current ping and the previous ping that was itself kept (not one already dropped by an earlier step). If that implied speed exceeds `max_speed_kph` and the current ping's `force_retain` is `False`, the current ping is dropped.

This catches a failure mode `remove_nearby_pings` and `impute_distorted_pings_with_distance`/`impute_distorted_pings_with_angle` both miss: a ping, or a sustained run of consecutive pings, that is individually self-consistent — not close enough to its neighbours to trip the nearby-ping check, and not a sharp enough single-point detour to trip the distance/angle-ratio checks (a SUSTAINED displacement across several consecutive pings barely raises those ratios). A ping that is physically impossible to reach from the previous kept ping in the elapsed time is still a real, catchable error — this function catches it on implied speed alone, independent of any detour-shape assumption.

#### Parameters
- **max_speed_kph** (`int`, `float`, optional): The maximum plausible instantaneous speed between two consecutive kept pings, in kilometers per hour. Defaults to `150`.

#### Behavior
- If the current ping is removed then:
  - The `cleaned_latitude` and `cleaned_longitude` of the current ping are set to `None`.
  - The `update_status` is set to `dropped`.
  - The `last_updated_by` is set to `"remove_pings_by_speed"`.
- Interpolated pings (pings added by the `interpolate_trace` method) are not dropped, and are not used as the "previous kept ping" reference for the pings after them.
- `force_retain=True` on the current ping prevents it from being dropped, same override contract as `remove_nearby_pings`.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_object.remove_pings_by_speed(max_speed_kph=120)
```

### 1.3 Impute distorted pings with distance

#### Description
The `impute_distorted_pings_with_distance` function imputes distorted pings based on distance-based criteria. It iterates through pings and calculates the pairwise Haversine distance between the current ping and its surrounding pings. If the ratio of the sum of distances from the current ping to the surrounding pings, compared to the distance between the surrounding pings, exceeds the specified `max_dist_ratio`, the current ping is replaced with the mean of its surrounding pings.

#### Parameters
- **max_dist_ratio** (`int`, `float`, optional): The maximum allowable ratio for the distance comparison. If the ratio exceeds this value, imputation occurs. Defaults to `3`.

#### Behavior
- If current ping is removed then:
  - The `cleaned_latitude` and `cleaned_longitude` of the current ping are updated.
  - The `update_status` is set to `updated`.
  - The `last_updated_by` is set to `"impute_distorted_pings_with_distance"`.
- Interpolated pings (pings added by the `interpolate_trace` method) are not updated.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_object.impute_distorted_pings_with_distance(max_dist_ratio=4)
```

### 1.4 Impute distorted pings with angle

#### Description
The `impute_distorted_pings_with_angle` function imputes distorted pings based on change in direction. It iterates through consecutive pings and calculates the change in direction at the current ping with respect to its adjacent pings (measured in degrees, with a maximum value of 180 degrees). If the change in direction exceeds the specified `max_delta_angle`, the current ping is replaced with the mean of its previous and next pings.

#### Parameters
- **max_delta_angle** (`int`, `float`, optional): The threshold for the maximum allowable change in direction at a ping. If the change exceeds this value, imputation occurs. Defaults to `120` degrees.

#### Behavior
- If current ping is removed then:
  - The `cleaned_latitude` and `cleaned_longitude` of the current ping are updated.
  - The `update_status` is set to `updated`.
  - The `last_updated_by` is set to `"impute_distorted_pings_with_angle"`.
- Interpolated pings (pings added by the `interpolate_trace` method) are not updated.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_object.impute_distorted_pings_with_angle(max_delta_angle=150)
```

### 1.5 Map match trace

#### Description
The `map_match_trace` function maps pings to the nearest roads using a specified OSRM (Open Source Routing Machine) server. The function processes pings in batches, as defined by the `ping_batch_size` parameter. It is important to note that the batch size should not exceed 100 unless the OSRM server is configured to handle larger requests.

#### Parameters
- **osrm_url** (`str`, optional): A URL that specifies the endpoint for accessing the map matching service provided by an OSRM instance. Defaults to `"{OSRM_URL}/match/v1/driving/"`, where `OSRM_URL` is read from an `OSRM_URL` environment variable or a `.env` file in the current working directory, or `"http://127.0.0.1:5000"` if neither is set.
- **ping_batch_size** (`int`, optional): The size of the batch of pings that will be map matched. Must be `>= 2`. Defaults to `5`.
- **map_matching_radius** (`int`, `float`, optional): Radius in meters for map matching — a location is map matched only if there is a road within this radius. Defaults to `20`.
- **avg_snap_distance** (`int`, `float`, optional): Average snap distance in meters allowed for all points in a segment. Defaults to `12`.
- **max_matched_dist_to_raw_dist_ratio** (`int`, `float`, optional): For two consecutive pings, the maximum allowed ratio of (distance between the map-matched points) to (distance between the raw points). Defaults to `1.3`.

#### Behavior
- If current ping is removed then:
  - The `cleaned_latitude` and `cleaned_longitude` of the current ping are updated.
  - The `update_status` is set to `updated`.
  - The `last_updated_by` is set to `map_match_trace`.
- Interpolated pings (pings added by the `interpolate_trace` method) are not updated.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_object.map_match_trace(osrm_url="http://127.0.0.1:5000/match/v1/driving/", ping_batch_size=10)
```


### 1.6 Interpolate trace

#### Description
The `interpolate_trace` function generates interpolated pings based on the criteria of distance between pings. If the distance between two consecutive pings is within user specified range, then the function attempts to interpolate the pings between those two consecutive pings.<br>Apply map_match_trace in advance as interpolation depends on map matching.

#### Parameters
- **osrm_url** (`str`, optional): A URL that specifies the endpoint for accessing the route service provided by an OSRM instance. Defaults to `"{OSRM_URL}/route/v1/driving/"`, where `OSRM_URL` is read from an `OSRM_URL` environment variable or a `.env` file in the current working directory, or `"http://127.0.0.1:5000"` if neither is set.
- **min_dist_from_prev_ping** (`int`, `float`, optional): Minimum distance in meters required between two consecutive map-matched pings for interpolation between them to be attempted. Must be `> 0` and less than `max_dist_from_prev_ping`. Defaults to `10`.
- **max_dist_from_prev_ping** (`int`, `float`, optional): Maximum distance in meters allowed between two consecutive map-matched pings for interpolation between them to be attempted. Must be `> 0`. Defaults to `250`.

#### Behavior
- Interpolated pings have their `cleaned_latitude` and `cleaned_longitude` updated with interpolated values.
- The `update_status` is set to "interpolated" and `last_updated_by` is set to "interpolate_trace".
- Fields such as `input_latitude`, `input_longitude`, `error_radius`, and `event_type` are set to `None`.
- The `timestamp` of interpolated pings is determined from the surrounding pings.
- Each interpolated ping receives a unique `ping_id`. . If the current ping is lets say nth interpolated ping between two consecutive ping_ids like `ping_id_i` and `ping_id_j` then the `ping_id` for this ping will be `ping_id_i_n`.
- The `force_retain` flag is set to `False`.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_object.interpolate_trace()
```


## 2. Tracely has following function for detecting stop events from trace data:

### 2.1 Stop events detection

#### Description
The `add_stop_events_info` function detects stop events from trace data, using infostop open source library.

#### Parameters
- **max_dist_bw_consecutive_pings** (`int`, `float`, optional): Max distance between time-consecutive pings to label them as part of a stop event. Defaults to `10`.
- **max_dist_for_merging_stop_points** (`int`, `float`, optional): Max distance between two representative stop event locations to form an edge (merge two stop events). Defaults to `0.001`.
- **min_staying_time** (`int`, optional): The shortest duration in seconds that can constitute a stop event. Defaults to `120`.
- **min_size** (`int`, optional): Minimum size of the group of pings that can constitute a stop event. Defaults to `2`.

#### Behavior
- If current ping is marked as part of a stop event then:
  - The `stop_event_status`, `representative_stop_event_latitude`, `representative_stop_event_longitude`, `cumulative_stop_event_time`, `stop_event_sequence_number` of the current ping are updated.
  - The `stop_event_status` is set to `True`.
  - The `representative_stop_event_latitude`, `representative_stop_event_longitude` are set to the representative latitude and longitude of the stop event to which the current ping belongs.
  - The `stop_event_sequence_number` is set to a positive integer, which represents the stop event of which the current pings is a part.
  - The `cumulative_stop_event_time` is a string that represents time accumulated till current ping for the concerned stop event, in minutes and seconds.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_obj.add_stop_events_info()
```


## 3. Tracely has following functions related to plotting trace(s):

### 3.1 Plotting raw trace

#### Description
The `plot_raw_trace` provides plot of raw trace. Plotted trace contains 10 layers with each layer containing 10% pings.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
raw_trace_map = clean_trace_obj.plot_raw_trace()
```

### 3.2 Plotting clean trace

#### Description
The `plot_clean_trace` provides plot of clean trace (latest trace after whatever cleaning operations have been applied). Plotted trace contains 10 layers with each layer containing 10% pings.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
clean_trace_map = clean_trace_obj.plot_clean_trace()
```

### 3.3 Plotting dual map for comparing raw trace with stop event points

#### Description
The `plot_raw_vs_stop_comparison_map` provides plot of raw trace and trace formed by representative locations for stop events. Trace on each side of dual map plot contains 10 layers with each layer containing 10% pings.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
raw_vs_stop_map = clean_trace_obj.plot_raw_vs_stop_comparison_map()
```

### 3.4 Plotting dual map for comparing two traces

#### Description
The `plot_cleaning_comparison_map` provides plot of two traces obtained from Tracely's output. Trace on each side of dual map plot contains 10 layers with each layer containing 10% pings.

#### Parameters
- **former_trace** (list): First trace to be added in comparison map. It must be of same format as trace data corresponding to "cleaned_trace" key in output trace data obtained using get_trace_cleaning_output method.
- **latter_trace** (list): Second trace to be added in comparison map. It must be of same format as trace data corresponding to "cleaned_trace" key in output trace data obtained using get_trace_cleaning_output method.

#### Behavior
- If length of former_trace is less than or equal to length of latter_trace:
  - former_trace is plotted on left, latter_trace is plotted on right.
- If length of former_trace is greater length of latter_trace:
  - latter_trace is plotted on left, former_trace is plotted on right.
- In short, trace with lesser pings is plotted on left.
- For any two pings in former_trace and latter_trace, if their `ping_id` is same, their index will also be same in both maps.

#### Example
```python
# Assuming you have an instance of Tracely's CleanTrace as clean_trace_object
# Plotting raw and clean trace for comparison
raw_output = clean_trace_obj.get_trace_cleaning_output()
raw_trace = raw_output["cleaned_trace"]
# Apply some cleaning functions
clean_output = clean_trace_obj.get_trace_cleaning_output()
clean_trace = clean_output["cleaned_trace"]
# Plot dual map for raw and clean trace
clean_trace_obj.plot_cleaning_comparison_map(raw_trace, clean_trace)
```

