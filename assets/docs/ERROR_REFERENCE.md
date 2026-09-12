# Error Reference

Every exception tracely raises carries `.to_dict()` → `{"error_message": ..., "status_code": ...}`.

```python
from tracely.exceptions.custom_exceptions import ValidationException

try:
    clean_trace_object.remove_nearby_pings(min_dist_bw_consecutive_pings=-5)
except ValidationException as error:
    print(error.to_dict())
    # {'error_message': 'min_dist_bw_consecutive_pings cannot be negative', 'status_code': 4003}
```

## Exception classes and status codes

| Exception | Code | Category | When |
|---|---|---|---|
| `ValidationException` | `4001` | `KEY_ERROR_EXCEPTION_CODE` / `INPUT_KEY_ERROR_EXCEPTION_CODE` | A required key is missing, an unexpected key is present, or a `metadata` dict has a non-string key. |
| `ValidationException` | `4002` | `DATA_FORMAT_EXCEPTION_CODE` | A field or parameter is the wrong Python type. |
| `ValidationException` | `4003` | `VALUE_EXCEPTION_CODE` | A field or parameter has an invalid value (negative, zero, out of range, duplicate, empty string, etc). |
| `ValidationException` | `4004` | `INVALID_TIME_EXCEPTION_CODE` | A `timestamp` is outside `[0, 2145916800000]`. |
| `ValidationException` | `4005` | `INVALID_COORDS_EXCEPTION_CODE` | A latitude/longitude is outside its valid range. |
| `InputOutputException` | `1001` | `FILE_READ_ERROR_CODE` | `convert_csv_to_trace_payload` couldn't read the CSV at the given path. |
| `InputOutputException` | `1002` | `MANDATORY_COLUMN_ERROR_CODE` | `convert_csv_to_trace_payload`'s CSV is missing `latitude`/`longitude`/`timestamp`. |
| `OSRMException` | `2001` | `CONNECTION_ERROR_CODE` | `map_match_trace` or `interpolate_trace` couldn't reach the OSRM server. |

**Valid timestamp range**: `[0, 2145916800000]` ms (1 Jan 1970 UTC through 1 Jan 2038 00:00:00 UTC).

---

## Input payload validation

### Structural (4001 / 4002)

| Condition | Message | Code |
|---|---|---|
| Trace payload isn't a dict | `{name} must be of type Dict but found {type}` | 4002 |
| Trace payload is an empty dict | `{name} cannot be an empty dictionary` | 4002 |
| `"trace"` key missing | `Expected key: 'trace' missing from the dictionary` | 4001 |
| Payload has a key other than `trace`/`vehicle_type`/`vehicle_speed` | `Unexpected key provided in trace payload dictionary` | 4001 |
| `trace` isn't a list | `trace must be of type List but found {type}` | 4002 |
| `trace` is an empty list | `trace cannot be an empty list` | 4002 |
| `vehicle_type` isn't a `str` | `vehicle_type must be of type String but found {type}` | 4002 |
| `vehicle_speed` isn't `int`/`float` | `vehicle_speed must be of type Int or Float but found {type}` | 4002 |
| `vehicle_speed` is `<= 0` | `vehicle_speed cannot be less than or equal to zero` | 4003 |
| Every ping has null `latitude`/`longitude` | `Trace should have at least one ping with non null latitude and longitude` | 4003 |

### Each ping (4001 / 4002 / 4003 / 4004 / 4005)

| Condition | Message | Code |
|---|---|---|
| Ping isn't a dict | `ping must be of type Dict but found {type}` | 4002 |
| Ping is an empty dict | `ping cannot be an empty dictionary` | 4002 |
| `latitude`/`longitude`/`timestamp` key missing | `Expected key: '{key}' missing from the dictionary` | 4001 |
| A key other than `ping_id`/`latitude`/`longitude`/`timestamp`/`error_radius`/`event_type`/`force_retain`/`metadata` is present | `Unexpected key provided in ping dictionary` | 4001 |
| `latitude`/`longitude`/`error_radius` isn't `int`/`float`/`None` | `{key} must be of type Int, Float or None but found {type}` | 4002 |
| `timestamp` isn't `int` | `timestamp must be of type Int but found {type}` | 4002 |
| `timestamp` outside `[0, 2145916800000]` | `timestamp must be in milliseconds and unix epoch format within range [0, 2145916800000] but found timestamp = {value}` | 4004 |
| `latitude` outside `[-90, 90]` | `latitude must be within range [-90 to 90] but found latitude = {value}` | 4005 |
| `longitude` outside `[-180, 180]` | `longitude must be within range [-180 to 180] but found longitude = {value}` | 4005 |
| `error_radius` is negative | `error_radius cannot be negative` | 4003 |
| `event_type` isn't `str`/`None` | `event_type must be of type String or None but found {type}` | 4002 |
| `force_retain` isn't `bool` | `force_retain must be of type Bool but found {type}` | 4002 |
| `metadata` isn't `dict` | `metadata must be of type Dict but found {type}` | 4002 |
| A `metadata` key isn't a `str` | `Expected keys of only string type in metadata dictionary` | 4001 |
| `ping_id` isn't `str` | `ping_id must be of type String but found {type}` | 4002 |
| `ping_id` is an empty string | `ping_id can not be an empty string` | 4003 |
| `ping_id` present on some pings but not all | `ping_id must be present in either all of the pings or in none of the pings` | 4002 |
| Duplicate `ping_id` values across pings | `Expected values for 'ping_id' to be unique, but found duplicate values` | 4003 |

---

## Per-function parameter validation

| Function | Parameter | Type rule | Value rule | Error code |
|---|---|---|---|---|
| `remove_nearby_pings` | `min_dist_bw_consecutive_pings` | int or float | `>= 0` | 4002 / 4003 |
| `remove_pings_by_speed` | `max_speed_kph` | int or float | `>= 0` | 4002 / 4003 |
| `impute_distorted_pings_with_distance` | `max_dist_ratio` | int or float | `>= 1` | 4002 / 4003 |
| `impute_distorted_pings_with_angle` | `max_delta_angle` | int or float | `>= 0` and `<= 180` | 4002 / 4003 |
| `map_match_trace` | `osrm_url` | str | — | 4002 |
| `map_match_trace` | `ping_batch_size` | int | `>= 2` | 4002 / 4003 |
| `map_match_trace` | `map_matching_radius` | int or float | `>= 0` | 4002 / 4003 |
| `map_match_trace` | `avg_snap_distance` | int or float | `>= 0` | 4002 / 4003 |
| `map_match_trace` | `max_matched_dist_to_raw_dist_ratio` | int or float | `>= 0` | 4002 / 4003 |
| `interpolate_trace` | `osrm_url` | str | — | 4002 |
| `interpolate_trace` | `min_dist_from_prev_ping` | int or float | `> 0`, and `< max_dist_from_prev_ping` | 4002 / 4003 |
| `interpolate_trace` | `max_dist_from_prev_ping` | int or float | `> 0` | 4002 / 4003 |
| `add_stop_events_info` | `max_dist_bw_consecutive_pings` | int or float | `> 0` | 4002 / 4003 |
| `add_stop_events_info` | `max_dist_for_merging_stop_points` | int or float | `> 0` | 4002 / 4003 |
| `add_stop_events_info` | `min_size` | int | `>= 2` | 4002 / 4003 |
| `add_stop_events_info` | `min_staying_time` | int | `> 0` | 4002 / 4003 |
| `convert_csv_to_trace_payload` | `csv_file_path` | str | — | 4002 |
| `convert_csv_to_trace_payload` | `vehicle_type` | str | — | 4002 |
| `convert_csv_to_trace_payload` | `vehicle_speed` | int or float | `> 0` | 4002 / 4003 |
| `convert_csv_to_trace_payload` | `force_retain_event_types` | bool | — | 4002 |

**Exact messages for the value-rule failures:**

| Message | Code |
|---|---|
| `min_dist_bw_consecutive_pings cannot be negative` | 4003 |
| `max_speed_kph cannot be negative` | 4003 |
| `max_dist_ratio can not be less than 1, but got {value}` | 4003 |
| `max_delta_angle cannot be negative` | 4003 |
| `max_delta_angle can have value only in range 0 to 180, but got {value}` | 4003 |
| `ping_batch_size cannot be less than 2` | 4003 |
| `map_matching_radius cannot be negative` | 4003 |
| `avg_snap_distance cannot be negative` | 4003 |
| `max_matched_dist_to_raw_dist_ratio cannot be negative` | 4003 |
| `min_dist_from_prev_ping cannot be less than or equal to zero` | 4003 |
| `max_dist_from_prev_ping cannot be less than or equal to zero` | 4003 |
| `min_dist_from_prev_ping must be less than max_dist_from_prev_ping` | 4003 |
| `max_dist_bw_consecutive_pings cannot be less than or equal to zero` | 4003 |
| `max_dist_for_merging_stop_points cannot be less than or equal to zero` | 4003 |
| `min_size cannot be less than 2` | 4003 |
| `min_staying_time cannot be less than or equal to zero` | 4003 |
| `vehicle_speed cannot be less than or equal to zero` | 4003 |

Every corresponding type-mismatch uses one of: `{name} must be of type Int but found {type}`,
`{name} must be of type Float but found {type}`, `{name} must be of type Int or Float but found {type}`,
`{name} must be of type String but found {type}`, `{name} must be of type Bool but found {type}` — code `4002` in every case.

---

## Output feasibility check

`output_validation_utils` re-validates whatever `get_trace_cleaning_output()`
produces, using the same 4001/4002/4003/4004/4005 codes as above, against
this exact shape:

- Top level must have exactly `cleaned_trace`, `cleaning_summary`,
  `distance_summary`, `stop_summary` — no more, no fewer.
- Each `cleaned_trace` entry must have exactly these 20 keys: `ping_id`,
  `input_latitude`, `input_longitude`, `timestamp`, `error_radius`,
  `event_type`, `force_retain`, `metadata`, `cleaned_latitude`,
  `cleaned_longitude`, `update_status`, `last_updated_by`,
  `stop_event_status`, `cumulative_stop_event_time`,
  `representative_stop_event_latitude`,
  `representative_stop_event_longitude`, `stop_event_sequence_number`,
  `time_since_prev_ping`, `dist_from_prev_ping`,
  `cleaned_trace_cumulative_dist`, `cleaned_trace_cumulative_time`.
- `cleaning_summary` must have exactly: `total_pings_in_input`,
  `total_non_null_pings_in_input`, `total_non_null_pings_in_output`,
  `total_trace_time`, `unchanged_percentage`, `drop_percentage`,
  `updation_percentage`, `interpolation_percentage`,
  `total_execution_time`.
- `distance_summary` must have exactly: `cumulative_distance_of_raw_trace`,
  `cumulative_distance_of_clean_trace`, `percent_reduction_in_dist`.
- `stop_summary` must have exactly: `stop_events_info`,
  `global_stop_events_info`. Each `stop_events_info` entry must have
  exactly: `stop_event_sequence_number`, `start_time`, `end_time`,
  `total_stop_event_time`, `number_of_pings`, `representative_latitude`,
  `representative_longitude`. `global_stop_events_info` must have exactly:
  `total_trace_time`, `total_stop_events_time`, `stop_event_percentage`.

Additional cross-field checks, all raised as `ValidationException` (4003):

| Condition | Message |
|---|---|
| `total_non_null_pings_in_input > total_pings_in_input` | `total_non_null_pings_in_input cannot be greater than total_pings_in_input` |
| `total_pings_in_input` in `cleaning_summary` != number of pings actually in the input payload | `total_pings_in_input in cleaning_summary must be equal to number of pings in input payload` |
| `unchanged_percentage + drop_percentage + updation_percentage + interpolation_percentage < 99.9` | `Sum of percentages of various update statuses should be at least 99.9` |

This whole feasibility check only ever runs internally (inside
`get_trace_cleaning_output`) — it exists to catch a bug in tracely itself
before it ever reaches a caller, not something a caller triggers directly.
