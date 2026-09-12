# Output Reference

The full shape of the dict returned by `CleanTrace.get_trace_cleaning_output()`.

```python
output = clean_trace_object.get_trace_cleaning_output()
# output = {"cleaned_trace": [...], "cleaning_summary": {...},
#           "distance_summary": {...}, "stop_summary": {...}}
```

---

## `cleaned_trace` (list)

One dict per ping. At least as many entries as pings in the input — more,
if `interpolate_trace` has been called and added synthetic pings.

There are exactly three ways a ping's location can differ from the input:
it can be **dropped** (`cleaned_latitude`/`cleaned_longitude` become
`None`), **updated** (moved to an imputed/matched location), or a brand
new **interpolated** ping can be inserted. Anything else is **unchanged**.

| Key | Type | Description |
|---|---|---|
| `ping_id` | `str` | Unique identifier. If provided on input, reused as-is. If auto-assigned, or if this is an interpolated ping, follows the scheme below. |
| `input_latitude` | `float \| None` | The original input latitude. `None` if this is an interpolated ping (it never existed in the input). |
| `input_longitude` | `float \| None` | The original input longitude. `None` if interpolated. |
| `timestamp` | `int` | Unix ms. For an interpolated ping, a value between its two surrounding input pings' timestamps. |
| `error_radius` | `float \| None` | The original input value. `None` if interpolated. |
| `event_type` | `str \| None` | The original input value. `None` if not provided, or if interpolated. |
| `force_retain` | `bool` | Same as input. `False` if interpolated. |
| `metadata` | `dict` | Matched back to the original input ping by `ping_id`. **`{}` (empty dict) if interpolated** — an interpolated ping's `ping_id` is synthetic and never matches any real input ping, so no metadata can be matched back to it. |
| `cleaned_latitude` | `float \| None` | The current cleaned latitude. `None` if this ping has been dropped. |
| `cleaned_longitude` | `float \| None` | The current cleaned longitude. `None` if dropped. |
| `update_status` | `str` | One of `"unchanged"`, `"dropped"`, `"updated"`, `"interpolated"` — see below. |
| `last_updated_by` | `str` | The name of the function that most recently changed this ping. `"never_updated"` if nothing has touched it yet. |
| `stop_event_status` | `bool` | `True` if this ping is part of a detected stop event. `False` by default, only ever set by `add_stop_events_info`. |
| `stop_event_sequence_number` | `int` | Which stop event this ping belongs to (an integer >= 1 per event). `-1` by default / if not part of a stop event. |
| `cumulative_stop_event_time` | `str` | Cumulative time spent in the stop event up to and including this ping, formatted `"{X} minutes and {Y} seconds"`. `"0 minutes and 0 seconds"` by default / if not a stop ping. |
| `representative_stop_event_latitude` | `float \| None` | The stop event's representative latitude, if this ping is part of one. `None` otherwise. |
| `representative_stop_event_longitude` | `float \| None` | The stop event's representative longitude. `None` otherwise. |
| `time_since_prev_ping` | `float` | Milliseconds since the previous ping (in cleaned-trace order). |
| `dist_from_prev_ping` | `float` | Metres from the previous ping (in cleaned-trace order). |
| `cleaned_trace_cumulative_dist` | `float` | Metres accumulated up to and including this ping. |
| `cleaned_trace_cumulative_time` | `float` | Milliseconds accumulated up to and including this ping. |

**`update_status` values:**

| Value | Meaning |
|---|---|
| `"unchanged"` | `cleaned_latitude`/`cleaned_longitude` are the same as the input. |
| `"dropped"` | This ping was dropped — `cleaned_latitude`/`cleaned_longitude` are `None`. |
| `"updated"` | The location was changed (imputed or map-matched), but the ping itself still corresponds to a real input ping. |
| `"interpolated"` | This ping did not exist in the input — it was synthesized by `interpolate_trace`. |

**Interpolated `ping_id` scheme**: if `n` synthetic pings are inserted
between two consecutive real pings whose ids are `ping_id_i` and
`ping_id_j`, the interpolated pings get ids `ping_id_i_1`, `ping_id_i_2`,
..., `ping_id_i_n` — the PRECEDING real ping's id, suffixed with a
1-based counter.

---

## `cleaning_summary` (dict)

| Key | Type | Description |
|---|---|---|
| `total_pings_in_input` | `int` | Total pings in the input, including any with null lat/lon. Must equal `len(payload["trace"])`. |
| `total_non_null_pings_in_input` | `int` | Pings in the input with non-null latitude and longitude. |
| `total_non_null_pings_in_output` | `int` | Non-null-location pings surviving in the output. |
| `total_trace_time` | `str` | Total span of the raw trace, formatted `"{X} hours, {Y} minutes and {Z} seconds"`. |
| `unchanged_percentage` | `float` | % of non-null input pings whose location is unchanged. |
| `drop_percentage` | `float` | % of non-null input pings that were dropped. |
| `updation_percentage` | `float` | % of non-null input pings that were updated. |
| `interpolation_percentage` | `float` | % of non-null input pings' count that the interpolated pings add. |
| `total_execution_time` | `float` | Seconds taken to construct the `CleanTrace` object and run every cleaning method called on it so far. |

`unchanged_percentage + drop_percentage + updation_percentage +
interpolation_percentage` is guaranteed to sum to at least `99.9`.

---

## `distance_summary` (dict)

| Key | Type | Description |
|---|---|---|
| `cumulative_distance_of_raw_trace` | `float` | Total haversine distance across the raw trace's consecutive pings, metres. Pings with null input lat/lon are skipped. |
| `cumulative_distance_of_clean_trace` | `float` | Same, but over the cleaned trace's `cleaned_latitude`/`cleaned_longitude`. |
| `percent_reduction_in_dist` | `float` | % reduction of clean vs. raw distance. `0` if the clean trace's distance is actually >= the raw trace's (never negative). |

---

## `stop_summary` (dict)

Populated only after `add_stop_events_info` has been called — otherwise
`stop_events_info` is `[]` and `global_stop_events_info` reflects zero
stop time.

| Key | Type | Description |
|---|---|---|
| `stop_events_info` | `list[dict]` | One entry per detected stop event — see below. |
| `global_stop_events_info` | `dict` | Trace-wide stop totals — see below. |

**Each `stop_events_info` entry:**

| Key | Type | Description |
|---|---|---|
| `stop_event_sequence_number` | `int` | Unique integer id for this stop event. |
| `start_time` | `str` | `"YYYY-MM-DD HH:MM:SS"`, when the stop event started. |
| `end_time` | `str` | `"YYYY-MM-DD HH:MM:SS"`, when it ended. |
| `total_stop_event_time` | `str` | `"{X} hours, {Y} minutes and {Z} seconds"`. |
| `number_of_pings` | `int` | Pings that are part of this stop event. |
| `representative_latitude` | `float` | Representative latitude of the stop location. |
| `representative_longitude` | `float` | Representative longitude of the stop location. |

**`global_stop_events_info`:**

| Key | Type | Description |
|---|---|---|
| `total_trace_time` | `str` | `"{X} hours, {Y} minutes and {Z} seconds"` — same total span as `cleaning_summary.total_trace_time`. |
| `total_stop_events_time` | `str` | Total time spent across every stop event, same format. |
| `stop_event_percentage` | `float` | `total_stop_events_time` as a % of `total_trace_time`. |

---

## Full example

```python
{
  "cleaned_trace": [
    {
      "ping_id": "1533",
      "input_latitude": 19.052419,
      "input_longitude": 73.072199,
      "timestamp": 1706755887680,
      "error_radius": 24.9,
      "event_type": "event",
      "force_retain": True,
      "metadata": {"trace_id": "110"},
      "cleaned_latitude": 19.052441,
      "cleaned_longitude": 73.072202,
      "update_status": "updated",
      "last_updated_by": "map_match_trace",
      "stop_event_status": True,
      "stop_event_sequence_number": 1,
      "cumulative_stop_event_time": "0 minutes and 0 seconds",
      "representative_stop_event_latitude": 19.052415,
      "representative_stop_event_longitude": 73.072195,
      "time_since_prev_ping": 0.0,
      "dist_from_prev_ping": 0.0,
      "cleaned_trace_cumulative_dist": 0.0,
      "cleaned_trace_cumulative_time": 0.0
    }
  ],
  "cleaning_summary": {
    "total_pings_in_input": 3114,
    "total_non_null_pings_in_input": 2965,
    "total_non_null_pings_in_output": 1644,
    "total_trace_time": "7 hours, 24 minutes and 38 seconds",
    "unchanged_percentage": 41.28,
    "drop_percentage": 44.55,
    "updation_percentage": 14.17,
    "interpolation_percentage": 0.0,
    "total_execution_time": 0.38711
  },
  "distance_summary": {
    "cumulative_distance_of_raw_trace": 36788.37,
    "cumulative_distance_of_clean_trace": 36788.37,
    "percent_reduction_in_dist": 0.0
  },
  "stop_summary": {
    "stop_events_info": [
      {
        "stop_event_sequence_number": 1,
        "start_time": "2024-02-01 08:21:27",
        "end_time": "2024-02-01 08:26:13",
        "total_stop_event_time": "0 hours, 4 minutes and 46 seconds",
        "number_of_pings": 34,
        "representative_latitude": 19.052406794117648,
        "representative_longitude": 73.07219488235295
      }
    ],
    "global_stop_events_info": {
      "total_trace_time": "7 hours, 24 minutes and 38 seconds",
      "total_stop_events_time": "3 hours, 47 minutes and 12 seconds",
      "stop_event_percentage": 51.098283229627405
    }
  }
}
```
