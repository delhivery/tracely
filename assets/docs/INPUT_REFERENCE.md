# Input Reference

The full shape of the `trace_payload` dict passed into `CleanTrace(trace_payload)`.

---

## Top-level payload

| Key | Type | Key required | Value required | Default | Description |
|---|---|:---:|:---:|---|---|
| `trace` | `list[dict]` | yes | yes | — | The list of pings — see [Ping](#ping) below. Must be a non-empty list. |
| `vehicle_type` | `str` | no | no | `"car"` | A string denoting the vehicle type. Not used in any cleaning/matching logic — validated for type only. |
| `vehicle_speed` | `int \| float` | no | no | `25` (km/hr) | Average speed of the vehicle. Not used in any cleaning/matching logic — validated for type and value only. |

Only `"trace"`, `"vehicle_type"`, `"vehicle_speed"` are accepted keys on the
payload dict — any other key raises.

---

## Ping

Each entry of `trace` is a dict with these keys:

| Key | Type | Key required | Value required | Default | Description |
|---|---|:---:|:---:|---|---|
| `latitude` | `int \| float \| None` | yes | yes (key present) | — | Decimal degrees, `[-90, 90]`, or `None`. |
| `longitude` | `int \| float \| None` | yes | yes (key present) | — | Decimal degrees, `[-180, 180]`, or `None`. |
| `timestamp` | `int` | yes | yes | — | Unix timestamp in **milliseconds**. Valid range `[0, 2145916800000]` (1 Jan 2038 00:00:00 UTC). |
| `ping_id` | `str` | no | — | auto-assigned | Unique identifier. **Must be present on either ALL pings or NONE** — a mix raises. If omitted everywhere, one is auto-assigned per ping. Cannot be an empty string; duplicates across pings raise. |
| `error_radius` | `float \| None` | no | — | `None` | GPS/location error radius, metres. If not `None`, must be `>= 0`. |
| `event_type` | `str \| None` | no | — | `None` | A string denoting an event associated with the ping. |
| `force_retain` | `bool` | no | — | `False` | If `True`, this ping is never dropped by any cleaning method regardless of that method's own drop condition. |
| `metadata` | `dict` | no | — | `{}` | Arbitrary extra data. Keys must be strings; values may be any type. |

Only the 8 keys above are accepted on a ping dict — any other key raises.
At least one ping in `trace` must have non-null `latitude` and `longitude`.

### Example

```python
{
    "trace": [
        {
            "latitude": 19.051482,
            "longitude": 73.071413,
            "timestamp": 1706759550433,
            "error_radius": 24.79,
            "force_retain": False,
        },
        {
            "latitude": 19.052407,
            "longitude": 73.074993,
            "timestamp": 1706774541094,
            "error_radius": 11.01,
            "event_type": "event_a",
            "force_retain": True,
            "metadata": {"id": 123123},
        },
        {
            "latitude": 19.052097,
            "longitude": 73.074919,
            "timestamp": 1706774375598,
        },
    ],
    "vehicle_type": "car",
    "vehicle_speed": 40,
}
```

---

## `convert_csv_to_trace_payload` — building a payload from a CSV

Utility to build the payload above from a CSV file, instead of constructing
it by hand.

```python
from tracely.utils.input_output_utils import convert_csv_to_trace_payload

payload = convert_csv_to_trace_payload(
    csv_file_path="file_path.csv",
    vehicle_type="car",
    vehicle_speed=25,
    force_retain_event_types=True,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `csv_file_path` | `str` | required | Path to the input CSV. Must contain at least `latitude`, `longitude`, `timestamp` columns. |
| `vehicle_type` | `str` | `"car"` | Passed straight into the payload's `vehicle_type`. |
| `vehicle_speed` | `int \| float` | `25` | Passed straight into the payload's `vehicle_speed`. Must be `> 0`. |
| `force_retain_event_types` | `bool` | `True` | Only takes effect when the CSV has an `event_type` column AND does NOT already have its own `force_retain` column. In that case, any row whose `event_type` is a non-empty string gets `force_retain=True`; every other row gets `force_retain=False`. If the CSV already has a `force_retain` column, that column's own values are used unchanged instead, regardless of this flag. |

**Extra columns become `metadata`.** Any CSV column that isn't one of the
mandatory (`latitude`/`longitude`/`timestamp`) or optional ping keys
(`ping_id`, `error_radius`, `event_type`, `force_retain`, `metadata`) is
folded into that ping's `metadata` dict, keyed by its own column name. If
the CSV happens to already have a column literally named `metadata`, it's
renamed internally and nested as `metadata["metadata"]` so it doesn't
collide with the dict itself.

Raises `InputOutputException` (code `1001`) if the file can't be read, or
(code `1002`) if a mandatory column is missing.
