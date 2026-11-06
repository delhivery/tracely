# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True
"""
Sequential stop-event grouping: groups time-consecutive GPS pings that stay
within `max_dist_bw_consecutive_pings` of each other's running median location
for at least `min_staying_time`, and reports the median location of each
qualifying group.

The grouping rule (median-distance threshold, minimum group size, minimum
duration) follows the same algorithm as the `infostop` package's own compiled
extension (`cpputils/main.cpp`, Ulf Aslak, GPLv3) -- re-implemented here in
Cython, under tracely's own GPLv3 license, so tracely no longer depends on
the unmaintained `infostop` package.
"""
import numpy as np
cimport numpy as np
from libc.math cimport sin, cos, asin, sqrt, M_PI

# Matches the `haversine` PyPI package's own Earth-radius constant
# (`_AVG_EARTH_RADIUS_KM = 6371.0088`), which `tracely.utils.utils.get_haversine_distance`
# already uses for every other distance calculation in this codebase (remove_nearby_pings,
# remove_pings_by_speed, imputation, OSRM matching). Kept consistent with that rather than
# with infostop's own historical value (6371000.0) that this module replaces.
cdef double _EARTH_RADIUS_M = 6371008.8
cdef double _DEG_TO_RAD = M_PI / 180.0


cdef inline double _haversine_distance(double lat1, double lon1, double lat2, double lon2) nogil:
    cdef double phi1 = lat1 * _DEG_TO_RAD
    cdef double phi2 = lat2 * _DEG_TO_RAD
    cdef double dphi = phi2 - phi1
    cdef double dlambda = (lon2 - lon1) * _DEG_TO_RAD
    cdef double a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return _EARTH_RADIUS_M * 2 * asin(sqrt(a))


def group_stationary_pings(np.ndarray[np.float64_t, ndim=1] latitudes,
                            np.ndarray[np.float64_t, ndim=1] longitudes,
                            np.ndarray[np.float64_t, ndim=1] timestamps,
                            double max_dist_bw_consecutive_pings,
                            long min_size,
                            double min_staying_time,
                            double max_time_between):
    """
    Groups time-consecutive pings into stationary events.

    Walks `latitudes`/`longitudes`/`timestamps` (same length, timestamps in
    ascending order, in seconds) in order. A ping joins the current group if
    its distance to the running median of the current group's latitude and
    longitude is at most `max_dist_bw_consecutive_pings` (meters) and the
    time since the previous ping is at most `max_time_between` (seconds).
    Otherwise the current group closes and a new one starts at that ping.
    A closed group becomes a stationary event only if it has at least
    `min_size` pings and spans at least `min_staying_time` seconds.

    Args:
        latitudes (numpy.ndarray[float64]): Ping latitudes, in decimal degrees.
        longitudes (numpy.ndarray[float64]): Ping longitudes, in decimal degrees.
        timestamps (numpy.ndarray[float64]): Ping unix timestamps, in seconds, ascending.
        max_dist_bw_consecutive_pings (float): Max distance, in meters, between a ping and its
                                                group's running median location, to stay in the group.
        min_size (int): Minimum number of pings for a group to count as a stationary event.
        min_staying_time (float): Minimum duration, in seconds, for a group to count as a stationary event.
        max_time_between (float): Maximum time, in seconds, between two consecutive pings to stay in the same group.

    Returns:
        tuple: (event_latitudes, event_longitudes, event_map, n_events)
            event_latitudes (numpy.ndarray[float64]): Median latitude of each stationary event, length n_events.
            event_longitudes (numpy.ndarray[float64]): Median longitude of each stationary event, length n_events.
            event_map (numpy.ndarray[int64]): Length same as input. For each ping, the index (into
                                              event_latitudes/event_longitudes) of its stationary event,
                                              or -1 if the ping is not part of any qualifying event.
            n_events (int): Number of stationary events found.
    """

    cdef long n = latitudes.shape[0]
    cdef np.ndarray[np.int64_t, ndim=1] event_map = np.empty(n, dtype=np.int64)
    cdef np.ndarray[np.float64_t, ndim=1] lat_buffer = np.empty(n, dtype=np.float64)
    cdef np.ndarray[np.float64_t, ndim=1] lon_buffer = np.empty(n, dtype=np.float64)
    cdef np.ndarray[np.float64_t, ndim=1] event_latitudes = np.empty(n, dtype=np.float64)
    cdef np.ndarray[np.float64_t, ndim=1] event_longitudes = np.empty(n, dtype=np.float64)

    cdef double[:] lat_v = latitudes
    cdef double[:] lon_v = longitudes
    cdef double[:] time_v = timestamps
    cdef double[:] lat_buf_v = lat_buffer
    cdef double[:] lon_buf_v = lon_buffer
    cdef double[:] event_lat_v = event_latitudes
    cdef double[:] event_lon_v = event_longitudes
    cdef np.int64_t[:] event_map_v = event_map

    cdef long group_size = 1
    cdef long n_events = 0
    cdef long group_start = 0
    cdef long event_index = 0
    cdef long i, k, idx, insert_pos, median_lo, median_hi
    cdef double median_lat, median_lon, distance, elapsed, prev_timestamp

    lat_buf_v[0] = lat_v[0]
    lon_buf_v[0] = lon_v[0]
    prev_timestamp = time_v[0]

    for i in range(1, n):
        median_lo = (group_size - 1) // 2
        median_hi = group_size // 2
        median_lat = 0.5 * (lat_buf_v[median_lo] + lat_buf_v[median_hi])
        median_lon = 0.5 * (lon_buf_v[median_lo] + lon_buf_v[median_hi])
        distance = _haversine_distance(lat_v[i], lon_v[i], median_lat, median_lon)
        elapsed = time_v[i] - prev_timestamp

        if distance <= max_dist_bw_consecutive_pings and elapsed <= max_time_between:
            insert_pos = group_size
            for k in range(group_size - 1, -1, -1):
                if lat_buf_v[k] > lat_v[i]:
                    lat_buf_v[k + 1] = lat_buf_v[k]
                    insert_pos = k
                else:
                    break
            lat_buf_v[insert_pos] = lat_v[i]

            insert_pos = group_size
            for k in range(group_size - 1, -1, -1):
                if lon_buf_v[k] > lon_v[i]:
                    lon_buf_v[k + 1] = lon_buf_v[k]
                    insert_pos = k
                else:
                    break
            lon_buf_v[insert_pos] = lon_v[i]

            group_size += 1
        else:
            if (i - group_start) >= min_size and (prev_timestamp - time_v[group_start]) >= min_staying_time:
                median_lo = (group_size - 1) // 2
                median_hi = group_size // 2
                event_lat_v[n_events] = 0.5 * (lat_buf_v[median_lo] + lat_buf_v[median_hi])
                event_lon_v[n_events] = 0.5 * (lon_buf_v[median_lo] + lon_buf_v[median_hi])
                for idx in range(group_start, i):
                    event_map_v[idx] = event_index
                event_index += 1
                n_events += 1
            else:
                for idx in range(group_start, i):
                    event_map_v[idx] = -1

            lat_buf_v[0] = lat_v[i]
            lon_buf_v[0] = lon_v[i]
            group_size = 1
            group_start = i

        prev_timestamp = time_v[i]

    if (n - group_start) >= min_size and (prev_timestamp - time_v[group_start]) >= min_staying_time:
        median_lo = (group_size - 1) // 2
        median_hi = group_size // 2
        event_lat_v[n_events] = 0.5 * (lat_buf_v[median_lo] + lat_buf_v[median_hi])
        event_lon_v[n_events] = 0.5 * (lon_buf_v[median_lo] + lon_buf_v[median_hi])
        for idx in range(group_start, n):
            event_map_v[idx] = event_index
        n_events += 1
    else:
        for idx in range(group_start, n):
            event_map_v[idx] = -1

    return event_latitudes[:n_events], event_longitudes[:n_events], event_map, n_events
