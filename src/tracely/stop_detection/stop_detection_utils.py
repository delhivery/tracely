"""
Stop-event detection for a cleaned trace.

Replaces the unmaintained `infostop` package. Groups time-consecutive pings
into stationary events (via the compiled `tracely._stop_event_grouping`
extension) and clusters nearby stationary events together (via `infomap`,
the same graph-clustering engine `infostop` itself used).
"""
import numpy as np
from sklearn.neighbors import BallTree
import infomap

from . import _stop_event_grouping

# Matches the `haversine` PyPI package's own Earth-radius constant
# (`_AVG_EARTH_RADIUS_KM = 6371.0088`), which `tracely.utils.utils.get_haversine_distance`
# already uses for every other distance calculation in this codebase, and the same constant
# `_stop_event_grouping.pyx` uses, for consistency across the whole module.
_EARTH_RADIUS_M = 6371008.8


def _find_nearby_event_pairs(event_coords, max_dist_for_merging_stop_points):
    """
    For each stationary event location, finds every other location within
    `max_dist_for_merging_stop_points` of it.

    Args:
        event_coords (numpy.ndarray): Array of shape (N, 2) of unique (latitude, longitude)
                                      stationary event locations, in decimal degrees.
        max_dist_for_merging_stop_points (float): Max distance, in meters, between two
                                                  stationary event locations to treat them as nearby.

    Returns:
        numpy.ndarray: Array of length N. Each element is an array of the indices (into
                       `event_coords`) of locations within `max_dist_for_merging_stop_points`
                       of that location, including itself.
    """

    event_coords_radians = np.radians(event_coords)
    radius_radians = max_dist_for_merging_stop_points / _EARTH_RADIUS_M

    tree = BallTree(event_coords_radians, metric="haversine")
    return tree.query_radius(event_coords_radians, r=radius_radians)


def _cluster_nearby_events(nearby_event_pairs, event_ping_counts):
    """
    Clusters stationary event locations that are nearby each other into stop events, using Infomap.

    A location with no other location nearby it (a "singleton") becomes its own, separate stop
    event. Otherwise, locations are connected in a graph (an edge between two locations that are
    nearby each other, weighted by the larger of the two locations' ping counts) and Infomap's
    two-level community detection assigns each connected location a stop event number.

    Args:
        nearby_event_pairs (numpy.ndarray): Output of `_find_nearby_event_pairs` — for each
                                            stationary event location, the indices of every
                                            other location nearby it, including itself.
        event_ping_counts (numpy.ndarray): For each stationary event location, how many pings
                                           (across the whole trace) share that exact location.

    Returns:
        numpy.ndarray: Array of length same as `nearby_event_pairs`. Stop event number for each
                       stationary event location, as a non-negative integer. Every location gets
                       a stop event number (singletons get their own); -1 is never returned here.
    """

    network = infomap.Infomap("--two-level --silent")

    singleton_locations = []
    has_non_singleton_location = False
    for location_index, nearby_locations in enumerate(nearby_event_pairs):
        if len(nearby_locations) > 1:
            network.add_node(location_index)
            has_non_singleton_location = True
        else:
            singleton_locations.append(location_index)

    for location_index, nearby_locations in enumerate(nearby_event_pairs):
        for other_location_index in nearby_locations[nearby_locations > location_index]:
            network.add_link(
                int(location_index),
                int(other_location_index),
                float(max(event_ping_counts[location_index], event_ping_counts[other_location_index])))

    if has_non_singleton_location:
        run_result = network.run()
        # infomap's Python API moved the module assignment from a network-level `.modules`
        # attribute (every infomap version tracely has ever used, up to and including the
        # version pinned today) to a `.modules()` method on the object `run()` returns, in a
        # later release. Handle both so a future infomap version bump degrades gracefully
        # instead of crashing.
        stop_event_numbers = dict(run_result.modules()) if run_result is not None else dict(network.modules)
    else:
        stop_event_numbers = {}

    next_stop_event_number = max(stop_event_numbers.values(), default=-1) + 1
    for location_index in singleton_locations:
        stop_event_numbers[location_index] = next_stop_event_number
        next_stop_event_number += 1

    return np.array([stop_event_numbers[i] for i in range(len(nearby_event_pairs))])


def get_stop_labels(trace_list, max_dist_bw_consecutive_pings, max_dist_for_merging_stop_points,
                     min_size, min_staying_time):
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
        numpy.ndarray: Array of labels of size same as number of pings in input trace_list.
    """

    trace_array = np.asarray(trace_list, dtype=np.float64)

    event_latitudes, event_longitudes, event_map, n_events = _stop_event_grouping.group_stationary_pings(
        trace_array[:, 0].copy(),
        trace_array[:, 1].copy(),
        trace_array[:, 2].copy(),
        float(max_dist_bw_consecutive_pings),
        int(min_size),
        float(min_staying_time),
        86400.0)

    if n_events == 0:
        raise ValueError("No stop events found. Check that `max_dist_bw_consecutive_pings`, "
                          "`min_staying_time` and `min_size` parameters are chosen correctly.")

    event_coords = np.column_stack([event_latitudes, event_longitudes])
    unique_event_coords, inverse_indices, event_ping_counts = np.unique(
        event_coords, return_inverse=True, return_counts=True, axis=0)

    nearby_event_pairs = _find_nearby_event_pairs(unique_event_coords, max_dist_for_merging_stop_points)
    stop_event_numbers = _cluster_nearby_events(nearby_event_pairs, event_ping_counts)

    ping_stop_event_numbers = stop_event_numbers[inverse_indices]
    # event_map uses -1 for pings that are not part of any qualifying stop event; appending -1
    # here lets that sentinel double as a valid index (Python indexes -1 as "last element").
    ping_stop_event_numbers_with_sentinel = np.hstack([ping_stop_event_numbers, -1])
    return ping_stop_event_numbers_with_sentinel[event_map]
