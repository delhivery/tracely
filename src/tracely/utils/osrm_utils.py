import requests
import polyline
import numpy as np

from .utils import get_haversine_distance
from ..exceptions.custom_exceptions import OSRMException
from ..exceptions.error_messages import OSRMErrorMessage, OSRMErrorCode


def create_segments(trace_df,
                    segment_size=5):
    """
    Divides a trace list into smaller chunks (segments) of a specified size.
    This function processes a DataFrame containing pings and creates segments of specified size.

    Args:
        trace_df (pandas.DataFrame): A DataFrame containing pings, which must include columns "cleaned_latitude", "cleaned_longitude", and "ping_id".
        segment_size (int, optional): The size of each segment. Defaults to 5.

    Returns:
        list: A list of segments, where each segment is a list of lists containing latitude, longitude, ping_id, last_updated_by and a flag.
    """

    # Drop null values of "cleaned_latitude" and "cleaned_longitude"
    filtered_trace_df = trace_df.dropna(subset=["cleaned_latitude", "cleaned_longitude"])

    # Get list of ["cleaned_latitude", "cleaned_longitude", "ping_id", "last_updated_by"]
    trace_list = filtered_trace_df[["cleaned_latitude", 
                                    "cleaned_longitude", 
                                    "ping_id", 
                                    "last_updated_by"]].values.tolist()
    
    # Add the flag (1) to each sublist
    trace_list = [row + [1] for row in trace_list]

    # Create segment list
    segment_list = []
    for i in range(0, len(trace_list), segment_size):
        segment = trace_list[i:i + segment_size]
        segment_list.append(segment)

    return segment_list


def process_trace_segments(segments, 
                           osrm_url, 
                           map_matching_radius,
                           avg_snap_distance, 
                           max_matched_dist_to_raw_dist_ratio):
    """
    Processes trace segments to match them with the road network.
    This function takes a list of trace segments and attempts to match each segment with the road network using OSRM. It evaluates the
    snap distance and applies logic to determine if pings should be updated or not based on their snapped distances.

    Args:
        segments (list): A list of lists, where each list is a segment. 
                         Each segment is a list of lists where each list contains:
                            :latitude (int, float)
                            :longitude (int, float)
                            :ping_id (str)
                            :last_updated_by (str) 
                            :flag (bool)
        osrm_url (str): A URL that specifies the endpoint for accessing map matching service provided by an OSRM instance.
                        Example: "http://127.0.0.1:5000/match/v1/driving/".
        map_matching_radius (int, float): The radius in meters for map matching. A location is map matched only if there is a road within the map matching radius. 
        avg_snap_distance (int, float): Average snap distance in meters for all points in a segment.
        max_matched_dist_to_raw_dist_ratio (int, float): For two consecutive pings, it is the maximum ratio between (distance between point after map matching) and (distance between point before map matching)
    
    Returns:
        matched_segments (list): A list of lists, where each list contains the following elements after map matching:
                                     :latitude (int, float): Represents matched location's latitude if match is found else original latitude.
                                     :longitude (int, float): Represents matched location's longitude if match is found else original longitude.
                                     :ping_id (str): Original ping id of the location. 
                                     :last_updated_by (str): The function of CleanTrace class which last updated the ping.
                                     :flag (bool): 1 if no match is found else 2.
    """

    matched_segments = []
    for segment in segments:
        decoded_geometry, \
            matched_geometry, \
            total_snap_distance = get_osrm_match(segment,
                                                 osrm_url,
                                                 map_matching_radius)

        if matched_geometry is None:
            matched_segments.extend(segment)
            continue

        negative_map_matched_flag = 1 
        positive_map_matched_flag = 2  

        # Check if snap distance of the segment is less than average snap distance specified by user
        if total_snap_distance < avg_snap_distance * len(segment):
            prev_match_lat, prev_match_lng = matched_geometry[0][0], matched_geometry[0][1]
            prev_lat, prev_lng = segment[0][0], segment[0][1]

            for matched_ping, raw_ping in zip(matched_geometry, segment):
                curr_match_lat, curr_match_lng = matched_ping[0], matched_ping[1]
                curr_lat, curr_lng = raw_ping[0], raw_ping[1]
                ping_id = raw_ping[2]
                last_updated_by = raw_ping[3]
                
                matched_distance = get_haversine_distance(prev_match_lat,
                                                          prev_match_lng,
                                                          curr_match_lat,
                                                          curr_match_lng)
                
                raw_distance = get_haversine_distance(prev_lat,
                                                      prev_lng,
                                                      curr_lat,
                                                      curr_lng)

                if ((last_updated_by == "interpolate_trace") or
                    (matched_distance > (max_matched_dist_to_raw_dist_ratio * raw_distance))):
                    prev_match_lat, prev_match_lng = curr_lat, curr_lng
                    
                    matched_segments.append([raw_ping[0],
                                             raw_ping[1],
                                             ping_id,
                                             last_updated_by,
                                             negative_map_matched_flag])  # Unmatched flag
                else:
                    prev_match_lat, prev_match_lng = curr_match_lat, curr_match_lng
                    
                    matched_segments.append([matched_ping[0],
                                             matched_ping[1],
                                             ping_id,
                                             last_updated_by,
                                             positive_map_matched_flag])  # Matched flag

                prev_lat, prev_lng = curr_lat, curr_lng

        else:
            for matched_ping, raw_ping in zip(matched_geometry, segment):
                ping_id = raw_ping[2]
                last_updated_by = raw_ping[3]

                snap_distance = get_haversine_distance(matched_ping[0],
                                                       matched_ping[1],
                                                       raw_ping[0],
                                                       raw_ping[1])
                
                if ((snap_distance < (avg_snap_distance / 2) and 
                    (last_updated_by != "interpolate_trace"))):
                    matched_segments.append([matched_ping[0],
                                             matched_ping[1],
                                             ping_id,
                                             last_updated_by,
                                             positive_map_matched_flag])  # Matched flag
                else:
                    matched_segments.append([raw_ping[0],
                                             raw_ping[1],
                                             ping_id,
                                             last_updated_by,
                                             negative_map_matched_flag])  # Unmatched flag

    return matched_segments


def get_osrm_route(geo_coords, 
                   osrm_url):
    """
    Fetches a driving route based on a sequence of geographical coordinates.
    This function sends a request to an OSRM server to retrieve the driving route for the
    specified geographical coordinates in the order they are provided. The function returns
    the route geometry, total distance in meters, and travel time in seconds.

    Args:
        geo_coords (list): A list of tuples where each tuple represents a location using two elements: (latitude, longitude).
                           Example format: [(lat1, lng1),(lat2, lng2)].
        osrm_url (str): The URL of the OSRM server for route service, including the routing endpoint.
                        Example: "http://127.0.0.1:5000/route/v1/driving/".

    Returns:
        tuple: A tuple containing three elements:
            route_geometry (list or None): A list of locations (tuples) representing the route taken to cover all geo_coords. 
                                           Each tuple is represented as (latitude, longitude). Returns None if status code 
                                           from osrm request is not 200 or when route_distance in NaN.
            route_distance (float): The total distance of the route in meters. Returns 0 if status code from osrm request 
                                    is not 200 or when route_distance in NaN.
            route_time (float): The estimated travel time in seconds. Returns 0 if status code from osrm request 
                                is not 200 or when route_distance in NaN.
    """

    # Create URL for route service request
    loc = ""
    for coord in geo_coords:
        loc += "{},{};".format(coord[1], coord[0])
    url = osrm_url + loc[:-1] + "?overview=full&annotations=speed"

    try:
        request_output = requests.get(url, timeout=30)
    except Exception:
        raise OSRMException(
            OSRMErrorMessage.CONNECTION_ERROR.format(url),
            OSRMErrorCode.CONNECTION_ERROR_CODE)

    if request_output.status_code == 200:
        res = request_output.json()
        geometry = res["routes"][0]["geometry"]
        route_distance = res["routes"][0]["distance"]
        route_time = res["routes"][0]["duration"]

        if ((not np.isfinite(route_distance)) or 
            (np.isnan(route_distance))):
            return None, 0, 0

        route_geometry = polyline.decode(geometry)

        return route_geometry, route_distance, route_time
    else:
        return None, 0, 0


def get_osrm_match(geo_coords, 
                   osrm_url, 
                   map_matching_radius):
    """
    Performs map matching for a sequence of geographical coordinates.
    This function sends a request to an OSRM server to obtain a map match for the specified
    sequence of geographical coordinates. It returns the decoded geometry, matched geometry,
    and the total snap distance of the matched trace with respect to the original trace.

    Args:
        geo_coords (list): A list of tuples where each tuple represents a location using two elements: (latitude, longitude).
                           Example format: [(lat1, lng1),(lat2, lng2)].
        osrm_url (str): The URL of the OSRM server for map matching. Example: "http://127.0.0.1:5000/match/v1/driving/".
        map_matching_radius (int, float): The radius in meters for map matching. A location is map matched only 
                                          if there is a road within the map matching radius.

    Returns:
        tuple: A tuple containing three elements:
            decoded_geometry (list or None): A list of locations (tuples) representing the route taken to cover all geo_coords. 
                                             Each tuple is represented as (latitude, longitude). The length of "decoded_geometry"
                                             may exceed that of "geo_coords". Returns None if status code from osrm request is not 200.
            matched_geometry (list or None): A list of tuples representing the matched route geometry in the 
                                             format (latitude, longitude). Its length is same as that of `geo_coords`. 
                                             Returns None if status code from osrm request is not 200.
            total_snap_distance (float or None): The total snap distance in meters of the matched trace with respect to 
                                                 the original trace. Returns None if status code from osrm request is not 200.
    """

    # Create URL for match service request
    loc = ""
    for coord in geo_coords:
        loc += "{},{};".format(coord[1], coord[0])

    radii = ";".join([str(map_matching_radius)] * len(geo_coords))

    loc = loc[:-1]
    url = (osrm_url + loc + "?overview=full" + "&radiuses=" + radii
           + "&generate_hints=false" + "&skip_waypoints=false"
           + "&gaps=ignore" + "&geometries=geojson" + "&annotations=true")
    
    try:
        request_output = requests.get(url, timeout=30)

    except Exception:
        raise OSRMException(
                OSRMErrorMessage.CONNECTION_ERROR.format(url),
                OSRMErrorCode.CONNECTION_ERROR_CODE
                )    
    
    # Get result if map matching is successful
    if request_output.status_code == 200:
        res = request_output.json()
        geometry = res["matchings"][0]["geometry"]
        data = res["tracepoints"]
        matched_geometry = []
        total_snap_distance = 0
        count = 0
        for point in data:
            if point is not None:
                loc = point["location"]
                matched_geometry.append([loc[1], loc[0]])
                count += 1
            else:
                # Use coordinates from geo_coords
                matched_geometry.append(
                    [geo_coords[count][0], geo_coords[count][1]])
                count += 1

        # Extract decoded geometry
        decoded_geometry = [(point[1], point[0]) for point in geometry["coordinates"]]
        
        # Calculate snap distance
        for i in range(len(matched_geometry)):
            total_snap_distance += get_haversine_distance(
                matched_geometry[i][0],
                matched_geometry[i][1],
                geo_coords[i][0],
                geo_coords[i][1])

        return decoded_geometry, matched_geometry, total_snap_distance

    # Return None if map matching is unsuccessful
    else:
        return None, None, None
