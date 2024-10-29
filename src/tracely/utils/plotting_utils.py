import numpy as np
import pandas as pd
import folium as fl
from folium import plugins

from .utils import convert_unix_timestamp_to_human_readable, \
                   convert_time_interval_to_human_readable


def create_general_popup(info_dict: dict,
                         width=350,
                         height=150,
                         max_width=400) -> fl.Popup:
    """
    Creates a HTML popup which can be attached to a folium map object. Popup is created using `info_dict`.

    Args:
        info_dict (dict): Dictionary of information which is used to create the popup.
        width (int, optional): Width of popup. Defaults to 350.
        height (int, optional): Height of popup. Defaults to 150.
        max_width (int, optional): Maximum width. Defaults to 400.

    Returns:
        folium.Popup: Folium popup object containing provided data.
    """
    
    html = ""
    for key in info_dict:
        html += f"<div><b>{key}</b>: {info_dict[key]}</div>"

    iframe = fl.IFrame(html=html, width=width, height=height)

    return fl.Popup(iframe, max_width=max_width)


def plot_raw_trace_from_trace_output(trace: list,
                                     map_object) -> fl.Map:
    """
    Provides map plot of raw trace. Raw trace is added in `map_object`

    Args:
        trace (list): List of pings.
        map_object (folium.Map): Map object in which trace is to be added.

    Returns:
        folium.Map: Map plot of raw trace.
    """

    trace_df = pd.DataFrame(trace).reset_index(drop=True)
    trace_df = trace_df.sort_values(by=["timestamp"])

    # Dividing the trace into 10 segments, each containing 10% of trace.
    n = len(trace_df)
    segment_size = n // 10

    # Create the "trace_segment" column
    trace_df["trace_segment"] = ((np.arange(n) // segment_size) + 1) * 10

    # Handle any remaining rows that don't fit evenly into segments
    trace_df["trace_segment"] = trace_df["trace_segment"].clip(upper=100)
    trace_df = trace_df.dropna(subset=["input_latitude", "input_longitude"]).reset_index(drop=True)

    raw_trace_geometry = []
    layers_dict = {}

    for segment_id in list(trace_df["trace_segment"].unique()):
        segment_name = "Trace_till_" + str(segment_id) + "%"
        raw_trace_segment_layer = fl.FeatureGroup(segment_name,
                                                  show=False)

        map_object.add_child(child=raw_trace_segment_layer)
        layers_dict[segment_id] = raw_trace_segment_layer

    # Extract latitude and longitude from trace_df and store as tuples in raw_trace_geometry
    raw_trace_geometry = list(zip(trace_df["input_latitude"], trace_df["input_longitude"]))

    for index, row in trace_df.iterrows():

        segment_id = row["trace_segment"]
        segment_layer = layers_dict[segment_id]

        if (index != (len(trace_df) - 1)):
            start = raw_trace_geometry[index]
            end = raw_trace_geometry[index + 1]
            fl.PolyLine(locations=[start, end],
                        color="red",
                        weight=3,
                        opacity=1,
                        tags=["raw"],
                        ).add_to(segment_layer)

        icon = plugins.BeautifyIcon(icon_shape="circle",
                                    number=index + 1,
                                    border_color="blue",
                                    background_color="transparent",
                                    inner_icon_style="font-size:12px;")

        raw_coordinates = [row["input_latitude"],
                           row["input_longitude"]]

        fl.Marker(raw_coordinates,
                  icon=icon,
                  tooltip="Index: " + str(index + 1) +
                  "_ping_id: " + row["ping_id"],
                  tags=[""],
                  popup=create_general_popup({"ping_id": row["ping_id"],
                                              "input_latitude": None if pd.isna(row["input_latitude"]) else row["input_latitude"],
                                              "input_longitude": None if pd.isna(row["input_longitude"]) else row["input_longitude"],
                                              "timestamp": int(row["timestamp"]),
                                              "time_string": convert_unix_timestamp_to_human_readable(int((row["timestamp"])//1000)),
                                              "error_radius": None if pd.isna(row["error_radius"]) else row["error_radius"],
                                              "event_type": None if pd.isna(row["event_type"]) else row["event_type"],
                                              "force_retain": row["force_retain"]}
                                              )
                        ).add_to(segment_layer)
 
    fl.LayerControl().add_to(map_object) 
    return map_object


def plot_clean_trace_from_trace_output(trace,
                                       map_object):
    """
    Provides map plot of clean trace. Clean trace is added in `map_object`

    Args:
        trace (list): List of pings.
        map_object (folium.Map): Map object in which trace is to be added.

    Returns:
        folium.Map: Map plot of clean trace.
    """

    trace_df = pd.DataFrame(trace).reset_index(drop=True)
    trace_df = trace_df.sort_values(by=["timestamp"])

    # Dividing the trace into 10 segments, each containing 10% of trace.
    n = len(trace_df)
    segment_size = n // 10

    # Create the "trace_segment" column
    trace_df["trace_segment"] = ((np.arange(n) // segment_size) + 1) * 10

    # Handle any remaining rows that don't fit evenly into segments
    trace_df["trace_segment"] = trace_df["trace_segment"].clip(upper=100)
    trace_df = trace_df.dropna(subset=["cleaned_latitude","cleaned_longitude"]).reset_index(drop=True)

    clean_trace_geometry = []
    layers_dict = {}

    for segment_id in list(trace_df["trace_segment"].unique()):
        segment_name = "Trace_till_" + str(segment_id) + "%"
        clean_trace_segment_layer = fl.FeatureGroup(segment_name,
                                                    show=False)

        map_object.add_child(child=clean_trace_segment_layer)
        layers_dict[segment_id] = clean_trace_segment_layer

    for index, row in trace_df.iterrows():

        if ((row["cleaned_latitude"] is None) or
           (row["cleaned_longitude"] is None) or
           (pd.isna(row["cleaned_latitude"])) or
           (pd.isna(row["cleaned_longitude"]))):
            continue

        clean_trace_geometry.append((row["cleaned_latitude"],row["cleaned_longitude"]))

    total_non_null_pings = len(clean_trace_geometry)
    counter = 0

    for index, row in trace_df.iterrows():

        if ((row["cleaned_latitude"] is None) or
           (row["cleaned_longitude"] is None) or
           (pd.isna(row["cleaned_latitude"])) or
           (pd.isna(row["cleaned_longitude"]))):
            continue

        segment_id = row["trace_segment"]
        segment_layer = layers_dict[segment_id]

        if (counter != (total_non_null_pings - 1)):
            start = clean_trace_geometry[counter]
            end = clean_trace_geometry[counter + 1]
            fl.PolyLine(locations=[start, end],
                        color="red",
                        weight=3,
                        opacity=1,
                        tags=["clean"],
                        ).add_to(segment_layer)
        counter += 1

        icon = plugins.BeautifyIcon(icon_shape="circle",
                                    number=index + 1,
                                    border_color="blue",
                                    background_color="transparent",
                                    inner_icon_style="font-size:12px;")

        clean_coordinates = [row["cleaned_latitude"],
                             row["cleaned_longitude"]]

        fl.Marker(clean_coordinates,
                  icon=icon,
                  tooltip="Index: " + str(index + 1) +
                  "_ping_id: " + row["ping_id"],
                  tags=[""],
                  popup=create_general_popup({"ping_id": row["ping_id"],
                                              "input_latitude": None if pd.isna(row["input_latitude"]) else row["input_latitude"],
                                              "input_longitude": None if pd.isna(row["input_longitude"]) else row["input_longitude"],
                                              "timestamp": int(row["timestamp"]),
                                              "time_string": convert_unix_timestamp_to_human_readable(int((row["timestamp"])//1000)),
                                              "error_radius": None if pd.isna(row["error_radius"]) else row["error_radius"],
                                              "event_type": None if pd.isna(row["event_type"]) else row["event_type"],
                                              "force_retain": row["force_retain"],
                                              "cleaned_latitude": None if pd.isna(row["cleaned_latitude"]) else row["cleaned_latitude"],
                                              "cleaned_longitude": None if pd.isna(row["cleaned_longitude"]) else row["cleaned_longitude"],
                                              "update_status": row["update_status"],
                                              "last_updated_by": row["last_updated_by"]
                                              })
                        ).add_to(segment_layer)

    fl.LayerControl().add_to(map_object)
    return map_object


def _plot_before_or_after_operation(trace_df,
                                    map_object,
                                    suffix,
                                    plotting_side="left"):
    """
    Provides plot of trace in folium.plugins.DualMap. Trace in `trace_df` is plotted in `map_object` on the side specified by `plotting_side`.

    Args:
        trace_df (pandas.DataFrame): Dataframe of pings.
        map_object (folium.plugins.DualMap): Comparison map object in which trace is to be added.
        suffix (str): Suffix of keys.
        plotting_side(str, optional): Indicates the side on which trace will be plotted, can be "left" or "right". Defaults to "left".

    Returns:
        folium.plugins.DualMap: Map containing plot of both maps.
    """

    raw_trace_geometry = []
    layers_dict = {}
    for segment_id in list(trace_df["trace_segment"].unique()):

        segment_name = "Trace_till_" + str(segment_id) + "%"
        raw_trace_segment_layer = fl.FeatureGroup(
            segment_name,
            show=False)

        if (plotting_side == "left"):
            map_object.m1.add_child(raw_trace_segment_layer)
        else:
            map_object.m2.add_child(raw_trace_segment_layer)

        layers_dict[segment_id] = raw_trace_segment_layer

    for index, row in trace_df.iterrows():

        if ((row["cleaned_latitude" + suffix] is None) or
           (row["cleaned_longitude" + suffix] is None) or
           (pd.isna(row["cleaned_latitude" + suffix])) or
           (pd.isna(row["cleaned_longitude" + suffix]))):
            continue

        raw_trace_geometry.append((row["cleaned_latitude" + suffix],row["cleaned_longitude" + suffix]))

    total_non_null_pings = len(raw_trace_geometry)
    counter = 0

    for index, row in trace_df.iterrows():

        if ((row["cleaned_latitude" + suffix] is None) or
           (row["cleaned_longitude" + suffix] is None) or
           (pd.isna(row["cleaned_latitude" + suffix])) or
           (pd.isna(row["cleaned_longitude" + suffix]))):
            continue

        segment_id = row["trace_segment"]
        segment_layer = layers_dict[segment_id]

        if (counter != (total_non_null_pings - 1)):
            start = raw_trace_geometry[counter]
            end = raw_trace_geometry[counter + 1]
            fl.PolyLine(locations=[start, end],
                        color="red",
                        weight=3,
                        opacity=1,
                        tags=["raw"],
                        ).add_to(segment_layer)
        counter += 1

        icon = plugins.BeautifyIcon(icon_shape="circle",
                                    number=index + 1,
                                    border_color="blue",
                                    background_color="transparent",
                                    inner_icon_style="font-size:12px;")

        raw_coordinates = [row["cleaned_latitude" + suffix],row["cleaned_longitude" + suffix]]

        fl.Marker(raw_coordinates,
                  icon=icon,
                  tooltip="Index: " + str(index + 1) +
                  "_ping_id: " + row["ping_id"],
                  tags=[""],
                  popup=create_general_popup({"ping_id": row["ping_id"],
                                              "input_latitude": None if pd.isna(row["input_latitude" + suffix]) else row["input_latitude" + suffix],
                                              "input_longitude": None if pd.isna(row["input_longitude" + suffix]) else row["input_longitude" + suffix],
                                              "timestamp": int(row["timestamp" + suffix]),
                                              "time_string": convert_unix_timestamp_to_human_readable(int((row["timestamp" + suffix])//1000)),
                                              "error_radius": None if pd.isna(row["error_radius" + suffix]) else row["error_radius" + suffix],
                                              "event_type": None if pd.isna(row["event_type" + suffix]) else row["event_type" + suffix],
                                              "force_retain": row["force_retain" + suffix],
                                              "cleaned_latitude": None if pd.isna(row["cleaned_latitude" + suffix]) else row["cleaned_latitude" + suffix],
                                              "cleaned_longitude": None if pd.isna(row["cleaned_longitude" + suffix]) else row["cleaned_longitude" + suffix],
                                              "update_status": row["update_status" + suffix],
                                              "last_updated_by": row["last_updated_by" + suffix]})
                        ).add_to(segment_layer)

    return map_object


def plot_cleaning_comparison_map_bw_two_traces(trace_before_operation,
                                               trace_after_operation,
                                               map_object) -> fl.plugins.DualMap:
    """
    Provides comparison plot of traces.
    Trace of smaller length(in terms of cleaned_latitude and cleaned_longitude) is plotted on left side of map, whereas the other trace is plotted on right side of map.
    If length of both trace is same then `trace_before_operation` is plotted on left side of map and `trace_after_operation` is plotted on right side of map.

    Args:
        trace_before_operation (list): First trace to be added in comparison map.
        trace_after_operation (list): Second trace to be added in comparison map.
        map_object (folium.plugins.DualMap): Comparison map in which traces are to be added.

    Returns:
        folium.plugins.DualMap: Map containing plot of both traces.
    """

    if (len(trace_before_operation) <= len(trace_after_operation)):

        trace_before_operation_df = pd.DataFrame(trace_before_operation)

        trace_after_operation_df = pd.DataFrame(trace_after_operation)

        before_operation_suffix = "_before"
        after_operation_suffix = "_after"

    else:
        trace_before_operation_df = pd.DataFrame(trace_after_operation)

        trace_after_operation_df = pd.DataFrame(trace_before_operation)

        before_operation_suffix = "_after"
        after_operation_suffix = "_before"

    trace_df = pd.merge(trace_after_operation_df,
                        trace_before_operation_df,
                        on=["ping_id"],
                        how="left",
                        suffixes=[after_operation_suffix, before_operation_suffix])

    trace_df["order"] = trace_df["ping_id"].apply(lambda x: trace_after_operation_df.index[trace_after_operation_df["ping_id"] == x][0])
    trace_df = trace_df.sort_values("order").drop(columns="order").reset_index(drop=True)

    trace_df = trace_df.reset_index(drop=True)

    # Dividing the trace into 10 segments, each containing 10% of trace.
    n = len(trace_df)
    segment_size = n // 10

    # Create the "trace_segment" column
    trace_df["trace_segment"] = ((np.arange(n) // segment_size) + 1) * 10

    # Handle any remaining rows that don"t fit evenly into segments
    trace_df["trace_segment"] = trace_df["trace_segment"].clip(upper=100)

    # Plot trace before operation
    map_object = _plot_before_or_after_operation(trace_df,
                                                 map_object,
                                                 suffix=before_operation_suffix,
                                                 plotting_side="left")
 
    # Plot trace after operation
    map_object = _plot_before_or_after_operation(trace_df,
                                                 map_object,
                                                 suffix=after_operation_suffix,
                                                 plotting_side="right")

    fl.LayerControl().add_to(map_object)

    return map_object


def _plot_stopping_map(trace_df,
                       map_object,
                       suffix,
                       plotting_side="right"):
    """
    Provides plot of stopping pings. Stopping pings are extracted from `trace_df` and added to `map_object`.

    Args:
        trace_df (pandas.DataFrame): Dataframe of pings.
        map_object (folium.plugins.DualMap): Map in which trace is to be added.
        suffix (str): Suffix of keys.
        plotting_side (str, optional): Indicates the side on which trace will be plotted, can be "left" or "right". Defaults to "left".

    Returns:
        folium.plugins.DualMap: Map containing plot of both maps.
    """

    layers_dict = {}

    # Create segment wise layers
    for segment_id in list(trace_df["trace_segment"].unique()):
        segment_name = "Trace_till_" + str(segment_id) + "%"
        raw_trace_segment_layer = fl.FeatureGroup(segment_name,
                                                  show=False)

        if (plotting_side == "left"):
            map_object.m1.add_child(raw_trace_segment_layer)
        else:
            map_object.m2.add_child(raw_trace_segment_layer)

        layers_dict[segment_id] = raw_trace_segment_layer

    # Replace values using replacement_dict
    replacement_dict = {"input_latitude": {None: np.nan},"input_longitude": {None: np.nan}}
    
    trace_df.replace(replacement_dict, inplace=True)
    trace_df.dropna(subset=["input_latitude" + suffix, "input_longitude" + suffix],
                    inplace=True)
    
    trace_df = trace_df[(trace_df["stop_event_status" + suffix] == True)]

    # Plot map for each stop pings group
    grouped = trace_df.groupby(f"stop_event_sequence_number{suffix}")
    prev_rp_ping = [None, None]
    polyline_color = "red"
    index = 0

    for name, group in grouped:
        # Get representative_latitude, representative_longitude of stationary event
        representative_latitude = group[f"representative_stop_event_latitude{suffix}"].iloc[0]
        representative_longitude = group[f"representative_stop_event_longitude{suffix}"].iloc[0]
        # Get batch size, stationary time, start and end time
        batch_size = group.shape[0]

        # Get start and end time of stationary group
        start_time = group[f"timestamp{suffix}"].min()
        start_time_hr = convert_unix_timestamp_to_human_readable(start_time//1000)
        
        end_time = group[f"timestamp{suffix}"].max()
        end_time_hr = convert_unix_timestamp_to_human_readable(end_time//1000)

        # Get total stationary time of the stationary group in human readable format
        stationary_event_time_hr = convert_time_interval_to_human_readable(((end_time//1000) - (start_time//1000)), format="ms")

        # Get segment id and segment layer
        segment_id = group["trace_segment"].iloc[0]
        segment_layer = layers_dict[segment_id]

        current_rp_ping = [representative_latitude,representative_longitude]

        # If current ping is valid, add it to map layer
        if not (pd.isna(current_rp_ping[0])
                or pd.isna(current_rp_ping[1])):
            if (None not in prev_rp_ping):
                fl.PolyLine(locations=[prev_rp_ping, current_rp_ping],
                            color=polyline_color,
                            weight=3,
                            opacity=1,
                            ).add_to(segment_layer)

            prev_rp_ping = current_rp_ping

            # Create folium icon
            icon = plugins.BeautifyIcon(icon_shape="circle",
                                        number=index + 1,
                                        border_color="blue",
                                        background_color="transparent",
                                        inner_icon_style="font-size:12px;")

            # Create folium marker
            fl.Marker(current_rp_ping,
                        icon=icon,
                        tooltip="stop_sequence_number: " + str(index + 1),
                        tags=[""],
                        popup=create_general_popup({"stop_sequence_number": name,
                                                    "representative_latitude": representative_latitude,
                                                    "representative_longitude": representative_longitude,
                                                    "batch_size": batch_size,
                                                    "start_time": start_time_hr,
                                                    "end_time": end_time_hr,
                                                    "stationary_time": stationary_event_time_hr},)
                        ).add_to(segment_layer)
            
            index += 1
    return map_object


def plot_stop_comparison_map(left_hand_trace,
                             right_hand_trace,
                             map_object):
    """
    Provides comparison plot of raw trace and stopping pings. Stopping pings are extracted from dataframe of pings provided in `trace_df` argument.

    Args:
        left_hand_trace (list): First trace to be added in comparison map. Plotted on left hand side.
        right_hand_trace (list): Second trace to be added in comparison map. Plotted on right hand side.
        map_object (folium.plugins.DualMap): Map object in which trace is to be added.

    Returns:
        folium.plugins.DualMap: Map containing plot of both maps.
    """

    left_hand_trace_df = pd.DataFrame(left_hand_trace)

    right_hand_trace_df = pd.DataFrame(right_hand_trace)

    before_operation_suffix = "_before"
    after_operation_suffix = "_after"

    trace_df = pd.merge(right_hand_trace_df,
                        left_hand_trace_df,
                        on=["ping_id"],
                        how="left",
                        suffixes=[after_operation_suffix, before_operation_suffix])

    trace_df["order"] = trace_df["ping_id"].apply(lambda x: right_hand_trace_df.index[right_hand_trace_df["ping_id"] == x][0])
    trace_df = trace_df.sort_values("order").drop(columns="order").reset_index(drop=True)

    trace_df = trace_df.reset_index(drop=True)

    # Dividing the trace into 10 segments, each containing 10% of trace.
    n = len(trace_df)
    segment_size = n // 10

    # Create the "trace_segment" column
    trace_df["trace_segment"] = ((np.arange(n) // segment_size) + 1) * 10
    
    # Handle any remaining rows that don't fit evenly into segments
    trace_df["trace_segment"] = trace_df["trace_segment"].clip(upper=100)

    # Plot trace before operation
    map_object = _plot_before_or_after_operation(trace_df,
                                                 map_object,
                                                 suffix=before_operation_suffix,
                                                 plotting_side="left")

    # Plot trace after operation
    map_object = _plot_stopping_map(
                 trace_df,
                 map_object,
                 suffix=after_operation_suffix,
                 plotting_side="right")

    fl.LayerControl().add_to(map_object)

    return map_object
