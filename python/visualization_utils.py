import numpy as np
import pandas as pd
from itertools import product
from matplotlib import cm
from matplotlib import pyplot as plt
from operator import itemgetter


def runtime_plots(
    operator_name,
    data_frame,
    dimensions,
    x_axes,
    y_axis,
    abline=False,
    constrained_layout=True,
    y_ticks=False,
    drill_down=False,
    unit="",
):
    colors = cm.get_cmap("tab10")(np.arange(10))
    plt.style.use("ggplot")
    max_y_value = np.amax(data_frame[y_axis])
    if max_y_value > 100000 or max_y_value < 0.1:
        title_max = format(max_y_value, ".3e")
    elif max_y_value > 1000:
        title_max = round(max_y_value)
    else:
        title_max = max_y_value
    unit = f" {unit}" if unit else unit
    x_axes.sort()
    if drill_down and not len(dimensions) in [2, 3]:
        raise ValueError("Drill down only supported for two or three dimensions")
    dimension_values = [
        [("combined", "combined")]
        if dimension == "combined"
        else [(dimension, value) for value in data_frame[dimension].unique()]
        for dimension in dimensions
    ]
    if not drill_down:
        dimension_values.sort(key=itemgetter(0))
        dimension_values.sort(key=len)
    rearange = len([dimension for dimension in dimensions if not dimension == "combined"]) > 1 and len(x_axes) == 1
    vertical_filter = ["combined"]
    horizontal_filter = ["combined"]
    if drill_down:
        horizontal_dimensions = dimension_values[0]
        vertical_filter = dimension_values[1]
        vertical_dimensions = x_axes * len(vertical_filter)
        horizontal_filter = ["combined"] if len(dimensions) == 2 else dimension_values[2]
    elif rearange:
        horizontal_dimensions = np.arange(np.amax([len(dimension) for dimension in dimension_values]))
        vertical_dimensions = dimensions
    else:
        horizontal_dimensions = np.concatenate(dimension_values)
        vertical_dimensions = x_axes

    vertical_span = len(vertical_dimensions) > 1
    horizontal_width = len(horizontal_filter) * len(horizontal_dimensions)
    horizontal_span = horizontal_width > 1
    plt.rcParams["figure.figsize"] = [4 * horizontal_width, 3 * len(vertical_dimensions) + 1]
    fig, axes = plt.subplots(
        nrows=len(vertical_dimensions), ncols=horizontal_width, constrained_layout=constrained_layout
    )
    fig.suptitle(f"{operator_name}, max. {y_axis} {title_max}{unit}", size=16)
    axis_max = np.amax(data_frame[y_axis]) * 1.05
    if abline:
        if not len(x_axes) == 1:
            raise ValueError("For drawing ablines, select only one x dimension.")
        axis_max = max(np.amax(data_frame[x_axes[0]]), np.amax(data_frame[y_axis])) * 1.05
        abline_values = range(0, int(axis_max), int(axis_max / 100))

    if vertical_span and horizontal_span:
        axes_2 = axes
    elif vertical_span:
        axes_2 = [[plot] for plot in axes]
    elif horizontal_span:
        axes_2 = [axes]
    else:
        axes_2 = [[axes]]

    for vertical_index, horizontal_index in product(range(len(vertical_dimensions)), range(horizontal_width)):
        x_axis = x_axes[0] if rearange else vertical_dimensions[vertical_index]
        plot = axes_2[vertical_index][horizontal_index]
        plot.set_facecolor("w")
        plot.grid(b=True, which="major", color="#e5e5e5", linestyle="-")
        plot.tick_params(axis="both", color="#e5e5e5")
        if not drill_down and rearange and horizontal_index >= len(dimension_values[vertical_index]):
            plot.axis("off")
            continue
        y_label = y_axis
        if drill_down:
            if len(horizontal_filter) == 1:
                filter_condition = horizontal_dimensions[horizontal_index][0]
                filter_predicate = horizontal_dimensions[horizontal_index][1]
                current_vertical_filter = vertical_filter[vertical_index % len(vertical_filter)]
                vertical_condition = current_vertical_filter[0]
                vertical_predicate = current_vertical_filter[1]
                plot_data = data_frame.loc[
                    (data_frame[filter_condition] == filter_predicate)
                    & (data_frame[vertical_condition] == vertical_predicate)
                ]
                y_label = f"{vertical_condition}: {vertical_predicate}\n{y_axis}"
                plot_title = f"{filter_condition}\n{filter_predicate}"
            else:
                filter_condition = horizontal_dimensions[horizontal_index % len(horizontal_filter)][0]
                filter_predicate = horizontal_dimensions[horizontal_index % len(horizontal_filter)][1]
                current_vertical_filter = vertical_filter[vertical_index % len(vertical_filter)]
                vertical_condition = current_vertical_filter[0]
                vertical_predicate = current_vertical_filter[1]
                current_horizontal_filter = horizontal_filter[horizontal_index // len(horizontal_filter)]
                horizontal_condition = current_horizontal_filter[0]
                horizontal_predicate = current_horizontal_filter[1]
                plot_data = data_frame.loc[
                    (data_frame[filter_condition] == filter_predicate)
                    & (data_frame[vertical_condition] == vertical_predicate)
                    & (data_frame[horizontal_condition] == horizontal_predicate)
                ]
                y_label = f"{vertical_condition}: {vertical_predicate}\n{y_axis}"
                plot_title = f"{horizontal_condition}: {horizontal_predicate}\n{filter_condition}: {filter_predicate}"
        elif rearange:
            filter_condition = dimension_values[vertical_index][horizontal_index][0]
            filter_predicate = dimension_values[vertical_index][horizontal_index][1]
            plot_data = (
                data_frame
                if filter_condition == "combined"
                else data_frame[data_frame[filter_condition] == filter_predicate]
            )
            plot_title = (
                f"\n{filter_condition}" if filter_condition == "combined" else f"{filter_condition}\n{filter_predicate}"
            )
        else:
            filter_condition = horizontal_dimensions[horizontal_index][0]
            filter_predicate = horizontal_dimensions[horizontal_index][1]
            plot_data = (
                data_frame
                if filter_condition == "combined"
                else data_frame[data_frame[filter_condition] == filter_predicate]
            )
            plot_title = (
                f"\n{filter_condition}" if filter_condition == "combined" else f"{filter_condition}\n{filter_predicate}"
            )
        plot.set_title(plot_title)
        x_values = [
            x_value
            for x_value in plot_data[x_axis].unique()
            if not ((type(x_value) == float and np.isnan(x_value)) or x_value == "0")
        ]
        filtered_plot_data = plot_data[plot_data[x_axis].isin(x_values)]
        plot.scatter(filtered_plot_data[x_axis], filtered_plot_data[y_axis], c=[colors[horizontal_index % len(colors)]])
        if abline:
            plot.plot(abline_values, abline_values, c="r", linestyle="-")
            plot.set_xlim([0, axis_max])
        plot.set_ylim([0, axis_max])
        if not y_ticks:
            plot.set_yticklabels([])
        if drill_down:
            plot.set_ylabel(y_axis, rotation=90)
        if horizontal_index == 0:
            plot.set_ylabel(y_label, rotation=90)
        plot.set_xlabel(x_axis)
    if not constrained_layout:
        plt.tight_layout()
        fig.subplots_adjust(top=0.9)
    return plt
