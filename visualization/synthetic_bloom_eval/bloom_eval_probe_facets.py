#!/usr/bin/env python3

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import pandas as pd
import seaborn as sns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create bloom-eval facet plots for probe time only."
    )
    parser.add_argument("csv_file", type=Path, help="Path to input CSV file")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory for generated PDFs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    csv_file = args.csv_file
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file does not exist: {csv_file}")

    out_dir = args.out_dir or (csv_file.parent / "probe_time_facets")
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.read_csv(csv_file)
    raw_df = raw_df[raw_df["hash_function"] != 0].copy()
    raw_df["filter_rate"] = 1 - (raw_df["hits"] / raw_df["vector_size"])

    hash_name_map = {
        0: "std::hash",
        1: "boost::hash_combine",
        2: "xxHash",
        3: "degski64",
    }

    grouping_columns = [
        "vector_size",
        "distinctiveness",
        "overlap",
        "filter_size",
        "hash_function",
        "k",
    ]
    raw_df = raw_df[raw_df.groupby(grouping_columns).cumcount() >= 2].reset_index(drop=True)

    aggregated = (
        raw_df.groupby(grouping_columns, as_index=False)
        .agg(
            median_probe_time_ns=("probe_time_ns", "median"),
            filter_rate=("filter_rate", "first"),
        )
    )

    aggregated["median_probe_time_ms"] = aggregated["median_probe_time_ns"] / 1_000_000
    aggregated["hash_name"] = aggregated["hash_function"].map(hash_name_map)

    vector_sizes = sorted(aggregated["vector_size"].unique())
    distinctiveness_values = sorted(aggregated["distinctiveness"].unique())
    hash_ids_present = sorted(aggregated["hash_function"].unique())
    hash_order = [hash_name_map[key] for key in hash_ids_present]
    marker_map = {1: "o", 2: "s", 3: "^", 4: "D"}
    k_order = sorted(aggregated["k"].unique())
    palette = dict(zip(hash_order, sns.color_palette("deep", n_colors=len(hash_order))))

    sns.set_theme(style="whitegrid")

    for vector_size in vector_sizes:
        for distinctiveness in distinctiveness_values:
            subset = aggregated[
                (aggregated["vector_size"] == vector_size)
                & (aggregated["distinctiveness"] == distinctiveness)
            ]
            if subset.empty:
                continue

            grid = sns.FacetGrid(
                subset,
                row="overlap",
                col="filter_size",
                margin_titles=True,
                height=3.6,
                aspect=1.35,
            )

            def scatterplot_with_lines(data, **kwargs):
                sns.scatterplot(
                    data=data,
                    x="filter_rate",
                    y="median_probe_time_ms",
                    hue="hash_name",
                    style="k",
                    hue_order=hash_order,
                    style_order=k_order,
                    markers=marker_map,
                    palette=palette,
                    s=48,
                    legend=False,
                    **kwargs,
                )
                for hash_name in hash_order:
                    hash_subset = data[data["hash_name"] == hash_name].sort_values("filter_rate")
                    if hash_subset.empty:
                        continue
                    plt.plot(
                        hash_subset["filter_rate"],
                        hash_subset["median_probe_time_ms"],
                        color=palette[hash_name],
                        linewidth=1.2,
                        alpha=0.8,
                    )

            grid.map_dataframe(scatterplot_with_lines)
            grid.set_titles(row_template="Overlap: {row_name}", col_template="Filter Size: {col_name}")
            grid.set_axis_labels("Filter Rate", "Probe Time (ms)")

            hash_handles = [
                Line2D([0], [0], color=palette[name], marker="o", linestyle="-", label=name)
                for name in hash_order
            ]
            k_handles = [
                Line2D(
                    [0],
                    [0],
                    color="black",
                    marker=marker_map[k_value],
                    linestyle="",
                    label=f"k={k_value}",
                )
                for k_value in k_order
            ]
            grid.fig.subplots_adjust(right=0.76)
            legend_ax = grid.fig.add_axes([0.82, 0.18, 0.17, 0.64])
            legend_ax.axis("off")
            legend_ax.legend(
                handles=hash_handles + k_handles,
                labels=[handle.get_label() for handle in hash_handles + k_handles],
                title="Hash Function / k",
                loc="center",
                frameon=True,
            )
            grid.fig.suptitle(
                f"Probe Time Facets | vector_size={vector_size} | distinctiveness={distinctiveness}",
                y=1.02,
            )

            out_file = out_dir / f"probe_facets_{vector_size}_{distinctiveness}.pdf"
            grid.fig.savefig(out_file)
            plt.close(grid.fig)
            print(f"Saved {out_file}")


if __name__ == "__main__":
    main()
