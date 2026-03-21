#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def _load_latencies_per_query(json_path: Path) -> dict[str, float]:
	with json_path.open("r", encoding="utf-8") as infile:
		benchmark_json = json.load(infile)

	latencies_ms = {}
	for benchmark in benchmark_json["benchmarks"]:
		query_name = benchmark["name"]
		successful_runs = benchmark.get("successful_runs", [])
		if not successful_runs:
			continue

		# Aggregate benchmark duration is in ns across all successful runs.
		avg_duration_ns = benchmark["duration"] / len(successful_runs)
		latencies_ms[query_name] = avg_duration_ns / 1_000_000.0

	return latencies_ms


def _compute_latency_change_percent(old_json: Path, new_json: Path) -> dict[str, float]:
	old_latencies = _load_latencies_per_query(old_json)
	new_latencies = _load_latencies_per_query(new_json)

	common_queries = sorted(set(old_latencies.keys()) & set(new_latencies.keys()), key=_query_sort_key)
	if not common_queries:
		raise ValueError(f"No overlapping queries between {old_json} and {new_json}.")

	changes = {}
	for query in common_queries:
		old_value = old_latencies[query]
		new_value = new_latencies[query]
		changes[query] = ((new_value - old_value) / old_value) * 100.0

	return changes


def _query_sort_key(query_name: str) -> tuple[int, int]:
	major_str, minor_str = query_name.split(".", maxsplit=1)
	return int(major_str), int(minor_str)


def _grouped_positions(queries: list[str]) -> tuple[np.ndarray, list[str]]:
	# SSB has 13 queries in four flights.
	flight_order = [["1.1", "1.2", "1.3"], ["2.1", "2.2", "2.3"], ["3.1", "3.2", "3.3", "3.4"], ["4.1", "4.2", "4.3"]]
	flight_gap = 1.0

	query_set = set(queries)
	ordered_queries: list[str] = []
	x_positions: list[float] = []
	x = 0.0

	for flight in flight_order:
		flight_queries = [query for query in flight if query in query_set]
		if not flight_queries:
			continue

		for query in flight_queries:
			ordered_queries.append(query)
			x_positions.append(x)
			x += 1.0

		x += flight_gap

	if set(ordered_queries) != query_set:
		extra_queries = sorted(query_set - set(ordered_queries), key=_query_sort_key)
		for query in extra_queries:
			ordered_queries.append(query)
			x_positions.append(x)
			x += 1.0

	return np.array(x_positions), ordered_queries


def _plot_sf(data_dir: Path, scale_factor: str) -> None:
	st_changes = _compute_latency_change_percent(
		data_dir / f"master_starschema_s{scale_factor}_st.json",
		data_dir / f"reduce_starschema_s{scale_factor}_st.json",
	)
	mt_changes = _compute_latency_change_percent(
		data_dir / f"master_starschema_s{scale_factor}_mt.json",
		data_dir / f"reduce_starschema_s{scale_factor}_mt.json",
	)

	common_queries = sorted(set(st_changes.keys()) & set(mt_changes.keys()), key=_query_sort_key)
	x, queries = _grouped_positions(common_queries)
	bar_width = 0.38

	st_values = [st_changes[query] for query in queries]
	mt_values = [mt_changes[query] for query in queries]

	plt.rcParams.update(
		{
			"font.family": "serif",
			"font.serif": ["DejaVu Serif", "Times New Roman", "Times"],
			"axes.titlesize": 14,
			"axes.labelsize": 12,
			"xtick.labelsize": 11,
			"ytick.labelsize": 12,
			"legend.fontsize": 13,
		}
	)

	fig, ax = plt.subplots(figsize=(11.2, 5.2), dpi=200)
	fig.patch.set_alpha(0.0)
	ax.set_facecolor("none")

	ax.bar(x - bar_width / 2, st_values, bar_width, label="ST", color="#4E7898")
	ax.bar(x + bar_width / 2, mt_values, bar_width, label="MT", color="#CA5A37")

	ax.axhline(0.0, color="black", linewidth=1)
	ax.set_ylabel("Latency change (%)")
	ax.set_xticks(x)
	ax.set_xticklabels([f"Q{query}" for query in queries], rotation=45, ha="right")
	ax.grid(axis="y", linestyle="--", linewidth=1.0, color="#BFBFBF")
	ax.set_axisbelow(True)
	ax.legend(
		loc="upper center",
		bbox_to_anchor=(0.5, 1.06),
		ncol=2,
		framealpha=1.0,
		facecolor="white",
		edgecolor="#BBBBBB",
	)

	all_values = st_values + mt_values
	y_min = min(all_values) - 6.0
	y_max = max(all_values) + 8.0
	ax.set_ylim(y_min, y_max)

	fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.98))

	pdf_path = data_dir / f"ssb_sf{scale_factor}_latency_change_st_mt.pdf"
	fig.savefig(pdf_path, transparent=True)
	plt.close(fig)

	print(f"Wrote {pdf_path}")


def main() -> None:
	parser = argparse.ArgumentParser(description="Plot SSB latency changes for SF1 and SF10.")
	parser.add_argument(
		"data_dir",
		nargs="?",
		default="visualization/data/2026-03-11_inlined_penalty",
		help="Directory containing master/reduce SSB benchmark JSON files.",
	)
	args = parser.parse_args()

	data_dir = Path(args.data_dir)
	_plot_sf(data_dir, "1")
	_plot_sf(data_dir, "10")


if __name__ == "__main__":
	main()
