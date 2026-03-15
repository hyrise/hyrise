#!/usr/bin/env python3

import argparse
import json
import re
from pathlib import Path

import matplotlib.pyplot as plt


def _load_latencies_per_query(json_path: Path) -> dict[str, float]:
	with json_path.open("r", encoding="utf-8") as infile:
		benchmark_json = json.load(infile)

	latencies_s = {}
	for benchmark in benchmark_json["benchmarks"]:
		query_name = benchmark["name"]
		successful_runs = benchmark.get("successful_runs", [])
		if not successful_runs:
			continue

		# Aggregate benchmark duration is in ns across all successful runs.
		avg_duration_ns = benchmark["duration"] / len(successful_runs)
		latencies_s[query_name] = avg_duration_ns / 1_000_000_000.0

	return latencies_s


def _query_sort_key(query_name: str) -> tuple[int, str, str]:
	match = re.fullmatch(r"(\d+)([A-Za-z]*)", query_name)
	if match:
		return int(match.group(1)), match.group(2).lower(), query_name
	return 10_000, "", query_name


def _common_sorted_queries(base: dict[str, float], reduce: dict[str, float]) -> list[str]:
	queries = sorted(set(base.keys()) & set(reduce.keys()), key=_query_sort_key)
	if not queries:
		raise ValueError("No overlapping queries in benchmark files.")
	return queries


def _plot_subplot(ax: plt.Axes, base_json: Path, reduce_json: Path, title: str) -> None:
	base_latencies = _load_latencies_per_query(base_json)
	reduce_latencies = _load_latencies_per_query(reduce_json)
	queries = _common_sorted_queries(base_latencies, reduce_latencies)

	x_values = [base_latencies[query] for query in queries]
	y_values = [reduce_latencies[query] for query in queries]

	improved_x: list[float] = []
	improved_y: list[float] = []
	stable_x: list[float] = []
	stable_y: list[float] = []
	regressed_x: list[float] = []
	regressed_y: list[float] = []

	for base, reduce in zip(x_values, y_values):
		if reduce <= base * 0.95:
			improved_x.append(base)
			improved_y.append(reduce)
		elif reduce >= base * 1.05:
			regressed_x.append(base)
			regressed_y.append(reduce)
		else:
			stable_x.append(base)
			stable_y.append(reduce)

	if improved_x:
		ax.scatter(improved_x, improved_y, s=18, color="#3F9D51", alpha=0.92, linewidths=0.0, label="Improved (>=5%)")
	if regressed_x:
		ax.scatter(regressed_x, regressed_y, s=18, color="#D86A7A", alpha=0.92, linewidths=0.0, label="Regressed (>=5%)")
	if stable_x:
		ax.scatter(stable_x, stable_y, s=16, color="#7F7F7F", alpha=0.85, linewidths=0.0, label="Within +/-5%")

	min_val = min(min(x_values), min(y_values), 0.0)
	max_val = max(max(x_values), max(y_values))
	limit_max = max_val * 1.18

	ax.plot([min_val, limit_max], [min_val, limit_max], linestyle="--", color="#9C9C9C", linewidth=1.0)
	ax.set_xscale("symlog", linthresh=0.1)
	ax.set_yscale("symlog", linthresh=0.1)
	ax.set_xlim(min_val, limit_max)
	ax.set_ylim(min_val, limit_max)
	ax.grid(True, which="both", linestyle="--", linewidth=0.55, color="#CFCFCF", alpha=0.8)
	ax.set_title(title, loc="left")


def plot_job_scatter(data_dir: Path) -> Path:
	plt.rcParams.update(
		{
			"font.family": "serif",
			"font.serif": ["DejaVu Serif", "Times New Roman", "Times"],
			"axes.titlesize": 11,
			"axes.labelsize": 10,
			"xtick.labelsize": 9,
			"ytick.labelsize": 9,
			"legend.fontsize": 8,
		}
	)

	fig, axes = plt.subplots(2, 1, figsize=(4.8, 6.0), dpi=200, sharex=True, sharey=True)

	_plot_subplot(
		axes[0],
		data_dir / "master_joinorder_st.json",
		data_dir / "reduce_joinorder_st.json",
		"(a) ST",
	)
	_plot_subplot(
		axes[1],
		data_dir / "master_joinorder_mt.json",
		data_dir / "reduce_joinorder_mt.json",
		"(b) MT",
	)

	axes[0].set_ylabel("Latency w/ Reduce [s]")
	axes[1].set_ylabel("Latency w/ Reduce [s]")
	axes[1].set_xlabel("Base latency [s]")

	handles, labels = axes[0].get_legend_handles_labels()
	if handles:
		fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.992))

	fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.962))

	pdf_path = data_dir / "joinorder_scatter_st_mt.pdf"
	fig.savefig(pdf_path, transparent=True)
	plt.close(fig)
	return pdf_path


def main() -> None:
	parser = argparse.ArgumentParser(description="Plot Join Order benchmark base vs reduce latency scatter plots for ST and MT.")
	parser.add_argument(
		"data_dir",
		nargs="?",
		default="visualization/data/2026-03-11_inlined_penalty",
		help="Directory containing master/reduce Join Order benchmark JSON files.",
	)
	args = parser.parse_args()

	pdf_path = plot_job_scatter(Path(args.data_dir))
	print(f"Wrote {pdf_path}")


if __name__ == "__main__":
	main()
