#!/usr/bin/env python3

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


IMPROVE_COLOR = "#3F9D51"
REGRESS_COLOR = "#D86A7A"
CONNECTOR_COLOR = "#9A9A9A"


# Ablation study data: incremental changes in percent
ABLATION_DATA = {
	"TPC-H": {
		"Blocking": -0.5,
		"Selector": -2.0,
		"Min-Max": 0.9,
	},
	"SSB": {
		"Blocking": 0.1,
		"Selector": 0.6,
		"Min-Max": -1.2,
	},
	"TPC-DS": {
		"Blocking": 1.0,
		"Selector": -1.1,
		"Min-Max": -4.3,
	},
	"JOB": {
		"Blocking": 0.0,
		"Selector": 0.2,
		"Min-Max": -2.1,
	},
}


def _compute_cumulative_values(incremental: dict[str, float]) -> tuple[list[str], list[float], list[float]]:
	"""
	Compute cumulative values from incremental changes.

	Returns:
		- labels: ["Blocking", "Selector", "Min-Max"]
		- deltas: incremental percentage changes
		- cumulative: cumulative values after each step
	"""
	labels = ["Blocking", "Selector", "Min-Max"]
	deltas = [incremental[label] for label in labels]

	cumulative = []
	running = 0.0
	for delta in deltas:
		running += delta
		cumulative.append(running)

	return labels, deltas, cumulative


def _annotation_y(delta: float, top: float, bottom: float, y_span: float) -> tuple[float, str]:
	"""Place labels outside tiny bars for readability."""
	bar_height = abs(delta)
	offset = 0.035 * y_span
	if bar_height < 0.55:
		if delta >= 0:
			return top + offset, "bottom"
		return bottom - offset, "top"
	return (top + 0.01 * y_span, "bottom") if delta >= 0 else (bottom - 0.01 * y_span, "top")


def _plot_waterfall_subplot(ax: plt.Axes, benchmark_name: str, incremental: dict[str, float], y_min: float, y_max: float) -> None:
	"""Plot a single waterfall chart for one benchmark."""
	labels, deltas, cumulative = _compute_cumulative_values(incremental)
	x_pos = np.arange(len(labels))
	y_span = y_max - y_min

	starts = [0.0] + cumulative[:-1]
	for i, (x, delta, start, end) in enumerate(zip(x_pos, deltas, starts, cumulative)):
		bottom = min(start, end)
		height = abs(delta)
		color = IMPROVE_COLOR if delta < 0 else REGRESS_COLOR
		if abs(delta) < 1e-12:
			color = "#B8B8B8"

		ax.bar(
			x,
			height,
			bottom=bottom,
			width=0.58,
			color=color,
			alpha=0.9,
			edgecolor="#555555",
			linewidth=0.45,
			zorder=3,
		)

		text_y, va = _annotation_y(delta, max(start, end), min(start, end), y_span)
		ax.text(
			x,
			text_y,
			f"{delta:+.1f}%",
			ha="center",
			va=va,
			fontsize=8.8,
			fontweight="semibold",
			color="#1D1D1D",
			bbox={"boxstyle": "round,pad=0.16", "facecolor": "white", "edgecolor": "none", "alpha": 0.82},
			zorder=6,
		)

		if i < len(x_pos) - 1:
			ax.plot(
				[x + 0.29, x + 1 - 0.29],
				[end, end],
				color=CONNECTOR_COLOR,
				linewidth=0.85,
				linestyle="-",
				zorder=4,
			)

	combined = cumulative[-1]
	line_color = IMPROVE_COLOR if combined < 0 else REGRESS_COLOR
	line_x_start = -0.48
	line_x_end = x_pos[-1] + 0.29
	ax.plot([line_x_start, line_x_end], [combined, combined], color=line_color, linestyle=(0, (4, 3)), linewidth=1.1, zorder=2)

	first_intersection_left: float | None = None
	for i, (start, end) in enumerate(zip(starts, cumulative)):
		bar_low = min(start, end)
		bar_high = max(start, end)
		if bar_low - 1e-12 <= combined <= bar_high + 1e-12:
			first_intersection_left = x_pos[i] - 0.29
			break

	if first_intersection_left is None:
		combined_label_x = 0.5 * (line_x_start + line_x_end)
	else:
		combined_label_x = 0.5 * (line_x_start + first_intersection_left)
	combined_label_y = max(y_min + 0.04 * y_span, combined - 0.06 * y_span)
	ax.text(
		combined_label_x,
		combined_label_y,
		f"Combined: {combined:+.1f}%",
		ha="center",
		va="top",
		fontsize=8.9,
		color=line_color,
		bbox={"boxstyle": "round,pad=0.18", "facecolor": "white", "edgecolor": "none", "alpha": 0.88},
		clip_on=True,
		zorder=7,
	)

	ax.set_xlim(-0.5, len(x_pos) - 0.5)
	ax.set_ylim(y_min, y_max)
	ax.set_xticks(x_pos)
	ax.set_xticklabels(labels, fontsize=10)
	ax.axhline(0, color="black", linewidth=1.0, zorder=2)
	ax.grid(axis="y", linestyle="--", linewidth=0.8, color="#CFCFCF", alpha=0.7)
	ax.set_axisbelow(True)
	ax.set_title(benchmark_name, loc="left", fontsize=11, fontweight="bold")


def plot_ablation_waterfall(output_path: Path) -> None:
	"""Create a 2×2 waterfall chart for all benchmarks."""
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
	
	# Collect all cumulative values to determine common y-scale
	all_values = []
	benchmark_names = ["TPC-H", "SSB", "TPC-DS", "JOB"]
	
	for name in benchmark_names:
		_, _, cumulative = _compute_cumulative_values(ABLATION_DATA[name])
		all_values.extend(cumulative)
		all_values.append(0.0)
	
	y_min = min(all_values) - 1.4
	y_max = max(all_values) + 1.4
	
	fig, axes = plt.subplots(2, 2, figsize=(10.0, 8.0), dpi=200)
	
	for ax, benchmark_name in zip(axes.flat, benchmark_names):
		_plot_waterfall_subplot(ax, benchmark_name, ABLATION_DATA[benchmark_name], y_min, y_max)
	
	# Add shared labels
	axes[1, 0].set_xlabel("Optimization Step", fontsize=10)
	axes[1, 1].set_xlabel("Optimization Step", fontsize=10)
	axes[0, 0].set_ylabel("Cumulative Latency Change (%)", fontsize=10)
	axes[1, 0].set_ylabel("Cumulative Latency Change (%)", fontsize=10)
	
	# Create legend
	improved_patch = mpatches.Patch(color=IMPROVE_COLOR, label="Improved")
	regressed_patch = mpatches.Patch(color=REGRESS_COLOR, label="Regressed")
	fig.legend(
		handles=[improved_patch, regressed_patch],
		loc="upper center",
		ncol=2,
		frameon=False,
		bbox_to_anchor=(0.5, 0.995),
		fontsize=9,
	)
	
	fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.97))
	
	fig.savefig(output_path, transparent=True)
	plt.close(fig)
	print(f"Wrote {output_path}")


def main() -> None:
	parser = argparse.ArgumentParser(description="Plot ablation study waterfall chart for TPC-H, SSB, TPC-DS, and JOB.")
	parser.add_argument(
		"--output",
		"-o",
		type=Path,
		default=Path("visualization/ablation_study/ablation_waterfall.pdf"),
		help="Output PDF path.",
	)
	args = parser.parse_args()
	
	plot_ablation_waterfall(args.output)


if __name__ == "__main__":
	main()
