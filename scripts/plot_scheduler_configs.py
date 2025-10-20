#!/usr/bin/env python3

import argparse
import atexit
import json
import matplotlib.pyplot as plt
import multiprocessing
import os
import pandas as pd
import seaborn as sns
import socket
import sys

from datetime import datetime
from pathlib import Path
from statistics import geometric_mean

MAX_CORE_COUNT = multiprocessing.cpu_count()


def get_parser():
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--host_dir",
    action="store",
    type=str,
    required=True,
    help="Which directoy to pull results from for plotting."
  )

  return parser

def get_results(result_file_path):
  with open(file) as json_file:
    result_file = json.load(json_file)

    averages = []

    for benchmark in result_file["benchmarks"]:
      averages.append(sum([item["duration"] for item in benchmark["successful_runs"]]) / len(benchmark["successful_runs"]))
      # print(benchmark["name"], " - ", averages[-1] / 1000, " us")

    sum_runtimes = sum(averages)
    geo_mean_runtimes = geometric_mean(averages)
    # print(f"Sum: {sum_runtimes / 1000} us")
    # print(f"Geomean: {geo_mean_runtimes / 1000} us")
  return (sum_runtimes, geo_mean_runtimes)


def get_settings_from_filename(result_file_path):
  def get_string_setting(input, prefix, suffix = "__"):
    assert prefix in input
    start = input.find(prefix) + len(prefix)
    return input[start:input.find(suffix, start)]

  fp = str(result_file_path)
  settings = {}
  settings["NUM_GROUPS_MIN_FACTOR"] = float(get_string_setting(fp, "__min_"))
  settings["NUM_GROUPS_MAX_FACTOR"] = float(get_string_setting(fp, "__max_"))
  settings["UPPER_LIMIT_QUEUE_SIZE_FACTOR"] = int(get_string_setting(fp, "__limit_"))
  settings["clients"] = int(get_string_setting(fp, "__clients_"))
  settings["cores"] = int(get_string_setting(fp, "__cores_", "."))

  settings["branch"] = "master" if "/master__" in fp else "martin/perf/num_groups"

  try:
    settings["benchmark"] = get_string_setting(fp, "/branch__", "__")
  except:
    settings["benchmark"] = get_string_setting(fp, "/master__", "__")

  return settings


if __name__ == "__main__":
  parser = get_parser()
  args, _ = parser.parse_known_args()

  hostname = socket.gethostname()

  df_dict = []

  for file in Path(args.host_dir).iterdir():
    if not str(file).endswith(".json"):
      continue

    settings = get_settings_from_filename(file)
    rt_sum, rt_gm = get_results(file)
    settings["RUNTIME_US"] = rt_sum
    settings["RUNTIME_GEOMEAN_US"] = rt_gm

    df_dict.append(settings)

  df = pd.DataFrame(df_dict)

  df_branch = df.query("branch != 'master'")
  df_master = df.query("branch == 'master'")

  df_merged = df_branch.merge(df_master, on=["clients", "cores", "benchmark"], suffixes=("_branch", "_master"))
  df_merged["RUNTIME_DIFF"] = df_merged.RUNTIME_US_branch / df_merged.RUNTIME_US_master
  df_merged["GEOMEAN_DIFF"] = df_merged.RUNTIME_GEOMEAN_US_branch / df_merged.RUNTIME_GEOMEAN_US_master
  df_merged = df_merged[["NUM_GROUPS_MIN_FACTOR_branch", "NUM_GROUPS_MAX_FACTOR_branch",
                         "UPPER_LIMIT_QUEUE_SIZE_FACTOR_branch", "clients", "cores", "benchmark", "RUNTIME_DIFF", "GEOMEAN_DIFF"]]
  print(str(df_merged.sort_values(by="RUNTIME_DIFF")))

  # Pivot data for each category
  def draw_heatmap(*args, **kwargs):
      data = kwargs.pop('data')
      vmin = kwargs.pop('vmin')
      vmax = kwargs.pop('vmax')
      d = data.pivot(index='NUM_GROUPS_MIN_FACTOR_branch', columns='NUM_GROUPS_MAX_FACTOR_branch', values='RUNTIME_DIFF')
      sns.heatmap(d, vmin=rt_diff_min, vmax=rt_diff_max,  **kwargs)

  for client_title, client_filter in [("Single-User", "clients == 1"), ("Multi-User", "clients > 1")]:
    # Create FacetGrid
    df_filtered = df_merged.query(client_filter)

    rt_diff_min = df_filtered['RUNTIME_DIFF'].min()
    rt_diff_max = df_filtered['RUNTIME_DIFF'].max()

    fg = sns.FacetGrid(df_filtered, row='UPPER_LIMIT_QUEUE_SIZE_FACTOR_branch', col='benchmark')
    fg.map_dataframe(draw_heatmap, vmin=rt_diff_min, vmax=rt_diff_max, cbar=True, cmap='coolwarm')
    plt.savefig(f"{args.host_dir}/{client_title}.pdf")
