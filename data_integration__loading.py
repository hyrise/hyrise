#!/usr/bin/env python3

import argparse
import subprocess

from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--hyrise_path", required=True, type=str)
parser.add_argument("--scale_factor", type=float)

args = parser.parse_args()

assert Path(args.hyrise_path).exists()

subprocess.run(["ninja", "-C", args.hyrise_path])

scale_factors = [args.scale_factor] if args.scale_factor else [1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]

for scale_factor in scale_factors:
  print(f"####\n#### SF {scale_factor}\n####")
  for config in ["NONE", "DB_Q3_COLUMNS", "Q3_COLUMNS"]:
    subprocess.run([f"./{args.hyrise_path}/hyrisePlayground", str(scale_factor)], env={"COLUMN_CONFIGURATION": config}) 

subprocess.run(["Rscript", "data_integration__plot.R"])
