#!/usr/bin/env python3

import argparse
import subprocess
import sys

from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--hyrise_path", required=True, type=str)
parser.add_argument("--scale_factor", type=float)
parser.add_argument('-d', '--debug', action='store_true')

args = parser.parse_args()

assert Path(args.hyrise_path).exists()

subprocess.run(["ninja", "-C", args.hyrise_path])

scale_factors = [args.scale_factor] if args.scale_factor else [1.0, 5.0, 10.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0]
if args.debug and not args.scale_factor:
    scale_factors = [args.scale_factor] if args.scale_factor else [0.1, 1.0, 10.0]

for scale_factor in scale_factors:
  for config in ["CSV", "NONE", "DB_Q3_COLUMNS", "Q3_COLUMNS"]:
    print(f"####\n#### SF {scale_factor} - {config}\n####")

    subprocess.run([f"./{args.hyrise_path}/hyrisePlayground", str(scale_factor)], env={"COLUMN_CONFIGURATION": config}) 

    subprocess.run(["Rscript", "data_integration__plot.R"])
