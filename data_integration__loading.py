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

scale_factors = [args.scale_factor] if args.scale_factor else [10.0, 50.0, 100.0, 200.0, 400.0, 1000.0]

for scale_factor in scale_factors:
  for config in ["NONE", "DB_CUSTKEY_ONLY", "DB_CUSTKEY_AND_MKTSEGMENT", "CUSTKEY_ONLY", "CUSTKEY_AND_MKTSEGMENT"]:
    subprocess.run([f"./{args.hyrise_path}/hyrisePlayground", str(scale_factor)], env={"COLUMN_CONFIGURATION": config}) 

subprocess.run(["Rscript", "data_integration__plot.R"])
