#!/usr/bin/env python3
import json
import pandas as pd
import matplotlib.pyplot as plt

# Load the JSON file
with open("data/20250606_first-microbenchmarks.json") as f:
    data = json.load(f)

# Helper to extract technique and case from benchmark name
def parse_benchmark_name(name):
    # Examples:
    # StarSchemaDataMicroBenchmarkFixture/SemiJoinWorstCase
    # StarSchemaDataMicroBenchmarkFixture/PrototypeBadCase/18/2
    parts = name.split("/")
    technique_case = parts[1]
    if "Prototype" in technique_case:
        # e.g. PrototypeBadCase/18/2
        technique = technique_case.replace("WorstCase", "").replace("BestCase", "").replace("BadCase", "")
        case = "WorstCase" if "WorstCase" in technique_case else "BestCase" if "BestCase" in technique_case else "BadCase"
        # Add suffix for Prototype variants
        if len(parts) > 2:
            technique += parts[2] + parts[3]
    else:
        # e.g. SemiJoinWorstCase
        if "WorstCase" in technique_case:
            technique = technique_case.replace("WorstCase", "")
            case = "WorstCase"
        elif "BestCase" in technique_case:
            technique = technique_case.replace("BestCase", "")
            case = "BestCase"
        elif "BadCase" in technique_case:
            technique = technique_case.replace("BadCase", "")
            case = "BadCase"
        else:
            technique = technique_case
            case = "Unknown"
    return technique, case

# Prepare a dict to collect results
results = {
    "WorstCase": {},
    "BestCase": {},
    "BadCase": {},
}

for bm in data["benchmarks"]:
    technique, case = parse_benchmark_name(bm["name"])
    if case in results:
        # Use real_time as the result
        results[case][technique] = bm["real_time"]

# Convert to DataFrame
df = pd.DataFrame.from_dict(results, orient="index")
df.index.name = "Case"

# Convert from nanoseconds to milliseconds
df = df / 1_000_000

print(df)

# Plot for each case
for case in df.index:
    plt.figure(figsize=(8, 5))
    df.loc[case].plot(kind="bar")
    plt.title(f"Benchmark Results for {case}")
    plt.ylabel("real_time (ms)")  # Update unit to milliseconds
    plt.xlabel("Reduction Technique")
    plt.tight_layout()
    plt.savefig(f"microbenchmark_{case}.pdf")  # Save as PDF
    plt.close()  # Close the figure to free memory
