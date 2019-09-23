#!/usr/bin/python3

# Given a range of benchmark jsons created by run_benchmarks_over_commit_range.sh, plots a graph showing the
# performance development over time

import matplotlib.pyplot as plt
import os
import pandas as pd
from pydriller import RepositoryMining
import re
import sys

if(len(sys.argv) != 4):
    exit("Usage: " + sys.argv[0] + " <path to Hyrise> <first commit> <last commit>")

results = []
column_names = ['Commit']

for commit in RepositoryMining(sys.argv[1], from_commit=sys.argv[2], to_commit=sys.argv[3]).traverse_commits():
	result = []
	result.append(commit.hash[:6] + " " + commit.msg.partition('\n')[0])

	json_path = "auto_" + commit.hash + ".json"

	if not os.path.exists(json_path):
		print("JSON file not found: " + json_path)
		for column in column_names:
			result.append(None)
		continue

	with open(json_path) as file:
		# If column headers are unset, set them
		if len(column_names) == 1:
			for name in re.findall(r'name": "(.*)"', file.read()):
				column_names.append(name)

			file.seek(0)

		# Quick and dirty way to retrieve the throughput. This is "better" than parsing the JSON because while the JSON has
		# changed over time, this field has been available for a long time.
		for items_per_second in re.findall(r'items_per_second": ([0-9]+\.[0-9]*)', file.read()):
			result.append(float(items_per_second))

		if len(result) != len(column_names):
			exit("Mismatching number of benchmarks, starting with commit " + commit)

	results.append(result)

df = pd.DataFrame(results)
df.columns = column_names

# Normalize dataframe so that all values are relative to the first throughput of that benchmark
df.iloc[:,1:] = df.iloc[:,1:].apply(lambda x: x / x[0], axis=0)

print(df)

df.plot.line(x='Commit', figsize=(64, 18))
plt.xticks(df.index, df['Commit'], rotation=90)
plt.ylabel('Throughput relative to first commit')
plt.tight_layout()
plt.savefig('output.pdf')
