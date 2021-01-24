#! /usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme()

"""
Assumptions:
	`rel` is gcc build folder, `rel_clang` is clang
"""
output_csv_filename = 'radix_test.csv'

output = open(output_csv_filename, 'w')
output.write('COMPILER,SCHEDULER_USED,CACHE_SIZE_FACTOR,SEMI_ADAPTION_FACTOR,QUERY_ID,RUNTIME_MS\n')

with open('radix_test', 'r') as file:
	config = {}
	query = ''
	for line in file:
		if line.startswith('  -> cache of '):
			line_cache_start = line[line.find('  -> cache of ') + len('  -> cache of '):]
			cache_factor = float(line_cache_start[:line_cache_start.find(' ')])
			config['cache_factor'] = cache_factor

			line_semi_start = line[line.find(' and semi of ') + len(' and semi of '):]
			semi_factor = float(line_semi_start[:line_semi_start.find(' ')])
			config['semi_factor'] = semi_factor

			line_dir_start = line[line.find('(') + 1:]
			dir =  line_dir_start[:line_dir_start.find(' ')]
			config['compiler'] = 'GCC' if dir == 'rel' else 'Clang'

			config['scheduler'] = True if '--scheduler' in line else False

		if 'Benchmarking TPC-H' in line:
			query = f"Q{line[line.rfind(' ') + 1:]}".strip()

		if line.startswith('  -> Executed '):
			line_latency_start = line[line.find('(Latency: ') + len('(Latency: '):]
			latency = float(line_latency_start[:line_latency_start.find(' ')])
			assert('ms/iter' in line)
			
			output.write(f"\"{config['compiler']}\",{config['scheduler']},{config['cache_factor']},{config['semi_factor']},\"{query}\",{latency}\n")

output.close()

df = pd.read_csv(output_csv_filename)


for compiler in pd.unique(df.COMPILER):
	for scheduler in pd.unique(df.SCHEDULER_USED):
		for query in list(pd.unique(df.QUERY_ID)) + ['all']:
			# Draw a heatmap with the numeric values in each cell
			f, ax = plt.subplots(figsize=(20, 15))

			if query == 'all':
				df_copy = df.query('COMPILER == @compiler and SCHEDULER_USED == @scheduler').copy()
				df_copy = df_copy.groupby(["COMPILER", "SCHEDULER_USED", "CACHE_SIZE_FACTOR", "SEMI_ADAPTION_FACTOR"]).mean("RUNTIME_MS").reset_index()
			else:
				df_copy = df.query('COMPILER == @compiler and SCHEDULER_USED == @scheduler and QUERY_ID == @query').copy()

			df_pivot = df_copy.pivot("CACHE_SIZE_FACTOR", "SEMI_ADAPTION_FACTOR", "RUNTIME_MS")
			sns.heatmap(df_pivot, annot=True, fmt="f", ax=ax)
			plt.savefig(f'radix_test__{compiler}_{scheduler}_{query}.pdf')