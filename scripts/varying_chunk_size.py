#!/usr/bin/env python3

import subprocess

for log_chunk_count in range(1, 21): # up to 1M chunks
  chunk_count = 2 ** log_chunk_count
  subprocess.run(["./hyriseBenchmarkTPCH", "-c", str(chunk_count), "-o", str(chunk_count) + "chunks.json", "--query_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/queries", "--table_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/tables/10M"])