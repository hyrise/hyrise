#!/usr/bin/env python3

import subprocess

for log_chunk_size in range(8, 25): # from 256 to 16M rows per chunk
  chunk_size = 2 ** log_chunk_size
  subprocess.run(["./hyriseBenchmarkFileBased", "-c", str(chunk_size), "-o", "chunksize_" + str(chunk_size) + ".json", "--query_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/queries", "--table_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/tables/10M"])