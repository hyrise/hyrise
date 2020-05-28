#!/usr/bin/env python3

import subprocess, os

for log_chunk_count in range(1, 21): # up to 1M chunks
  chunk_count = 2**log_chunk_count

  env = os.environ.copy()
  env["CLUSTERING"] = "partitioning.json"

  print("""
{
  "ACDOCA" : [
        {"column_name": "DOCNR", "partitions": %i, "mode": "size"},
  ]
}
    """ % (chunk_count), file=open("partitioning.json", "w"))

  subprocess.run(["echo", "./hyriseBenchmarkFileBased", "-o", str(chunk_count) + "chunks.json", "--query_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/queries", "--table_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/tables/10M"], env=env)
