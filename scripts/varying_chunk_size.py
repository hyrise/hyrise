#!/usr/bin/env python3

import subprocess, os

for log_chunk_count in range(1, 20): # up to 256k chunks
  chunk_count = 2**log_chunk_count
  print("===== " + str(chunk_count) + " chunks =====")

  env = os.environ.copy()
  env["PARTITIONING"] = "partitioning.json"

  print("""
{
  "ACDOCA" : [
        {"column_name": "DOCNR", "partitions": %i, "mode": "size"}
  ]
}
    """ % (chunk_count), file=open("partitioning.json", "w"))

  subprocess.run(["./hyriseBenchmarkFileBased", "-o", str(chunk_count) + "chunks.json", "--query_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/queries", "--table_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/tables/10M"], env=env)
#  subprocess.run(["./hyriseBenchmarkFileBased", "-o", str(chunk_count) + "chunks.json", "--query_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/queries", "--table_path=/home/Jan.Kossmann/hyrise_partitioning/resources/benchmark/acdoca/tables/10M", "--visualize"], env=env, stdout=open(str(chunk_count) + ".log", "w"))
#  subprocess.run(["mv", "oltp_sum_receivables-PQP.svg", "oltp_sum_receivables-" + str(chunk_count) + "-PQP.svg"])
