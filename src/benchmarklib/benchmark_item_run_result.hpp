#pragma once

#include <atomic>

#include "benchmark_config.hpp"
#include "sql/sql_pipeline.hpp"

namespace opossum {

// Stores the result of a SINGLE run of a single benchmark item (e.g., one execution of TPC-H query 5).
struct BenchmarkItemRunResult {
  BenchmarkItemRunResult(Duration begin, Duration duration, std::vector<SQLPipelineMetrics> metrics);

  // Stores the begin timestamp of this run (measured as time since start of benchmark)
  Duration begin;

  // Stores the runtime of this run
  Duration duration;

  // Holds one entry per SQLPipeline executed as part of a run of this item. For benchmarks like TPC-H, where each
  // item corresponds to a single TPC-H query, this vector always has a size of 1. For others, like TPC-C, there
  // are multiple SQL queries executed and thus multiple entries in the inner vector.
  std::vector<SQLPipelineMetrics> metrics;
};

}  // namespace opossum
