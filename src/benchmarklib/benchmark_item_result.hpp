#pragma once

#include <atomic>
#include "sql/sql_pipeline.hpp"

#include "benchmark_config.hpp"

namespace opossum {

// Stores the result of ALL runs of a single benchmark item (e.g., TPC-H query 5).
struct BenchmarkItemResult {
  BenchmarkItemResult();

  // Need to explicitly implement move, because std::atomic implicitly deletes it...
  BenchmarkItemResult(BenchmarkItemResult&& other) noexcept;
  BenchmarkItemResult& operator=(BenchmarkItemResult&& other) noexcept;

  std::atomic<size_t> num_iterations = 0;

  // Used only for BenchmarkMode::Ordered mode
  Duration duration_of_all_runs{0};

  // Stores the runtime of the iterations of this item
  tbb::concurrent_vector<Duration> durations;

  // Holds one entry per execution of this item. The inner vector holds one entry per SQLPipeline executed as part of
  // this item. For benchmarks like TPC-H, where each item corresponds to a single TPC-H query, this vector always has
  // a size of 1. For others, like TPC-C, there are multiple SQL queries executed and thus multiple entries in the
  // inner vector.
  tbb::concurrent_vector<std::vector<SQLPipelineMetrics>> metrics;

  // The *optional* is set if the verification was executed; the *bool* is true if the verification succeeded.
  std::optional<bool> verification_passed;
};

}  // namespace opossum
