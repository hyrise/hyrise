#pragma once

#include <atomic>
#include "sql/sql_pipeline.hpp"

#include "benchmark_config.hpp"

namespace opossum {

struct QueryBenchmarkResult {
  QueryBenchmarkResult();

  // Need to explicitly implement move, because std::atomic implicitly deletes it...
  QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept;
  QueryBenchmarkResult& operator=(QueryBenchmarkResult&&) = default;

  std::atomic<size_t> num_iterations = 0;

  // Using uint64_t instead of std::chrono to be able to use fetch_add()
  std::atomic<uint64_t> duration_ns{0};

  tbb::concurrent_vector<SQLPipelineMetrics> metrics;

  std::optional<bool> verification_passed;
};

}  // namespace opossum
