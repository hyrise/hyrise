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
  Duration duration = Duration{};
  tbb::concurrent_vector<std::shared_ptr<SQLPipelineMetrics>> metrics;

  std::optional<bool> verification_passed;
};

}  // namespace opossum
