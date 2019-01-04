#pragma once

#include "sql/sql_pipeline.hpp"

#include "benchmark_config.hpp"

namespace opossum {

struct QueryBenchmarkResult : public Noncopyable {
  QueryBenchmarkResult() { metrics.reserve(1'000'000); }

  QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
    num_iterations.store(other.num_iterations);
    duration = std::move(other.duration);
    metrics = std::move(other.metrics);
  }

  QueryBenchmarkResult& operator=(QueryBenchmarkResult&&) = default;

  std::atomic<size_t> num_iterations = 0;
  Duration duration = Duration{};
  tbb::concurrent_vector<std::shared_ptr<SQLPipelineMetrics>> metrics;
};

}  // namespace opossum
