#pragma once

#include <atomic>
#include <tbb/concurrent_vector.h> // NEEDEDINCLUDE

#include "benchmark_config.hpp"

namespace opossum {

struct QueryBenchmarkResult : public Noncopyable {
  QueryBenchmarkResult();

  QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept;

  QueryBenchmarkResult& operator=(QueryBenchmarkResult&&) = default;

  std::atomic<size_t> num_iterations = 0;
  Duration duration = Duration{};
  tbb::concurrent_vector<Duration> iteration_durations;
};

}  // namespace opossum
