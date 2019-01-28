#pragma once

#include <tbb/concurrent_vector.h> // NEEDEDINCLUDE
#include <atomic> // NEEDEDINCLUDE

#include "benchmark_config.hpp" // NEEDEDINCLUDE

namespace opossum {

struct QueryBenchmarkResult : public Noncopyable {
  QueryBenchmarkResult();

  QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept;

  QueryBenchmarkResult& operator=(QueryBenchmarkResult&&) = default;

  std::atomic<size_t> num_iterations = 0;
  Duration duration = Duration{};
  tbb::concurrent_vector<Duration> iteration_durations;

  std::optional<bool> verification_passed;
};

}  // namespace opossum
