#pragma once

#include "benchmark_config.hpp"

namespace opossum {

struct QueryBenchmarkResult : public Noncopyable {
  QueryBenchmarkResult() { iteration_durations.reserve(1'000'000); }

  QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
    num_iterations.store(other.num_iterations);
    duration = std::move(other.duration);
    iteration_durations = std::move(other.iteration_durations);
  }

  QueryBenchmarkResult& operator=(QueryBenchmarkResult&&) = default;

  std::atomic<size_t> num_iterations = 0;
  Duration duration = Duration{};
  tbb::concurrent_vector<Duration> iteration_durations;
};

}  // namespace opossum
