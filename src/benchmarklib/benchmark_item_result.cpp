#include "benchmark_item_result.hpp"

namespace opossum {

BenchmarkItemResult::BenchmarkItemResult() { metrics.reserve(1'000'000); }

BenchmarkItemResult::BenchmarkItemResult(BenchmarkItemResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration_of_all_runs = other.duration_of_all_runs;
  metrics = other.metrics;
  durations = other.durations;
  verification_passed = other.verification_passed;
}

BenchmarkItemResult& BenchmarkItemResult::operator=(BenchmarkItemResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration_of_all_runs = other.duration_of_all_runs;
  metrics = other.metrics;
  durations = other.durations;
  verification_passed = other.verification_passed;
  return *this;
}

}  // namespace opossum
