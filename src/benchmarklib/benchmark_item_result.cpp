#include "benchmark_item_result.hpp"

namespace opossum {

BenchmarkItemResult::BenchmarkItemResult() { metrics.reserve(1'000'000); }

BenchmarkItemResult::BenchmarkItemResult(BenchmarkItemResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration_ns.store(other.duration_ns);
  metrics = other.metrics;
  verification_passed = other.verification_passed;
}

BenchmarkItemResult& BenchmarkItemResult::operator=(BenchmarkItemResult&& other) {
  num_iterations.store(other.num_iterations);
  duration_ns.store(other.duration_ns);
  metrics = other.metrics;
  verification_passed = other.verification_passed;
  return *this;
}

}  // namespace opossum
