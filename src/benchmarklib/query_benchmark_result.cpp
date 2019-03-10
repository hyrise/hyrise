#include "query_benchmark_result.hpp"

namespace opossum {

QueryBenchmarkResult::QueryBenchmarkResult() { metrics.reserve(1'000'000); }

QueryBenchmarkResult::QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration_ns.store(other.duration_ns);
  metrics = other.metrics;
  verification_passed = other.verification_passed;
}

}  // namespace opossum
