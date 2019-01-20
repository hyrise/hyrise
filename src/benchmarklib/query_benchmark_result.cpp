#include "query_benchmark_result.hpp"

namespace opossum {

QueryBenchmarkResult::QueryBenchmarkResult() { metrics.reserve(1'000'000); }

QueryBenchmarkResult::QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration = std::move(other.duration);
  metrics = std::move(other.metrics);
  verification_passed = std::move(other.verification_passed);
}

}  // namespace opossum
