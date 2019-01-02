#include "query_benchmark_result.hpp"

namespace opossum {

QueryBenchmarkResult::QueryBenchmarkResult() { iteration_durations.reserve(1'000'000); }

QueryBenchmarkResult::QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration = std::move(other.duration);
  iteration_durations = std::move(other.iteration_durations);
}

}  // namespace opossum
