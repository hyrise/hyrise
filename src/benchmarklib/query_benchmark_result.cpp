#include "query_benchmark_result.hpp" // NEEDEDINCLUDE

namespace opossum {

QueryBenchmarkResult::QueryBenchmarkResult() { iteration_durations.reserve(1'000'000); }

QueryBenchmarkResult::QueryBenchmarkResult(QueryBenchmarkResult&& other) noexcept {
  num_iterations.store(other.num_iterations);
  duration = other.duration;
  iteration_durations = other.iteration_durations;
}

}  // namespace opossum
