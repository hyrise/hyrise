#include "benchmark_item_result.hpp"

namespace opossum {

BenchmarkItemResult::BenchmarkItemResult() {
  successful_runs.reserve(1'000'000);
  unsuccessful_runs.reserve(1'000);
}

}  // namespace opossum
