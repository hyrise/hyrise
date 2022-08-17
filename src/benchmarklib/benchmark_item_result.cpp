#include "benchmark_item_result.hpp"

namespace hyrise {

BenchmarkItemResult::BenchmarkItemResult() {
  successful_runs.reserve(1'000'000);
  unsuccessful_runs.reserve(1'000);
}

}  // namespace hyrise
