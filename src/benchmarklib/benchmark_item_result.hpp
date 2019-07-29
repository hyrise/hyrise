#pragma once

#include <atomic>

#include "benchmark_config.hpp"
#include "benchmark_item_run_result.hpp"

namespace opossum {

// Stores the result of ALL runs of a single benchmark item (e.g., TPC-H query 5).
struct BenchmarkItemResult {
  BenchmarkItemResult();

  // Stores the detailed information about the runs executed.
  tbb::concurrent_vector<BenchmarkItemRunResult> runs;

  // Stores the execution duration of the item if run in BenchmarkMode::Ordered. `runs/duration` is iterations/s, even
  // if multiple clients executed the item in parallel. Undefined for BenchmarkMode::Shuffled.
  Duration duration;

  // The *optional* is set if the verification was executed; the *bool* is true if the verification succeeded.
  std::optional<bool> verification_passed;
};

}  // namespace opossum
