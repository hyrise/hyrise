#include "benchmark_item_run_result.hpp"

namespace opossum {

BenchmarkItemRunResult::BenchmarkItemRunResult(Duration begin, Duration duration,
                                               std::vector<SQLPipelineMetrics> metrics)
    : begin(begin), duration(duration), metrics(std::move(metrics)) {}

}  // namespace opossum
