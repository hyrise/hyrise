#include "benchmark_item_run_result.hpp"

#include <utility>
#include <vector>

#include "benchmark_config.hpp"
#include "sql/sql_pipeline.hpp"

namespace hyrise {

BenchmarkItemRunResult::BenchmarkItemRunResult(Duration init_begin, Duration init_duration,
                                               std::vector<SQLPipelineMetrics> init_metrics)
    : begin(init_begin), duration(init_duration), metrics(std::move(init_metrics)) {}

}  // namespace hyrise
