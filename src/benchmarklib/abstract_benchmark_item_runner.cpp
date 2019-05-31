#include "abstract_benchmark_item_runner.hpp"

#include <boost/algorithm/string/replace.hpp>

#include "benchmark_sql_executor.hpp"
#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

AbstractBenchmarkItemRunner::AbstractBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config)
    : _config(config) {}

std::pair<std::vector<SQLPipelineMetrics>, bool> AbstractBenchmarkItemRunner::execute_item(
    const BenchmarkItemID item_id) {
  std::optional<std::string> visualize_prefix;
  if (_config->enable_visualization) {
    auto name = item_name(item_id);
    boost::replace_all(name, " ", "_");
    visualize_prefix = std::move(name);
  }

  BenchmarkSQLExecutor sql_executor(_config->enable_jit, _sqlite_wrapper, visualize_prefix);
  _on_execute_item(item_id, sql_executor);
  return {std::move(sql_executor.metrics), sql_executor.any_verification_failed};
}

void AbstractBenchmarkItemRunner::set_sqlite_wrapper(const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper) {
  _sqlite_wrapper = sqlite_wrapper;
}

}  // namespace opossum
