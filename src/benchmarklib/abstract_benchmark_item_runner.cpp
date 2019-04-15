#include "abstract_benchmark_item_runner.hpp"

#include <boost/algorithm/string/replace.hpp>

#include "benchmark_sql_executor.hpp"
#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

std::pair<std::vector<SQLPipelineMetrics>, bool> AbstractBenchmarkItemRunner::execute_item(
    const BenchmarkItemID item_id) {
  std::optional<std::string> visualize_prefix;
  if (enable_visualization) {
    auto name = item_name(item_id);
    boost::replace_all(name, " ", "_");
    visualize_prefix = std::move(name);
  }

  BenchmarkSQLExecutor sql_executor(enable_jit, _sqlite_wrapper, visualize_prefix);
  _execute_item(item_id, sql_executor);
  return {std::move(sql_executor.metrics), sql_executor.any_verification_failed};
}

void AbstractBenchmarkItemRunner::set_sqlite_wrapper(std::shared_ptr<SQLiteWrapper> sqlite_wrapper) {
  _sqlite_wrapper = sqlite_wrapper;
}

const std::vector<BenchmarkItemID>& AbstractBenchmarkItemRunner::selected_items() const { return _selected_items; }

}  // namespace opossum
