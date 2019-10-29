#include "abstract_benchmark_item_runner.hpp"

#include <boost/algorithm/string.hpp>

#include "benchmark_sql_executor.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/list_directory.hpp"
#include "utils/load_table.hpp"

namespace opossum {

AbstractBenchmarkItemRunner::AbstractBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config)
    : _config(config) {}

void AbstractBenchmarkItemRunner::load_dedicated_expected_results(
    const std::filesystem::path& expected_results_directory_path) {
  Assert(std::filesystem::is_directory(expected_results_directory_path),
         "Expected results path (" + expected_results_directory_path.string() + ") has to be a directory.");

  const auto is_tbl_file = [](const std::string& filename) { return boost::algorithm::ends_with(filename, ".tbl"); };

  _dedicated_expected_results.resize(items().size());

  std::cout << "- Loading expected result tables"
            << "\n";

  for (const auto& entry : list_directory(expected_results_directory_path)) {
    if (is_tbl_file(entry)) {
      const auto item_name = entry.stem().string();

      const auto iter = std::find_if(items().cbegin(), items().cend(), [this, &item_name](const auto& item) {
        return this->item_name(item) == item_name;
      });
      if (iter != items().cend()) {
        std::cout << "-  Loading result table " + entry.string() << "\n";
        _dedicated_expected_results[*iter] = load_table(entry.string());
      }
    }
  }
}

bool AbstractBenchmarkItemRunner::has_item_without_dedicated_result() {
  // `_dedicated_expected_results` is either empty if `load_dedicated_expected_results` was not called
  // or a sparse vector with the same size as `items()`.
  if (!items().empty() && _dedicated_expected_results.empty()) {
    return true;
  }
  for (const auto& dedicated_result : _dedicated_expected_results) {
    if (!dedicated_result) {
      return true;
    }
  }
  return false;
}

void AbstractBenchmarkItemRunner::on_tables_loaded() {}

std::tuple<bool, std::vector<SQLPipelineMetrics>, bool> AbstractBenchmarkItemRunner::execute_item(
    const BenchmarkItemID item_id) {
  std::optional<std::string> visualize_prefix;
  if (_config->enable_visualization) {
    auto name = item_name(item_id);
    boost::replace_all(name, " ", "_");
    visualize_prefix = std::move(name);
  }

  BenchmarkSQLExecutor sql_executor(_sqlite_wrapper, visualize_prefix);
  auto success = _on_execute_item(item_id, sql_executor);
  return {success, std::move(sql_executor.metrics), sql_executor.any_verification_failed};
}

void AbstractBenchmarkItemRunner::set_sqlite_wrapper(const std::shared_ptr<SQLiteWrapper>& sqlite_wrapper) {
  _sqlite_wrapper = sqlite_wrapper;
}

const std::vector<int>& AbstractBenchmarkItemRunner::weights() const {
  static const std::vector<int> empty_vector;
  return empty_vector;
}

}  // namespace opossum
