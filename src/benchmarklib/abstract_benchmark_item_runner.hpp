#pragma once

#include <functional>
#include <string>
#include <vector>

#include "benchmark_item_result.hpp"
#include "benchmark_sql_executor.hpp"
#include "strong_typedef.hpp"

STRONG_TYPEDEF(size_t, BenchmarkItemID);

namespace opossum {

// Item runners execute the SQL queries associated with a given benchmark. In their simplest form, an item is a single
// query, for example a TPC-H query. More complex items are those of the TPC-C benchmark, which combine multiple
// queries and logic in an item such as "NewOrder". Parameters can be randomized for some benchmarks (e.g., TPC-H).

class AbstractBenchmarkItemRunner {
 public:
  AbstractBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config);

  virtual ~AbstractBenchmarkItemRunner() = default;

  // Executes a benchmark item and returns information about the SQL statements executed during its execution as well
  // as a bool indicating whether the verification FAILED.
  std::pair<std::vector<SQLPipelineMetrics>, bool> execute_item(const BenchmarkItemID item_id);

  // Returns the names of the individual items (e.g., "TPC-H 1")
  virtual std::string item_name(const BenchmarkItemID item_id) const = 0;

  // Returns the number of items supported by the benchmark
  virtual size_t available_item_count() const = 0;

  // Returns the BenchmarkItemIDs of all selected items
  const std::vector<BenchmarkItemID>& selected_items() const;

  // Set the SQLite wrapper used for query verification. `nullptr` disables verification.
  void set_sqlite_wrapper(std::shared_ptr<SQLiteWrapper> sqlite_wrapper);

 protected:
  // Executes the benchmark item with the given ID. BenchmarkItemRunners should not use the SQL pipeline directly,
  // but use the provided BenchmarkSQLExecutor. That class not only tracks the execution metrics and provides them
  // back to the benchmark runner, but it also implements SQLite verification and plan visualization.
  virtual void _execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) = 0;

  std::shared_ptr<BenchmarkConfig> _config;

  std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;

  std::vector<BenchmarkItemID> _selected_items;
};

}  // namespace opossum
