#pragma once

#include <memory>
#include <string>

#include "tuning/index_evaluator.hpp"
#include "tuning/index_selector.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * The IndexTuner creates indices based on the current state of the system in
 * order to optimize query performance.
 * It analyzes the query plan cache and table statistics in order to
 * choose appropriate indices using pluggable IndexSelectionHeuristic instances.
 */
class IndexTuner {
 public:
  explicit IndexTuner(std::shared_ptr<SystemStatistics> statistics);

  void execute();

 protected:
  // Creates an index on all chunks of the specified table/column
  void _create_index(const std::string& table_name, ColumnID column_id);
  void _delete_index(const std::string& table_name, ColumnID column_id);

 protected:
  std::shared_ptr<SystemStatistics> _statistics;
  std::unique_ptr<IndexEvaluator> _evaluator;
  std::unique_ptr<IndexSelector> _selector;
};

}  // namespace opossum
