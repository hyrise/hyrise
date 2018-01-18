#pragma once

#include <memory>
#include <string>

#include "tuning/abstract_index_evaluator.hpp"
#include "tuning/abstract_index_selector.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * The IndexTuner creates indices based on the current state of the system in
 * order to optimize query performance.
 *
 * It uses an AbstractIndexEvaluator to evaluate both existing and non existing indices
 * in terms of system performance impact and then determines an operation sequence
 * that is expected to improve system performance by using an AbstractIndexSelector.
 *
 * While executing the operation sequence it considers a time budget.
 */
class IndexTuner {
 public:
  explicit IndexTuner(std::shared_ptr<SystemStatistics> statistics);

  void execute();

 protected:
  // Creates an index on all chunks of the specified table/column
  void _create_index(const std::string& table_name, ColumnID column_id);
  // Deletes an index on chunks of the specified table/column
  void _delete_index(const std::string& table_name, ColumnID column_id);

 protected:
  std::shared_ptr<SystemStatistics> _statistics;
  std::unique_ptr<AbstractIndexEvaluator> _evaluator;
  std::unique_ptr<AbstractIndexSelector> _selector;
};

}  // namespace opossum
