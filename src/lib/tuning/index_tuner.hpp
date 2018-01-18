#pragma once

#include <memory>
#include <string>

#include "tuning/abstract_index_evaluator.hpp"
#include "tuning/abstract_index_selector.hpp"
#include "tuning/index_operation.hpp"
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
 *
 * Both the time and memory budget (the latter being enforced by AbstractIndexSelector)
 * are initially infinite (i.e. disabled).
 */
class IndexTuner {
 public:
  explicit IndexTuner(std::shared_ptr<SystemStatistics> statistics);

  /**
   * The time budget (in seconds) limits the time available for
   * index creation and deletion operations.
   *
   * The time consumed by the AbstractIndexEvaluator and -Selector is
   * not counted against this budget.
   *
   * A value of positive infinity disables time budget checking.
   */
  void set_time_budget(float budget);
  void disable_time_budget();
  float time_budget();

  /**
   * The memory budget (in MiB) limits the space that can be consumed
   * by all indices maintained by the IndexTuner.
   *
   * A value of positive infinity disables memory budget checking.
   */
  void set_memory_budget(float budget);
  void disable_memory_budget();
  float memory_budget();

  /**
   * Initiates the tuning process
   */
  void execute();

 protected:
  void _execute_operations(const std::vector<IndexOperation>& operations);

  void _create_index(const std::string& table_name, ColumnID column_id);
  void _delete_index(const std::string& table_name, ColumnID column_id);

 protected:
  std::shared_ptr<SystemStatistics> _statistics;
  std::unique_ptr<AbstractIndexEvaluator> _evaluator;
  std::unique_ptr<AbstractIndexSelector> _selector;

  float _time_budget;
  float _memory_budget;
};

}  // namespace opossum
