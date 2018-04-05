#pragma once

#include <memory>
#include <vector>
#include <unordered_map>

#include "scheduler/abstract_task.hpp"
#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class AbstractOperator;

/**
 * Makes an AbstractOperator scheduleable
 */
class OperatorTask : public AbstractTask {
 public:
  explicit OperatorTask(AbstractOperatorSPtr op);

  /**
   * Create tasks recursively from result operator and set task dependencies automatically.
   */
  static const std::vector<OperatorTaskSPtr> make_tasks_from_operator(
      AbstractOperatorSPtr op);

  const AbstractOperatorSPtr& get_operator() const;

  std::string description() const override;

 protected:
  void _on_execute() override;

  /**
   * Create tasks recursively. Called by `make_tasks_from_operator`. Returns the root of the subtree that was added.
   * @param task_by_op  Cache to avoid creating duplicate Tasks for diamond shapes
   */
  static OperatorTaskSPtr _add_tasks_from_operator(
      AbstractOperatorSPtr op, std::vector<OperatorTaskSPtr>& tasks,
      std::unordered_map<AbstractOperatorSPtr, OperatorTaskSPtr>& task_by_op);

 private:
  AbstractOperatorSPtr _op;
};


}  // namespace opossum
