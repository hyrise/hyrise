#pragma once

#include <memory>
#include <vector>
#include <unordered_map>

#include "scheduler/abstract_task.hpp"

namespace opossum {

class AbstractOperator;

/**
 * Makes an AbstractOperator scheduleable
 */
class OperatorTask : public AbstractTask {
 public:
  explicit OperatorTask(std::shared_ptr<AbstractOperator> op);

  /**
   * Create tasks recursively from result operator and set task dependencies automatically.
   */
  static const std::vector<std::shared_ptr<OperatorTask>> make_tasks_from_operator(
      std::shared_ptr<AbstractOperator> op);

  const std::shared_ptr<AbstractOperator>& get_operator() const;

  std::string description() const override;

 protected:
  void _on_execute() override;

  /**
   * Create tasks recursively. Called by `make_tasks_from_operator`. Returns the root of the subtree that was added.
   * @param task_by_op  Cache to avoid creating duplicate Tasks for diamond shapes
   */
  static std::shared_ptr<OperatorTask> _add_tasks_from_operator(
      std::shared_ptr<AbstractOperator> op, std::vector<std::shared_ptr<OperatorTask>>& tasks,
      std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>>& task_by_op);

 private:
  std::shared_ptr<AbstractOperator> _op;
};
}  // namespace opossum
