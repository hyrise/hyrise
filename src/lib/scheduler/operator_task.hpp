#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "scheduler/abstract_task.hpp"

namespace opossum {

class AbstractOperator;

/**
 * Makes an AbstractOperator scheduleable
 */
class OperatorTask : public AbstractTask {
 public:
  // We don't like abbreviations, but "operator" is a keyword
  OperatorTask(std::shared_ptr<AbstractOperator> op, CleanupTemporaries cleanup_temporaries,
               SchedulePriority priority = SchedulePriority::Default, bool stealable = true);

  /**
   * Create tasks recursively from result operator and set task dependencies automatically.
   */
  static std::vector<std::shared_ptr<OperatorTask>> make_tasks_from_operator(
      const std::shared_ptr<AbstractOperator>& op, CleanupTemporaries cleanup_temporaries);

  const std::shared_ptr<AbstractOperator>& get_operator() const;

  std::string description() const override;

 protected:
  void _on_execute() override;

  /**
   * Create tasks recursively. Called by `make_tasks_from_operator`. Returns the root of the subtree that was added.
   * @param task_by_op  Cache to avoid creating duplicate Tasks for diamond shapes
   */
  static std::shared_ptr<OperatorTask> _add_tasks_from_operator(
      const std::shared_ptr<AbstractOperator>& op, std::vector<std::shared_ptr<OperatorTask>>& tasks,
      std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>>& task_by_op,
      CleanupTemporaries cleanup_temporaries);

 private:
  std::shared_ptr<AbstractOperator> _op;
  CleanupTemporaries _cleanup_temporaries;
};
}  // namespace opossum
