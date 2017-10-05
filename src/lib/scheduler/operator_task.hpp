#pragma once

#include <memory>
#include <vector>

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

 protected:
  void _on_execute() override;

  /**
   * Create tasks recursively. Called by `make_tasks_from_operator`.
   */
  static void _add_tasks_from_operator(std::shared_ptr<AbstractOperator> op,
                                       std::vector<std::shared_ptr<OperatorTask>>& tasks);

 private:
  std::shared_ptr<AbstractOperator> _op;
};
}  // namespace opossum
