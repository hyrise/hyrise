#pragma once

#include <memory>

#include "operators/abstract_operator.hpp"

#include "scheduler/abstract_task.hpp"

namespace opossum {

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
  void on_execute() override;

 private:
  std::shared_ptr<AbstractOperator> _op;
};
}  // namespace opossum
