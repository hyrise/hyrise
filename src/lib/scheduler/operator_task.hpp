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
  // We don't like abbreviations, but "operator" is a keyword
  OperatorTask(std::shared_ptr<AbstractOperator> op, SchedulePriority priority = SchedulePriority::Default,
               bool stealable = true);

  /**
   * Create tasks recursively from result operator and set task dependencies automatically.
   */
  static std::vector<std::shared_ptr<AbstractTask>> make_tasks_from_operator(
      const std::shared_ptr<AbstractOperator>& op);

  const std::shared_ptr<AbstractOperator>& get_operator() const;

  std::string description() const override;

 protected:
  void _on_execute() override;

 private:
  std::shared_ptr<AbstractOperator> _op;
};
}  // namespace opossum
