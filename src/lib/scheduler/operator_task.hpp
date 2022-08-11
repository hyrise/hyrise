#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "scheduler/abstract_task.hpp"

namespace hyrise {

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
   * Creates tasks recursively from the given operator @param op and sets task dependencies automatically.
   * @returns a pair, consisting of a vector of unordered tasks and a pointer to the root operator task that would
   *          otherwise be hidden inside the vector.
   */
  static std::pair<std::vector<std::shared_ptr<AbstractTask>>, std::shared_ptr<OperatorTask>> make_tasks_from_operator(
      const std::shared_ptr<AbstractOperator>& op);

  const std::shared_ptr<AbstractOperator>& get_operator() const;

  std::string description() const override;

  /**
   * Transitions this OperatorTask to TaskState::Done.
   * @pre Operator must have already been executed.
   */
  void skip_operator_task();

 protected:
  void _on_execute() override;

 private:
  std::shared_ptr<AbstractOperator> _op;
};
}  // namespace hyrise
