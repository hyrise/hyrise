#include <memory>
#include <string>
#include <utility>
#include <vector>
#pragma once

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
   * Note: Creating tasks is not thread-safe and concurrently creating tasks from the same (sub-)PQP is discouraged. We
   *       used to create tasks for uncorrelated subqueries ad-hoc and likely concurrently in the past, but this caused
   *       either segfaults or deadlocks (see #2520). Thus, we only create (i) almost all tasks at once for each
   *       SQLPipelineStatement and (ii) tasks for correlated subqueries ad-hoc in the ExpressionEvaluator (which have
   *       to use a deep copy for each instance anyway, see
   *       ExpressionEvaluator::_evaluate_subquery_expression_for_row).
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
