#include "operator_task.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"

#include "scheduler/job_task.hpp"
#include "scheduler/worker.hpp"
#include "utils/tracing/probes.hpp"
#include "utils/timer.hpp"

namespace opossum {
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op, SchedulePriority priority, bool stealable)
    : AbstractTask(priority, stealable), _op(std::move(op)) {}

std::string OperatorTask::description() const {
  return "OperatorTask with id: " + std::to_string(id()) + " for op: " + _op->description();
}

std::vector<std::shared_ptr<AbstractTask>> OperatorTask::make_tasks_from_operator(
    const std::shared_ptr<AbstractOperator>& op) {
  std::vector<std::shared_ptr<AbstractTask>> tasks;
  std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<AbstractTask>> task_by_op;
  _add_tasks_from_operator(op, tasks, task_by_op);
  return tasks;
}

std::shared_ptr<AbstractTask> OperatorTask::_add_tasks_from_operator(
    const std::shared_ptr<AbstractOperator>& op, std::vector<std::shared_ptr<AbstractTask>>& tasks,
    std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<AbstractTask>>& task_by_op) {
  const auto task_by_op_it = task_by_op.find(op);
  if (task_by_op_it != task_by_op.end()) return task_by_op_it->second;

  auto task = std::make_shared<OperatorTask>(op);
  task_by_op.emplace(op, task);

  if (auto left = op->mutable_left_input()) {
    auto subtree_root = _add_tasks_from_operator(left, tasks, task_by_op);
    subtree_root->set_as_predecessor_of(task);
  }

  if (auto right = op->mutable_right_input()) {
    auto subtree_root = _add_tasks_from_operator(right, tasks, task_by_op);
    subtree_root->set_as_predecessor_of(task);
  }

  // Add AFTER the inputs to establish a task order where predecessor get executed before successors
  tasks.push_back(task);

  return task;
}

const std::shared_ptr<AbstractOperator>& OperatorTask::get_operator() const { return _op; }

void OperatorTask::_on_execute() {
  Timer performance_timer;
  auto context = _op->transaction_context();
  if (context) {
    switch (context->phase()) {
      case TransactionPhase::Active:
        // the expected default case
        break;

      case TransactionPhase::Conflicted:
      case TransactionPhase::RolledBackAfterConflict:
        // The transaction already failed. No need to execute this.
        if (auto read_write_operator = std::dynamic_pointer_cast<AbstractReadWriteOperator>(_op)) {
          // Essentially a noop, because no modifications are recorded yet. Better be on the safe side though.
          read_write_operator->rollback_records();
        }
        return;
      case TransactionPhase::Committing:
      case TransactionPhase::Committed:
        Fail("Trying to execute an operator for a transaction that is already committed");

      case TransactionPhase::RolledBackByUser:
        Fail("Trying to execute an operator for a transaction that has been rolled back by the user");
    }
  }

  DTRACE_PROBE2(HYRISE, OPERATOR_TASKS, reinterpret_cast<uintptr_t>(_op.get()), reinterpret_cast<uintptr_t>(this));
  _op->execute();

  /**
   * Check whether the operator is a ReadWrite operator, and if it is, whether it failed.
   * If it failed, trigger rollback of transaction.
   */
  auto rw_operator = std::dynamic_pointer_cast<AbstractReadWriteOperator>(_op);
  if (rw_operator && rw_operator->execute_failed()) {
    Assert(context, "Read/Write operator cannot have been executed without a context.");

    context->rollback(RollbackReason::Conflict);
  }

  // Get rid of temporary tables that are not needed anymore
  // Because `clear_output` is only called by the successive OperatorTasks, we can be sure that no one cleans up the
  // root (i.e., the final result)
  for (const auto& weak_predecessor : predecessors()) {
    const auto predecessor = std::dynamic_pointer_cast<OperatorTask>(weak_predecessor.lock());
    DebugAssert(predecessor, "Predecessor of OperatorTask is not an OperatorTask itself");

    // If someone else still holds a shared_ptr to the table (e.g., a ReferenceSegment pointing to a materialized
    // temporary table), it will not yet get deleted
    if (predecessor->get_operator()->consumer_count() == 0) predecessor->get_operator()->clear_output();
  }

  if constexpr (HYRISE_DEBUG) {
    const auto walltime = performance_timer.lap();
    std::cout << _op->name() << _op
              << "\n time: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(walltime))
              << std::endl;
  }
}
}  // namespace opossum
