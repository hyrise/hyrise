#include "operator_task.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"

#include "scheduler/job_task.hpp"
#include "scheduler/processing_unit.hpp"
#include "scheduler/worker.hpp"

namespace opossum {
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op, CleanupTemporaries cleanup_temporaries, bool stealable)
    : AbstractTask(stealable), _op(std::move(op)), _cleanup_temporaries(cleanup_temporaries) {}

std::string OperatorTask::description() const {
  return "OperatorTask with id: " + std::to_string(id()) + " for op: " + _op->description();
}

const std::vector<std::shared_ptr<OperatorTask>> OperatorTask::make_tasks_from_operator(
    const std::shared_ptr<AbstractOperator>& op, CleanupTemporaries cleanup_temporaries) {
  std::vector<std::shared_ptr<OperatorTask>> tasks;
  std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>> task_by_op;
  OperatorTask::_add_tasks_from_operator(op, tasks, task_by_op, cleanup_temporaries);
  return tasks;
}

std::shared_ptr<OperatorTask> OperatorTask::_add_tasks_from_operator(
    std::shared_ptr<AbstractOperator> op, std::vector<std::shared_ptr<OperatorTask>>& tasks,
    std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>>& task_by_op,
    CleanupTemporaries cleanup_temporaries) {
  const auto task_by_op_it = task_by_op.find(op);
  if (task_by_op_it != task_by_op.end()) return task_by_op_it->second;

  const auto task = std::make_shared<OperatorTask>(op, cleanup_temporaries);
  task_by_op.emplace(op, task);

  if (auto left = op->mutable_input_left()) {
    auto subtree_root = OperatorTask::_add_tasks_from_operator(left, tasks, task_by_op, cleanup_temporaries);
    subtree_root->set_as_predecessor_of(task);
  }

  if (auto right = op->mutable_input_right()) {
    auto subtree_root = OperatorTask::_add_tasks_from_operator(right, tasks, task_by_op, cleanup_temporaries);
    subtree_root->set_as_predecessor_of(task);
  }

  // Add AFTER the inputs to establish a task order where predecessor get executed before successors
  tasks.push_back(task);

  return task;
}

const std::shared_ptr<AbstractOperator>& OperatorTask::get_operator() const { return _op; }

void OperatorTask::_on_execute() {
  auto context = _op->transaction_context();
  if (context) {
    switch (context->phase()) {
      case TransactionPhase::Active:
        // the expected default case
        break;

      case TransactionPhase::Aborted:
      case TransactionPhase::RolledBack:
        // The transaction already failed. No need to execute this.
        if (auto read_write_operator = std::dynamic_pointer_cast<AbstractReadWriteOperator>(_op)) {
          // Essentially a noop, because no modifications are recorded yet. Better be on the safe side though.
          read_write_operator->rollback_records();
        }
        return;
        break;

      case TransactionPhase::Committing:
      case TransactionPhase::Committed:
        Fail("Trying to execute operators for a transaction that is already committed");
    }
  }

  _op->execute();

  /**
   * Check whether the operator is a ReadWrite operator, and if it is, whether it failed.
   * If it failed, trigger rollback of transaction.
   */
  auto rw_operator = std::dynamic_pointer_cast<AbstractReadWriteOperator>(_op);
  if (rw_operator && rw_operator->execute_failed()) {
    Assert(context != nullptr, "Read/Write operator cannot have been executed without a context.");

    context->rollback();
  }

  // Get rid of temporary tables that are not needed anymore
  // Because `clear_output` is only called by the successive OperatorTasks, we can be sure that no one cleans up the
  // root (i.e., the final result)
  if (_cleanup_temporaries == CleanupTemporaries::Yes) {
    for (const auto& weak_predecessor : predecessors()) {
      const auto predecessor = std::dynamic_pointer_cast<OperatorTask>(weak_predecessor.lock());
      DebugAssert(predecessor != nullptr, "predecessor of OperatorTask is not an OperatorTask itself");
      auto previous_operator_still_needed = false;

      for (const auto& successor : predecessor->successors()) {
        if (successor.get() != this && !successor->is_done()) {
          previous_operator_still_needed = true;
        }
      }
      // If someone else still holds a shared_ptr to the table (e.g., a ReferenceColumn pointing to a materialized
      // temporary table), it will not yet get deleted
      if (!previous_operator_still_needed) predecessor->get_operator()->clear_output();
    }
  }
}
}  // namespace opossum
