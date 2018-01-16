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
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op) : _op(std::move(op)) {}

std::string OperatorTask::description() const {
  return "OperatorTask with id: " + std::to_string(id()) + " for op: " + _op->description();
}

const std::vector<std::shared_ptr<OperatorTask>> OperatorTask::make_tasks_from_operator(
    std::shared_ptr<AbstractOperator> op) {
  std::vector<std::shared_ptr<OperatorTask>> tasks;
  OperatorTask::_add_tasks_from_operator(op, tasks);
  return tasks;
}

std::shared_ptr<OperatorTask> OperatorTask::_add_tasks_from_operator(
    std::shared_ptr<AbstractOperator> op, std::vector<std::shared_ptr<OperatorTask>>& tasks) {
  std::shared_ptr<OperatorTask> task = op->operator_task();
  bool add_task = false;
  if (!task) {
    // We need to make sure that we don't create two tasks for the same operator. This could happen if the same
    // operator is used as input for two other operators.
    task = std::make_shared<OperatorTask>(op);
    op->set_operator_task(task);
    add_task = true;
  }

  if (auto left = op->mutable_input_left()) {
    auto subtree_root = OperatorTask::_add_tasks_from_operator(left, tasks);
    subtree_root->set_as_predecessor_of(task);
  }

  if (auto right = op->mutable_input_right()) {
    auto subtree_root = OperatorTask::_add_tasks_from_operator(right, tasks);
    subtree_root->set_as_predecessor_of(task);
  }

  if (add_task) tasks.push_back(task);

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
}
}  // namespace opossum
