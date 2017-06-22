#include "operator_task.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "operators/rollback_records.hpp"

#include "scheduler/processing_unit.hpp"
#include "scheduler/worker.hpp"

namespace opossum {
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op) : _op(std::move(op)) {}

const std::vector<std::shared_ptr<OperatorTask>> OperatorTask::make_tasks_from_operator(
    std::shared_ptr<AbstractOperator> op) {
  std::vector<std::shared_ptr<OperatorTask>> tasks;

  auto task = std::make_shared<OperatorTask>(op);

  if (auto left = op->input_left()) {
    auto left_tasks = OperatorTask::make_tasks_from_operator(left);
    left_tasks.back()->set_as_predecessor_of(task);
    tasks.insert(tasks.end(), left_tasks.begin(), left_tasks.end());
  }

  if (auto right = op->input_right()) {
    auto right_tasks = OperatorTask::make_tasks_from_operator(right);
    right_tasks.back()->set_as_predecessor_of(task);
    tasks.insert(tasks.end(), right_tasks.begin(), right_tasks.end());
  }

  tasks.push_back(task);

  return tasks;
}

const std::shared_ptr<AbstractOperator> &OperatorTask::get_operator() const { return _op; }

void OperatorTask::on_execute() {
  auto context = _op->transaction_context();

  // Do not execute Operators in transaction marked as failed. Not doing so is crucial in order to make sure no other
  // tasks of the Transaction run while the Rollback happens.
  if (context && context->phase() == TransactionPhase::Failed) return;

  _op->execute();

  /**
   * Check whether the operator is a ReadWrite operator, and if it is, whether it failed.
   * If it failed, wait for the remaining active tasks to finish and schedule a Rollback
   */
  auto rw_operator = std::dynamic_pointer_cast<AbstractReadWriteOperator>(_op);
  if (rw_operator && rw_operator->execute_failed() && context) {
    TransactionManager::get().fail(*context);

    // It's possible that there is no worker on the current thread, e.g. when the task is run in a task
    auto worker = Worker::get_this_thread_worker();
    if (worker) {
      auto processing_unit = worker->processing_unit().lock();
      if (processing_unit) {  // just be safe, though the processing_unit().lock() shouldn't fail
        processing_unit->wake_or_create_worker();
      }
    }

    context->wait_for_active_operators_to_finish();

    auto rollback_records = std::make_shared<RollbackRecords>();
    rollback_records->set_transaction_context(context);

    std::make_shared<OperatorTask>(rollback_records)->schedule(CURRENT_NODE_ID, SchedulePriority::High);
  }
}
}  // namespace opossum
