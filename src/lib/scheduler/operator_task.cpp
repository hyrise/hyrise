#include "operator_task.hpp"

#include <memory>
#include <unordered_set>
#include <utility>

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"

#include "scheduler/job_task.hpp"
#include "utils/tracing/probes.hpp"

namespace {

using namespace opossum;  // NOLINT

/**
 * Create tasks recursively. Called by `make_tasks_from_operator`.
 * @returns the root of the subtree that was added.
 * @param task_by_op is used as a cache to avoid creating duplicate tasks. For example, in the case of diamond shapes.
 */
std::shared_ptr<OperatorTask> add_operator_tasks_recursively(const std::shared_ptr<AbstractOperator>& op,
                                                             std::unordered_set<std::shared_ptr<OperatorTask>>& tasks) {
  auto task = op->get_or_create_operator_task();
  // By using an unordered set, we can avoid adding duplicate tasks.
  const auto& [_, inserted] = tasks.insert(task);
  if (!inserted) return task;

  if (auto left = op->mutable_left_input()) {
    if (auto left_subtree_root = add_operator_tasks_recursively(left, tasks)) {
      left_subtree_root->set_as_predecessor_of(task);
    }
  }
  if (auto right = op->mutable_right_input()) {
    if (auto right_subtree_root = add_operator_tasks_recursively(right, tasks)) {
      right_subtree_root->set_as_predecessor_of(task);
    }
  }

  return task;
}

}  // namespace

namespace opossum {
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op, SchedulePriority priority, bool stealable)
    : AbstractTask(priority, stealable), _op(std::move(op)) {}

std::string OperatorTask::description() const {
  return "OperatorTask with id: " + std::to_string(id()) + " for op: " + _op->description();
}

std::pair<std::vector<std::shared_ptr<AbstractTask>>, std::shared_ptr<OperatorTask>>
OperatorTask::make_tasks_from_operator(const std::shared_ptr<AbstractOperator>& op) {
  std::unordered_set<std::shared_ptr<OperatorTask>> operator_tasks_set;
  const auto& root_operator_task = add_operator_tasks_recursively(op, operator_tasks_set);

  return std::make_pair(
      std::vector<std::shared_ptr<AbstractTask>>(operator_tasks_set.begin(), operator_tasks_set.end()),
      root_operator_task);
}

const std::shared_ptr<AbstractOperator>& OperatorTask::get_operator() const { return _op; }

void OperatorTask::skip_operator_task() {
  Assert(shared_from_this().use_count() == 2, "Expected this OperatorTask to have a single owner.");
  Assert(_op->executed(), "An OperatorTask can only be skipped if its operator has already been executed.");
  /**
   * Use dummy transitions to switch to TaskState::Done because the AbstractTask cannot switch to TaskState::Done
   * directly.
   */
  auto success_scheduled = this->_try_transition_to(TaskState::Scheduled);
  Assert(success_scheduled, "Expected successful transition to TaskState::Scheduled.");

  auto success_started = this->_try_transition_to(TaskState::Started);
  Assert(success_started, "Expected successful transition to TaskState::Started.");

  auto success_done = this->_try_transition_to(TaskState::Done);
  Assert(success_done, "Expected successful transition to TaskState::Done.");
}

void OperatorTask::_on_execute() {
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
}

}  // namespace opossum
