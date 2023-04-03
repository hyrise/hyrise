#include "operator_task.hpp"

#include <unordered_set>
#include <utility>
#include <queue>

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "operators/get_table.hpp"
#include "scheduler/job_task.hpp"


namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * Create tasks recursively. Called by `make_tasks_from_operator`.
 * @returns the root of the subtree that was added.
 * @param task_by_op is used as a cache to avoid creating duplicate tasks. For example, in the case of diamond shapes.
 */
std::shared_ptr<OperatorTask> add_operator_tasks_recursively(const std::shared_ptr<AbstractOperator>& op,
                                                             std::unordered_set<std::shared_ptr<OperatorTask>>& tasks) {
  auto task = op->get_or_create_operator_task();
  Assert(task, "Operator did not return a task.");
  // By using an unordered set, we can avoid adding duplicate tasks.
  const auto& [_, inserted] = tasks.insert(task);
  if (!inserted) {
    return task;
  }

  if (auto left = op->mutable_left_input()) {
    const auto& left_subtree_root = add_operator_tasks_recursively(left, tasks);
    left_subtree_root->set_as_predecessor_of(task);
  }

  if (auto right = op->mutable_right_input()) {
    const auto& right_subtree_root = add_operator_tasks_recursively(right, tasks);
    right_subtree_root->set_as_predecessor_of(task);
  }

  for (const auto& subquery : op->uncorrelated_subqueries()) {
    // The `subquery_root` is the operator referenced by any PQPSubqueryExpression required by the operator (e.g., a
    // subquery providing the value for a TableScan operator). By recursing into these uncorrelated subqueries as well,
    // we ensure that uncorrelated subqueries are correctly scheduled in the same way as regular input operators.
    const auto& subquery_root = add_operator_tasks_recursively(subquery, tasks);
    subquery_root->set_as_predecessor_of(task);
  }

  return task;
}

enum class TaskVisitation { VisitSuccessors, DoNotVisitSuccessors };

template <typename Visitor>
void visit_tasks_upwards(const std::shared_ptr<OperatorTask>& task, Visitor visitor) {

  std::queue<std::shared_ptr<OperatorTask>> node_queue;
  node_queue.push(task);

  std::unordered_set<std::shared_ptr<OperatorTask>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (visitor(node) == TaskVisitation::VisitSuccessors) {
      for (const auto& successor : node->successors()) {
        node_queue.push(std::static_pointer_cast<OperatorTask>(successor));
      }
    }
  }
}

void link_tasks_for_subquery_pruning(const std::unordered_set<std::shared_ptr<OperatorTask>>& tasks) {
  for (const auto& task : tasks) {
    const auto& op = task->get_operator();
    if (op->type() != OperatorType::GetTable) {
      continue;
    }
    const auto& get_table = static_cast<GetTable&>(*op);

    for (const auto& table_scan_ref : get_table.prunable_subquery_predicates) {
      const auto& table_scan = table_scan_ref.lock();
      Assert(table_scan, "Expected correct table scan.");

      for (const auto& subquery : table_scan->uncorrelated_subqueries()) {
        const auto& t = subquery->get_or_create_operator_task();
      Assert(tasks.contains(t), "Unknown OperatorTask.");
      auto is_acyclic = true;
      visit_tasks_upwards(task, [&](const auto& successor){
        if (successor == t) {
          is_acyclic = false;
          return TaskVisitation::DoNotVisitSuccessors;
        }
        return TaskVisitation::VisitSuccessors;
      });

      if (is_acyclic) {
        t->set_as_predecessor_of(task);
        std::cout << "linked " << t->get_operator()->description() << " and " << task->get_operator()->description() << std::endl;
      }

      }

    }

    }
}

}  // namespace

namespace hyrise {
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op, SchedulePriority priority, bool stealable)
    : AbstractTask(priority, stealable), _op(std::move(op)) {}

std::string OperatorTask::description() const {
  return "OperatorTask with id: " + std::to_string(id()) + " for op: " + _op->description();
}

std::pair<std::vector<std::shared_ptr<AbstractTask>>, std::shared_ptr<OperatorTask>>
OperatorTask::make_tasks_from_operator(const std::shared_ptr<AbstractOperator>& op) {
  auto operator_tasks_set = std::unordered_set<std::shared_ptr<OperatorTask>>{};
  const auto& root_operator_task = add_operator_tasks_recursively(op, operator_tasks_set);

  std::cout << *root_operator_task->get_operator()->lqp_node << std::endl << std::endl << *root_operator_task->get_operator() << std::endl;

  link_tasks_for_subquery_pruning(operator_tasks_set);

  return std::make_pair(
      std::vector<std::shared_ptr<AbstractTask>>(operator_tasks_set.begin(), operator_tasks_set.end()),
      root_operator_task);
}

const std::shared_ptr<AbstractOperator>& OperatorTask::get_operator() const {
  return _op;
}

void OperatorTask::skip_operator_task() {
  // Newly created tasks always have TaskState::Created. However, AbstractOperator::get_or_create_operator_task needs
  // to create an OperatorTask in TaskState::Done, if the operator has already executed. This function provides a quick
  // path to TaskState::Done using dummy transactions.
  // While performing dummy transactions, a task should not be used elsewhere, e.g., in the scheduler, to prevent
  // race conditions, conflicts etc. Thus, the following Assert checks the use_count of the task's shared pointer.
  // Only the owning instance should hold a shared pointer to the task. But, since this function needs to create
  // another shared pointer to query the use_count, the Assert checks for `use_count() == 2` as follows:
  Assert(shared_from_this().use_count() == 2, "Expected this OperatorTask to have a single owner.");
  Assert(_op->executed(), "An OperatorTask can only be skipped if its operator has already been executed.");

  /**
   * Use dummy transitions to switch to TaskState::Done because the AbstractTask cannot switch to TaskState::Done
   * directly.
   */
  auto success_scheduled = _try_transition_to(TaskState::Scheduled);
  Assert(success_scheduled, "Expected successful transition to TaskState::Scheduled.");

  auto success_started = _try_transition_to(TaskState::Started);
  Assert(success_started, "Expected successful transition to TaskState::Started.");

  auto success_done = _try_transition_to(TaskState::Done);
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

}  // namespace hyrise
