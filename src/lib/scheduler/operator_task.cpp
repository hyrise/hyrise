#include "operator_task.hpp"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "operators/get_table.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/task_utils.hpp"

namespace std {

template <>
struct hash<std::tuple<std::string, std::shared_ptr<hyrise::Table>, hyrise::ColumnID>> {
  size_t operator()(const std::tuple<std::string, std::shared_ptr<hyrise::Table>, hyrise::ColumnID>& table_column_identifier) const {
  auto hash = size_t{0};
  boost::hash_combine(hash, std::get<0>(table_column_identifier));
  boost::hash_combine(hash, std::get<1>(table_column_identifier));
  boost::hash_combine(hash, std::get<2>(table_column_identifier));
  return hash;
  }
};

}  // namespace std

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

  auto columns_to_load = std::unordered_set<std::tuple<std::string, std::shared_ptr<Table>, ColumnID>>{};
  columns_to_load.reserve(16);

  const auto& storage_manager = Hyrise::get().storage_manager;
  visit_lqp(op->lqp_node, [&](const auto& node) {
    for (const auto& node_expression : node->node_expressions) {
      visit_expression(node_expression, [&](const auto& expression) {
        if (expression->type != ExpressionType::LQPColumn) {
          return ExpressionVisitation::VisitArguments;
        }

        const auto& column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
        const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());
        if (!stored_table_node) {
          return ExpressionVisitation::VisitArguments;
        }

        columns_to_load.emplace(stored_table_node->table_name,
                                storage_manager.get_table(stored_table_node->table_name),
                                column_expression->original_column_id);

        return ExpressionVisitation::VisitArguments;
      });
    }
    return LQPVisitation::VisitInputs;
  });

  auto sorted_columns_to_load = std::vector(columns_to_load.begin(), columns_to_load.end());
  std::sort(sorted_columns_to_load.begin(), sorted_columns_to_load.end(), [](const auto& lhs, const auto& rhs) {
    return std::get<1>(lhs)->row_count() > std::get<1>(rhs)->row_count();
  });

  // auto sstream = std::stringstream{};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(columns_to_load.size());
  for (const auto& [table_name, table, column_id] : sorted_columns_to_load) {
    if (column_id == INVALID_COLUMN_ID) {
      continue;
    }

    const auto chunk = table->get_chunk(ChunkID{0});
    // if (column_id > chunk->column_count()) {
    //   // std::cerr << std::format("Skipping the loading of columnID {}.\n", static_cast<size_t>(column_id));
    //   continue;
    // }

    // std::cout << "We should load " << &*table << " and " << column_id << std::endl;
    const auto segment = chunk->get_segment(column_id);
    if (!std::dynamic_pointer_cast<PlaceHolderSegment>(segment)) {
      // Only access histogram, when first segment is a PlaceHolderSegment (remember, we replace all segments of a columns).
      continue;
    }

    // sstream << table_name << "::" << column_id << " \t--\t ";

    jobs.emplace_back(std::make_shared<JobTask>([&, table_name=table_name, column_id=column_id]() {
      data_loading_utils::load_column_when_necessary(table_name, column_id, false);
      // resolve_data_type(table->column_data_type(column_id), [&, table=table, column_id=column_id](const auto& data_type) {
      //   using ColumnDataType = typename std::decay_t<decltype(data_type)>::type;

      //   // Just use data utils load column?!?!?
      //   const auto column_statistics = table->table_statistics()->column_statistics;
      //   const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(column_statistics[column_id]);
      //   attribute_statistics->histogram();
      // });
    }, SchedulePriority::Background));
    jobs.back()->set_as_predecessor_of(task);
  }

  // sstream << "\nNow operator task for >> " << *op << " << has " << task->predecessors().size() << " predecessors.\n\n";
  // std::cerr << sstream.str();

  Hyrise::get().scheduler()->schedule_tasks(jobs);

  for (const auto& subquery : op->uncorrelated_subqueries()) {
    // The `subquery_root` is the operator referenced by any PQPSubqueryExpression required by the operator (e.g., a
    // subquery providing the value for a TableScan operator). By recursing into these uncorrelated subqueries as well,
    // we ensure that uncorrelated subqueries are correctly scheduled in the same way as regular input operators.
    const auto& subquery_root = add_operator_tasks_recursively(subquery, tasks);
    subquery_root->set_as_predecessor_of(task);
  }

  return task;
}

/**
 * Sets tasks that can be used to prune chunks by predicates with uncorrelated subqueries as successors of the GetTable
 * tasks. Guarantees that the resulting task graph is still acyclic.
 */
void link_tasks_for_subquery_pruning(const std::unordered_set<std::shared_ptr<OperatorTask>>& tasks) {
  for (const auto& task : tasks) {
    const auto& op = task->get_operator();
    if (op->type() != OperatorType::GetTable) {
      continue;
    }

    const auto& get_table = static_cast<GetTable&>(*op);
    for (const auto& table_scan : get_table.prunable_subquery_predicates()) {
      for (const auto& subquery : table_scan->uncorrelated_subqueries()) {
        // All tasks have already been created, so we must be able to get the cached task from each operator.
        const auto& subquery_root = subquery->get_or_create_operator_task();
        Assert(tasks.contains(subquery_root), "Unknown OperatorTask.");

        // Cycles in the task graph would lead to deadlocks during execution. This could happen if a table can be pruned
        // using a predicate on itself (e.g., `SELECT * FROM a_table WHERE x > (SELECT AVG(x) FROM a_table)`) and the
        // LQPTranslator created a single GetTable operator due to operator deduplication. To make sure we do not
        // introduce cycles, we include the prunable_subquery_predicates of a StoredTableNode in its equality check.
        // Thus, we have two unequal nodes that are translated to distinct operators by the LQPTranslator (and no
        // further sanity check should be necessary). However, we still check for cycles after linking all tasks in
        // debug builds.
        subquery_root->set_as_predecessor_of(task);
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

  // GetTable operators can store references to TableScans as prunable subquery predicates (see get_table.hpp for
  // details).
  // We set the tasks associated with the uncorrelated subqueries as predecessors of the GetTable tasks so the GetTable
  // operators can extract the predicate values and perform dynamic chunk pruning. However, we cannot link the tasks
  // during the recursive creation of operator tasks (see map_prunable_subquery_predicates.hpp). Potentially, not all
  // tasks are yet created when reaching the GetTable tasks. Furthermore, we need the entire task graph to ensure that
  // it is acyclic.
  link_tasks_for_subquery_pruning(operator_tasks_set);

  // Ensure the task graph is acyclic, i.e., no task is any (n-th) successor of itself. Tasks in cycles would end up in
  // a deadlock during execution, mutually waiting for the other tasks' execution. Even if the tasks are never executed,
  // cycles create memory leaks since tasks hold shared pointers to their predecessors.
  if constexpr (HYRISE_DEBUG) {
    visit_tasks(root_operator_task, [](const auto& task) {
      for (const auto& direct_successor : task->successors()) {
        visit_tasks_upwards(direct_successor, [&](const auto& successor) {
          Assert(task != successor, "Task graph contains a cycle.");
          return TaskUpwardVisitation::VisitSuccessors;
        });
      }

      return TaskVisitation::VisitPredecessors;
    });
  }

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
  // std::cerr << std::format("Operator task ({}) running with taskID {} and #predecessors: {}.\n", description(), static_cast<size_t>(id()), predecessors().size());
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
        Fail("Trying to execute an operator for a transaction that is already committed.");

      case TransactionPhase::RolledBackByUser:
        Fail("Trying to execute an operator for a transaction that has been rolled back by the user.");
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
