#include "sql_query_plan.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "operators/table_scan.hpp"

namespace opossum {

SQLQueryPlan::SQLQueryPlan(CleanupTemporaries cleanup_temporaries) : _cleanup_temporaries(cleanup_temporaries) {}

void SQLQueryPlan::add_tree_by_root(std::shared_ptr<AbstractOperator> op) { _roots.push_back(op); }

void SQLQueryPlan::append_plan(const SQLQueryPlan& other_plan) {
  _roots.insert(_roots.end(), other_plan._roots.begin(), other_plan._roots.end());
}

std::vector<std::shared_ptr<OperatorTask>> SQLQueryPlan::create_tasks() const {
  std::vector<std::shared_ptr<OperatorTask>> tasks;

  for (const auto& root : _roots) {
    std::vector<std::shared_ptr<OperatorTask>> sub_list;
    sub_list = OperatorTask::make_tasks_from_operator(root, _cleanup_temporaries);
    tasks.insert(tasks.end(), sub_list.begin(), sub_list.end());
  }

  return tasks;
}

const std::vector<std::shared_ptr<AbstractOperator>>& SQLQueryPlan::tree_roots() const { return _roots; }

SQLQueryPlan SQLQueryPlan::deep_copy() const {
  SQLQueryPlan new_plan{_cleanup_temporaries};

  for (const auto& root : _roots) {
    DebugAssert(root.get() != nullptr, "Root operator in plan should not be null.");
    std::shared_ptr<AbstractOperator> new_root = root->deep_copy();
    new_plan.add_tree_by_root(new_root);
  }

  new_plan._parameter_ids = _parameter_ids;

  return new_plan;
}

void SQLQueryPlan::set_transaction_context(std::shared_ptr<TransactionContext> context) {
  for (const auto& root : _roots) {
    root->set_transaction_context_recursively(context);
  }
}

void SQLQueryPlan::set_parameter_ids(const std::unordered_map<ValuePlaceholderID, ParameterID>& parameter_ids) {
  _parameter_ids = parameter_ids;
}

const std::unordered_map<ValuePlaceholderID, ParameterID>& SQLQueryPlan::parameter_ids() const {
  return _parameter_ids;
}

}  // namespace opossum
