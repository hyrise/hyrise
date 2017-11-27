#include "sql_query_plan.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "operators/table_scan.hpp"

namespace opossum {

SQLQueryPlan::SQLQueryPlan() : _num_parameters(0) {}

void SQLQueryPlan::add_tree_by_root(std::shared_ptr<AbstractOperator> op) { _roots.push_back(op); }

void SQLQueryPlan::append_plan(const SQLQueryPlan& other_plan) {
  _roots.insert(_roots.end(), other_plan._roots.begin(), other_plan._roots.end());
}

std::vector<std::shared_ptr<OperatorTask>> SQLQueryPlan::create_tasks() const {
  std::vector<std::shared_ptr<OperatorTask>> tasks;

  for (const auto& root : _roots) {
    std::vector<std::shared_ptr<OperatorTask>> sub_list;
    sub_list = OperatorTask::make_tasks_from_operator(root);
    tasks.insert(tasks.end(), sub_list.begin(), sub_list.end());
  }

  return tasks;
}

const std::vector<std::shared_ptr<AbstractOperator>>& SQLQueryPlan::tree_roots() const { return _roots; }

SQLQueryPlan SQLQueryPlan::recreate(const std::vector<AllParameterVariant>& arguments) const {
  SQLQueryPlan new_plan;

  for (const auto& root : _roots) {
    DebugAssert(root.get() != nullptr, "Root operator in plan should not be null.");
    std::shared_ptr<AbstractOperator> new_root = root->recreate(arguments);
    new_plan.add_tree_by_root(new_root);
  }

  return new_plan;
}

void SQLQueryPlan::set_transaction_context(std::shared_ptr<TransactionContext> context) {
  for (const auto& root : _roots) {
    root->set_transaction_context_recursively(context);
  }
}

void SQLQueryPlan::set_num_parameters(uint16_t num_parameters) { _num_parameters = num_parameters; }

uint16_t SQLQueryPlan::num_parameters() const { return _num_parameters; }

}  // namespace opossum
