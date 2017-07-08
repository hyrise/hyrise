#include "sql_query_plan.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "operators/table_scan.hpp"

namespace opossum {

size_t operator_tree_size(const std::shared_ptr<const AbstractOperator>& root) {
  size_t num = 1;
  if (root->input_left()) {
    num += operator_tree_size(root->input_left());
  }
  if (root->input_right()) {
    num += operator_tree_size(root->input_right());
  }
  return num;
}

SQLQueryPlan::SQLQueryPlan() : _num_parameters(0) {}

void SQLQueryPlan::add_tree() { _roots.emplace_back(); }

void SQLQueryPlan::set_root(std::shared_ptr<AbstractOperator> op) {
  DebugAssert(_roots.size() > 0, "Query plan should contain at lest one operator tree.");
  _roots.back() = op;
}

const std::shared_ptr<AbstractOperator>& SQLQueryPlan::root() const { return _roots.back(); }

size_t SQLQueryPlan::num_trees() const { return _roots.size(); }

size_t SQLQueryPlan::num_operators() const {
  size_t num = 0;
  for (const auto root : _roots) {
    DebugAssert(root.get() != nullptr, "Root operator in plan should not be null.");
    num += operator_tree_size(root);
  }
  return num;
}

void SQLQueryPlan::append_plan(const SQLQueryPlan& other_plan) {
  _roots.insert(_roots.end(), other_plan.roots().begin(), other_plan.roots().end());
}

void SQLQueryPlan::clear() { _roots.clear(); }

std::vector<std::shared_ptr<OperatorTask>> SQLQueryPlan::tasks() const {
  std::vector<std::shared_ptr<OperatorTask>> tasks;

  for (const auto root : _roots) {
    std::vector<std::shared_ptr<OperatorTask>> sub_list;
    sub_list = OperatorTask::make_tasks_from_operator(root);
    tasks.insert(tasks.end(), sub_list.begin(), sub_list.end());
  }

  return tasks;
}

const std::vector<std::shared_ptr<AbstractOperator>>& SQLQueryPlan::roots() const { return _roots; }

SQLQueryPlan SQLQueryPlan::recreate(const std::vector<AllParameterVariant>& arguments) const {
  SQLQueryPlan new_plan;

  for (const auto& root : _roots) {
    DebugAssert(root.get() != nullptr, "Root operator in plan should not be null.");
    std::shared_ptr<AbstractOperator> new_root = root->recreate(arguments);
    new_plan.add_tree();
    new_plan.set_root(new_root);
  }

  return new_plan;
}

void SQLQueryPlan::set_num_parameters(uint16_t num_parameters) { _num_parameters = num_parameters; }

uint16_t SQLQueryPlan::num_parameters() const { return _num_parameters; }

}  // namespace opossum
