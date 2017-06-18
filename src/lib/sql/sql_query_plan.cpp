#include "sql_query_plan.hpp"

#include <memory>
#include <vector>

namespace opossum {

SQLQueryPlan::SQLQueryPlan() {}

size_t SQLQueryPlan::size() const { return _tasks.size(); }

std::shared_ptr<OperatorTask> SQLQueryPlan::back() const { return _tasks.back(); }

void SQLQueryPlan::add_task(std::shared_ptr<OperatorTask> task) { _tasks.push_back(task); }

void SQLQueryPlan::clear() { _tasks.clear(); }

void SQLQueryPlan::append(const SQLQueryPlan& other_plan) {
  _tasks.insert(_tasks.end(), other_plan.tasks().begin(), other_plan.tasks().end());
}

std::shared_ptr<OperatorTask> SQLQueryPlan::task(size_t index) const { return _tasks[index]; }

const std::vector<std::shared_ptr<OperatorTask>>& SQLQueryPlan::tasks() const { return _tasks; }

std::vector<std::shared_ptr<OperatorTask>> SQLQueryPlan::cloneTasks() const {
  const auto root = this->back();
  return deepCloneTasks(root->get_operator());
}

std::vector<std::shared_ptr<OperatorTask>> SQLQueryPlan::deepCloneTasks(std::shared_ptr<const AbstractOperator> root) const {
  std::vector<std::shared_ptr<OperatorTask>> tasks;

  auto root_copy = root->clone();
  auto root_copy_task = std::make_shared<OperatorTask>(root_copy);

  auto left_op = root->input_left();
  if (left_op.get() != nullptr) {
    auto left_tasks = deepCloneTasks(left_op);
    tasks.insert(tasks.end(), left_tasks.begin(), left_tasks.end());

    root_copy->set_input_left(left_tasks.back()->get_operator());
    left_tasks.back()->set_as_predecessor_of(root_copy_task);
  }

  auto right_op = root->input_right();
  if (right_op.get() != nullptr) {
    auto right_tasks = deepCloneTasks(right_op);
    tasks.insert(tasks.end(), right_tasks.begin(), right_tasks.end());

    root_copy->set_input_left(right_tasks.back()->get_operator());
    right_tasks.back()->set_as_predecessor_of(root_copy_task);
  }

  tasks.push_back(root_copy_task);
  return tasks;
}

}  // namespace opossum
