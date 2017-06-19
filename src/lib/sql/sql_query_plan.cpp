#include "sql_query_plan.hpp"

#include <memory>
#include <utility>
#include <vector>

namespace opossum {

SQLQueryPlan::SQLQueryPlan(std::vector<std::shared_ptr<OperatorTask>> tasks) : _tasks(std::move(tasks)) {}

size_t SQLQueryPlan::size() const { return _tasks.size(); }

std::shared_ptr<OperatorTask> SQLQueryPlan::back() const { return _tasks.back(); }

void SQLQueryPlan::add_task(std::shared_ptr<OperatorTask> task) { _tasks.push_back(task); }

void SQLQueryPlan::append(const SQLQueryPlan& other_plan) {
  _tasks.insert(_tasks.end(), other_plan.tasks().begin(), other_plan.tasks().end());
}

void SQLQueryPlan::clear() { _tasks.clear(); }

const std::vector<std::shared_ptr<OperatorTask>>& SQLQueryPlan::tasks() const { return _tasks; }

SQLQueryPlan SQLQueryPlan::recreate() const {
  // Recreate the task tree.
  const auto root = this->back();
  std::vector<std::shared_ptr<OperatorTask>> new_tasks;
  new_tasks.reserve(_tasks.size());
  recreate_tasks_deep(root->get_operator(), &new_tasks);

  // Return a new query plan instance with the new task tree.
  SQLQueryPlan new_plan(std::move(new_tasks));
  return new_plan;
}

void SQLQueryPlan::recreate_tasks_deep(const std::shared_ptr<const AbstractOperator>& root_operator,
                                       std::vector<std::shared_ptr<OperatorTask>>* tasks) const {
  auto root_copy = root_operator->recreate();
  auto root_copy_task = std::make_shared<OperatorTask>(root_copy);

  // Traverse left input.
  auto left_op = root_operator->input_left();
  if (left_op.get() != nullptr) {
    recreate_tasks_deep(left_op, tasks);

    root_copy->set_input_left(tasks->back()->get_operator());
    tasks->back()->set_as_predecessor_of(root_copy_task);
  }

  // Traverse right input.
  auto right_op = root_operator->input_right();
  if (right_op.get() != nullptr) {
    recreate_tasks_deep(right_op, tasks);

    root_copy->set_input_right(tasks->back()->get_operator());
    tasks->back()->set_as_predecessor_of(root_copy_task);
  }

  // Add the root task at the end.
  tasks->push_back(std::move(root_copy_task));
}

}  // namespace opossum
