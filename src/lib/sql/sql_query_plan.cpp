#include "sql_query_plan.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "operators/table_scan.hpp"

namespace opossum {

SQLQueryPlan::SQLQueryPlan(std::vector<std::shared_ptr<OperatorTask>> tasks)
    : _tasks(std::move(tasks)), _num_parameters(0) {}

size_t SQLQueryPlan::size() const { return _tasks.size(); }

std::shared_ptr<OperatorTask> SQLQueryPlan::back() const { return _tasks.back(); }

void SQLQueryPlan::pop_back() { _tasks.pop_back(); }

void SQLQueryPlan::add_task(std::shared_ptr<OperatorTask> task) { _tasks.push_back(task); }

void SQLQueryPlan::append(const SQLQueryPlan& other_plan) {
  _tasks.insert(_tasks.end(), other_plan.tasks().begin(), other_plan.tasks().end());
}

void SQLQueryPlan::clear() { _tasks.clear(); }

const std::vector<std::shared_ptr<OperatorTask>>& SQLQueryPlan::tasks() const { return _tasks; }

SQLQueryPlan SQLQueryPlan::recreate(const std::vector<AllParameterVariant>& arguments) const {
  // Recreate the task tree.
  const std::shared_ptr<const AbstractOperator>& root_operator = this->back()->get_operator();
  std::shared_ptr<AbstractOperator> new_root = root_operator->recreate(arguments);
  const auto new_tasks = OperatorTask::make_tasks_from_operator(new_root);

  // Return a new query plan instance with the new task tree.
  SQLQueryPlan new_plan(std::move(new_tasks));
  return new_plan;
}

void SQLQueryPlan::set_num_parameters(uint16_t num_parameters) { _num_parameters = num_parameters; }

uint16_t SQLQueryPlan::num_parameters() const { return _num_parameters; }

}  // namespace opossum
