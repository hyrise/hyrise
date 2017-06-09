#include "sql_query_plan.hpp"

#include <memory>
#include <vector>

namespace opossum {

SQLQueryPlan::SQLQueryPlan() {}

size_t SQLQueryPlan::size() const { return _tasks.size(); }

std::shared_ptr<OperatorTask> SQLQueryPlan::back() { return _tasks.back(); }

void SQLQueryPlan::addTask(std::shared_ptr<OperatorTask> task) { _tasks.push_back(task); }

void SQLQueryPlan::clear() { _tasks.clear(); }

void SQLQueryPlan::append(const SQLQueryPlan& other_plan) {
  _tasks.insert(_tasks.end(), other_plan.tasks().begin(), other_plan.tasks().end());
}

const std::vector<std::shared_ptr<OperatorTask>>& SQLQueryPlan::tasks() const { return _tasks; }

}  // namespace opossum
