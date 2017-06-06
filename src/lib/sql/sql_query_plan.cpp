#include "sql_query_plan.hpp"

namespace opossum {

SQLQueryPlan::SQLQueryPlan() {}

size_t SQLQueryPlan::size() const { return _tasks.size(); }

std::shared_ptr<OperatorTask> SQLQueryPlan::back() { return _tasks.back(); }

void SQLQueryPlan::add(std::shared_ptr<OperatorTask> task) { _tasks.push_back(task); }

void SQLQueryPlan::clear() { _tasks.clear(); }

const std::vector<std::shared_ptr<OperatorTask>>& SQLQueryPlan::tasks() const { return _tasks; }

}  // namespace opossum
