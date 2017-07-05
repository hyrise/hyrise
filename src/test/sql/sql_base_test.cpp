#include "sql_base_test.hpp"

#include <memory>
#include <string>
#include <utility>

#include "sql/sql_query_operator.hpp"

namespace opossum {

std::shared_ptr<OperatorTask> SQLBaseTest::execute_query_task(const std::string& query, bool schedule_plan) {
  auto op = std::make_shared<SQLQueryOperator>(query, schedule_plan);
  auto task = std::make_shared<OperatorTask>(op);
  task->execute();

  if (op->parse_tree_cache_hit()) {
    parse_tree_cache_hits++;
  }

  if (op->query_plan_cache_hit()) {
    query_plan_cache_hits++;
  }
  return task;
}

void SQLBaseTest::schedule_query_task(const std::string& query) {
  auto op = std::make_shared<SQLQueryOperator>(query);
  auto task = std::make_shared<OperatorTask>(op);
  task->schedule();
}

}  // namespace opossum
