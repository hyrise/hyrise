
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql_planner.hpp"
#include "utils/load_table.hpp"
#include "operators/print.hpp"

#include "SQLParserResult.h"
#include "SQLParser.h"

using namespace opossum;

int main() {
  StorageManager::get().add_table("int_float4", load_table("src/test/tables/int_float4.tbl", 2));

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString("SELECT * FROM int_float4 WHERE a > 12345 OR b < 351.0 OR (b > 457.0 AND b < 458.0)",
      &parse_result);

  Assert (parse_result.isValid(), "");

  auto plan = SQLPlanner::plan(parse_result);
  auto tasks = OperatorTask::make_tasks_from_operator(plan.tree_roots()[0]);

  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  auto result_operator = tasks.back()->get_operator();

  Print::print(result_operator->get_output());

  return 0;
}
