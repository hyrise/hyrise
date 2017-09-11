
#include <memory>
#include <string>
#include <fstream>
#include <iostream>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

// #include "operators/get_table.hpp"
// #include "operators/table_scan.hpp"
#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
// #include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
// #include "scheduler/topology.hpp"
#include "sql/sql_planner.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLSQLiteRunnerTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));
  }
};

TEST_F(SQLSQLiteRunnerTest, CompareToSQLiteTestRunner) {
  std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);

  std::ifstream file("src/test/sql/sql_sqlite_runner_test.testqueries");
  std::string query;
  while (std::getline(file, query))
  {
    if (query.empty()) { continue; }
    std::cout << query << std::endl;

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    // Expect the query to be a single statement.
    auto plan = SQLPlanner::plan(parse_result);
    auto result_operator = plan.tree_roots().front();

    auto tasks = OperatorTask::make_tasks_from_operator(result_operator);
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);

    auto result_table = tasks.back()->get_operator()->get_output();
    // Print::print(result_table, 0);

    EXPECT_TABLE_EQ(result_table, table_a, true);
  }
}

}  // namespace opossum
