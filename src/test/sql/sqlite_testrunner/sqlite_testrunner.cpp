
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_planner.hpp"
#include "sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLiteTestRunner : public BaseTest {
 protected:
  void SetUp() override {
    std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner.tables");
    std::string line;
    while (std::getline(file, line)) {
      if (line.empty()) {
        continue;
      }

      std::vector<std::string> args;
      boost::algorithm::split(args, line, boost::is_space());

      if (args.size() != 2) {
        continue;
      }

      std::string table_file = args.at(0);
      std::string table_name = args.at(1);

      _sqlite.create_table_from_tbl(table_file, table_name);

      std::shared_ptr<Table> table = load_table(table_file, 0);
      StorageManager::get().add_table(table_name, std::move(table));
    }
  }

  SqliteWrapper _sqlite;
};

TEST_F(SQLiteTestRunner, CompareToSQLiteTestRunner) {
  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner.testqueries");
  std::string query;
  while (std::getline(file, query)) {
    if (query.empty()) {
      continue;
    }

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

    std::cout << "Testing query: " << query << " ..." << std::endl;

    auto result_table = tasks.back()->get_operator()->get_output();
    auto sqlite_result_table = _sqlite.execute_query(query);

    EXPECT_TABLE_EQ(result_table, sqlite_result_table, true);
  }
}

}  // namespace opossum
