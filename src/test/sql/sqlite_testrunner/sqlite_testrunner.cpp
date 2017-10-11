
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

#include "concurrency/transaction_manager.hpp"
#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_planner.hpp"
#include "sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLiteTestRunner : public BaseTest {
 protected:
  void _set_up() {
    StorageManager::get().reset();
    _sqlite.reset(new SQLiteWrapper());

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

      _sqlite->create_table_from_tbl(table_file, table_name);

      std::shared_ptr<Table> table = load_table(table_file, 0);
      StorageManager::get().add_table(table_name, std::move(table));
    }
  }

  std::unique_ptr<SQLiteWrapper> _sqlite;
};

TEST_F(SQLiteTestRunner, CompareToSQLiteTestRunner) {
  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  std::string query;
  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") {
      continue;
    }

    _set_up();

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    EXPECT_TRUE(parse_result.isValid()) << "Query not valid: " << query;
    if (!parse_result.isValid()) {
      continue;
    }

    auto plan = SQLPlanner::plan(parse_result);

    auto tx_context = TransactionManager::get().new_transaction_context();
    for (const auto& root : plan.tree_roots()) {
      auto tasks = OperatorTask::make_tasks_from_operator(root);

      for (auto& task : tasks) {
        task->get_operator()->set_transaction_context(tx_context);
      }

      CurrentScheduler::schedule_and_wait_for_tasks(tasks);
    }

    auto result_table = plan.tree_roots().back()->get_output();
    auto sqlite_result_table = _sqlite->execute_query(query);

    bool order_sensitive = false;

    if (parse_result.getStatement(0)->is(hsql::kStmtSelect)) {
      auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result.getStatement(0));
      order_sensitive = (select_statement->order != nullptr);
    }

    EXPECT_TRUE(_table_equal(*result_table, *sqlite_result_table, order_sensitive, false)) << "Query failed: " << query;
  }
}

}  // namespace opossum
