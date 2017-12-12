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

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_planner.hpp"
#include "sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLiteTestRunner : public BaseTestWithParam<std::string> {
 protected:
  void SetUp() override {
    StorageManager::get().reset();
    _sqlite = std::make_unique<SQLiteWrapper>();

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

      DebugAssert(!StorageManager::get().has_table(table_name), "Table already loaded");

      _sqlite->create_table_from_tbl(table_file, table_name);

      std::shared_ptr<Table> table = load_table(table_file, Chunk::MAX_SIZE);
      StorageManager::get().add_table(table_name, std::move(table));
    }

    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_numa_topology()));
  }

  std::unique_ptr<SQLiteWrapper> _sqlite;
};

std::vector<std::string> read_queries_from_file() {
  std::vector<std::string> queries;

  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  std::string query;
  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") {
      continue;
    }
    queries.emplace_back(std::move(query));
  }

  return queries;
}

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  const std::string query = GetParam();

  SQLPipeline sql_pipeline{query};
  const auto& result_table = sql_pipeline.get_result_table();

  auto sqlite_result_table = _sqlite->execute_query(query);

  // The problem is that we can only infer columns from sqlite if they have at least one row.
  ASSERT_TRUE(result_table->row_count() > 0 && sqlite_result_table->row_count() > 0)
      << "The SQLiteTestRunner cannot handle queries without results";

  auto order_sensitivity = OrderSensitivity::No;

  const auto& parse_result = sql_pipeline.get_parsed_sql();
  if (parse_result.getStatements().back()->is(hsql::kStmtSelect)) {
    auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result.getStatements().back());
    if (select_statement->order != nullptr) {
      order_sensitivity = OrderSensitivity::Yes;
    }
  }

  ASSERT_TRUE(check_table_equal(result_table, sqlite_result_table, order_sensitivity, TypeCmpMode::Lenient,
                                FloatComparisonMode::RelativeDifference))
      << "Query failed: " << query;
}

auto formatter = [](const testing::TestParamInfo<std::string>) {
  // stupid, but otherwise Wextra complains about the unused macro parameter
  static int test = 1;
  return std::to_string(test++);
};
INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerInstances, SQLiteTestRunner, testing::ValuesIn(read_queries_from_file()),
                        formatter);

}  // namespace opossum
