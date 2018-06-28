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
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

using TestConfiguration = std::pair<std::string, bool>;  // SQL Query, use_jit

class SQLiteTestRunner : public BaseTestWithParam<TestConfiguration> {
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

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::unique_ptr<SQLiteWrapper> _sqlite;
};

std::vector<TestConfiguration> build_combinations() {
  std::vector<std::string> queries;
  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  std::string query;
  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") {
      continue;
    }
    queries.emplace_back(std::move(query));
  }

  std::vector<TestConfiguration> tests;
  for (const auto& query : queries) {
    tests.push_back({query, false});
    if constexpr (HYRISE_JIT_SUPPORT) {
      tests.push_back({query, true});
    }
  }
  return tests;
}

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  const auto& [query, use_jit] = GetParam();

  SCOPED_TRACE("TPC-H " + query + " " + (use_jit ? "with JIT" : "without JIT"));

  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    if constexpr (HYRISE_JIT_SUPPORT) {
      lqp_translator = std::make_shared<JitAwareLQPTranslator>();
    }
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }

  auto sql_pipeline = SQLPipelineBuilder{query}.with_lqp_translator(lqp_translator).create_pipeline();
  const auto& result_table = sql_pipeline.get_result_table();

  auto sqlite_result_table = _sqlite->execute_query(query);

  // The problem is that we can only infer columns from sqlite if they have at least one row.
  ASSERT_TRUE(result_table->row_count() > 0 && sqlite_result_table->row_count() > 0)
      << "The SQLiteTestRunner cannot handle queries without results";

  auto order_sensitivity = OrderSensitivity::No;

  const auto& parse_result = sql_pipeline.get_parsed_sql_statements().back();
  if (parse_result->getStatements().front()->is(hsql::kStmtSelect)) {
    auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result->getStatements().back());
    if (select_statement->order != nullptr) {
      order_sensitivity = OrderSensitivity::Yes;
    }
  }

  ASSERT_TRUE(check_table_equal(result_table, sqlite_result_table, order_sensitivity, TypeCmpMode::Lenient,
                                FloatComparisonMode::RelativeDifference))
      << "Query failed: " << query;
}

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerInstances, SQLiteTestRunner,
                        testing::ValuesIn(build_combinations()), );  // NOLINT

}  // namespace opossum
