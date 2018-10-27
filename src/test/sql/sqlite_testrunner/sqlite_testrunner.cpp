#include <boost/algorithm/string/predicate.hpp>
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
#include "utils/load_table.hpp"

namespace opossum {

using TestConfiguration = std::tuple<std::string, bool, bool>;  // SQL Query, use_jit, use_mvcc

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

      std::shared_ptr<Table> table = load_table(table_file, 10);
      StorageManager::get().add_table(table_name, std::move(table));
    }

    opossum::Topology::use_numa_topology();
    opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::unique_ptr<SQLiteWrapper> _sqlite;
};

std::vector<TestConfiguration> read_queries_from_file() {
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
    tests.push_back({query, false, false});
    if constexpr (HYRISE_JIT_SUPPORT) {
      tests.push_back({query, true, true});
      // If validate is not present, there is the possibility that one JitOperatorWrapper can combine more operators
      if (boost::starts_with(query, "SELECT")) {
        tests.push_back({query, true, false});
      }
    }
  }
  return tests;
}

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  const auto& [query, use_jit, use_mvcc] = GetParam();

  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    lqp_translator = std::make_shared<JitAwareLQPTranslator>();
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }

  SCOPED_TRACE("SQLite " + query + " " + (use_jit ? "with JIT" : "without JIT") + " " +
               (use_mvcc ? "with MVCC" : "without MVCC"));

  const auto prepared_statement_cache = std::make_shared<PreparedStatementCache>();

  auto sql_pipeline = SQLPipelineBuilder{query}
                          .with_prepared_statement_cache(prepared_statement_cache)
                          .with_lqp_translator(lqp_translator)
                          .with_mvcc(UseMvcc(use_mvcc))
                          .create_pipeline();

  const auto& result_table = sql_pipeline.get_result_table();

  auto sqlite_result_table = _sqlite->execute_query(query);

  // The problem is that we can only infer column types from sqlite if they have at least one row.
  ASSERT_TRUE(result_table && result_table->row_count() > 0 && sqlite_result_table &&
              sqlite_result_table->row_count() > 0)
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
                        testing::ValuesIn(read_queries_from_file()), );  // NOLINT

}  // namespace opossum
