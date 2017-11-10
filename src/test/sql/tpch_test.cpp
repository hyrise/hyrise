#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_operator.hpp"
#include "operators/print.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "sql/sqlite_testrunner/sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_queries.hpp"

namespace opossum {

class TPCHTest : public ::testing::TestWithParam<size_t> {
 protected:
  std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;

  void SetUp() override {
    // Chosen rather arbitrarily
    const auto chunk_size = 100;

    std::vector<std::string> tpch_table_names({"customer", "lineitem", "nation", "orders", "part", "partsupp", "region",
                                               "supplier"});

    _sqlite_wrapper = std::make_shared<SQLiteWrapper>();

    for (const auto &tpch_table_name : tpch_table_names) {
      const auto tpch_table_path = std::string("src/test/tables/tpch/") + tpch_table_name + ".tbl";
      StorageManager::get().add_table(tpch_table_name, load_table(tpch_table_path, chunk_size));
      _sqlite_wrapper->create_table_from_tbl(tpch_table_path, tpch_table_name);
    }
  }

  void TearDown() override {
    StorageManager::reset();
  }

  std::shared_ptr<AbstractOperator> translate_query_to_operator(const std::string query, bool optimize) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      std::cout << parse_result.errorMsg() << std::endl;
      std::cout << "ErrorLine: " << parse_result.errorLine() << std::endl;
      std::cout << "ErrorColumn: " << parse_result.errorColumn() << std::endl;
      throw std::runtime_error("Query is not valid.");
    }

    auto result_node = SQLToASTTranslator{false}.translate_parse_result(parse_result)[0];

    if (optimize) {
      result_node = Optimizer::get().optimize(result_node);
    }

    return ASTToOperatorTranslator{}.translate_node(result_node);
  }

  std::shared_ptr<OperatorTask> schedule_query_and_return_task(const std::string query, bool optimize) {
    auto result_operator = translate_query_to_operator(query, optimize);
    auto tasks = OperatorTask::make_tasks_from_operator(result_operator);
    for (auto& task : tasks) {
      task->schedule();
    }
    return tasks.back();
  }

  void execute_and_check(const std::string query, std::shared_ptr<Table> expected_result,
                         bool order_sensitive = false) {
    auto result_optimized = schedule_query_and_return_task(query, true)->get_operator()->get_output();
    Print::print(result_optimized);
    EXPECT_TABLE_EQ(result_optimized, expected_result, order_sensitive, false);
  }
};

TEST_P(TPCHTest, TPCHQueryTest) {
  const auto query_idx = GetParam();

  SCOPED_TRACE("TPC-H " + std::to_string(query_idx + 1));

  const auto query = tpch_queries[query_idx];
  const auto sqlite_result_table = _sqlite_wrapper->execute_query(query);

  execute_and_check(query, sqlite_result_table, true);
}

// clang-format off
INSTANTIATE_TEST_CASE_P(TPCHTestInstances, TPCHTest, ::testing::Values(
  0,
  // 1, /* // Enable once we support Subselects in WHERE condition */
  2,
  // 3, /* Enable once we support Exists and Subselects in WHERE condition */
  4,
  5,
  6,
  // 7, /* Enable once CASE and arithmetic operations of Aggregations are supported */
  8,
  9
  // 10, /* Enable once we support Subselects in Having clause */
  // 11, /* Enable once we support IN */
  // 12, /* Enable once we support nested expressions in Join Condition */
  // 13, /* Enable once we support Case */
  // 14, /* We do not support Views yet */
  // 15, /* Enable once we support Subselects in WHERE condition */
  // 16, /* Enable once we support Subselects in WHERE condition */
  // 17, /* Enable once we support Subselects in WHERE condition */
  // 18, /* Enable once we support OR in WHERE condition */
  // 19, /* Enable once we support Subselects in WHERE condition */
  // 20 /* Enable once we support Exists and Subselect in WHERE condition */
), );  // NOLINT
// clang-format on

}  // namespace opossum
