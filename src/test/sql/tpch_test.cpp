#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "operators/abstract_operator.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_queries.hpp"

namespace opossum {

class TPCHTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> customer = load_table("src/test/tables/tpch/customer.tbl", 2);
    std::shared_ptr<Table> lineitem = load_table("src/test/tables/tpch/lineitem.tbl", 2);
    std::shared_ptr<Table> nation = load_table("src/test/tables/tpch/nation.tbl", 2);
    std::shared_ptr<Table> orders = load_table("src/test/tables/tpch/orders.tbl", 2);
    std::shared_ptr<Table> part = load_table("src/test/tables/tpch/part.tbl", 2);
    std::shared_ptr<Table> partsupplier = load_table("src/test/tables/tpch/partsupplier.tbl", 2);
    std::shared_ptr<Table> region = load_table("src/test/tables/tpch/region.tbl", 2);
    std::shared_ptr<Table> supplier = load_table("src/test/tables/tpch/supplier.tbl", 2);
    StorageManager::get().add_table("customer", std::move(customer));
    StorageManager::get().add_table("lineitem", std::move(lineitem));
    StorageManager::get().add_table("nation", std::move(nation));
    StorageManager::get().add_table("orders", std::move(orders));
    StorageManager::get().add_table("part", std::move(part));
    StorageManager::get().add_table("partsupp", std::move(partsupplier));
    StorageManager::get().add_table("region", std::move(region));
    StorageManager::get().add_table("supplier", std::move(supplier));
  }

  std::shared_ptr<AbstractOperator> translate_query_to_operator(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      std::cout << parse_result.errorMsg() << std::endl;
      std::cout << "ErrorLine: " << parse_result.errorLine() << std::endl;
      std::cout << "ErrorColumn: " << parse_result.errorColumn() << std::endl;
      throw std::runtime_error("Query is not valid.");
    }

    auto result_node = SQLToASTTranslator::get().translate_parse_result(parse_result)[0];
    return ASTToOperatorTranslator::get().translate_node(result_node);
  }

  std::shared_ptr<OperatorTask> schedule_query_and_return_task(const std::string query) {
    auto result_operator = translate_query_to_operator(query);
    auto tasks = OperatorTask::make_tasks_from_operator(result_operator);
    for (auto& task : tasks) {
      task->schedule();
    }
    return tasks.back();
  }

  void execute_and_check(const std::string query, std::shared_ptr<Table> expected_result,
                         bool order_sensitive = false) {
    auto result_task = schedule_query_and_return_task(query);
    EXPECT_TABLE_EQ(result_task->get_operator()->get_output(), expected_result, order_sensitive);
  }
};

TEST_F(TPCHTest, TPCH1) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch1.tbl", 2);
  execute_and_check(tpch_queries[0], expected_result, true);
}

// Enable once we support Subselects in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH2) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch2.tbl", 2);
  execute_and_check(tpch_queries[1], expected_result, true);
}

TEST_F(TPCHTest, TPCH3) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch3.tbl", 2);
  execute_and_check(tpch_queries[2], expected_result, true);
}

// Enable once we support Exists and Subselects in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH4) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch4.tbl", 2);
  execute_and_check(tpch_queries[3], expected_result, true);
}

TEST_F(TPCHTest, TPCH5) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch5.tbl", 2);
  execute_and_check(tpch_queries[4], expected_result, true);
}

TEST_F(TPCHTest, TPCH6) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch6.tbl", 2);
  execute_and_check(tpch_queries[5], expected_result, true);
}

// Enable once OR is supported in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH7) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch7.tbl", 2);
  execute_and_check(tpch_queries[6], expected_result, true);
}

// Enable once CASE and arithmetic operations of Aggregations are supported
TEST_F(TPCHTest, DISABLED_TPCH8) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch8.tbl", 2);
  execute_and_check(tpch_queries[7], expected_result, true);
}

TEST_F(TPCHTest, TPCH9) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch9.tbl", 2);
  execute_and_check(tpch_queries[8], expected_result, true);
}

TEST_F(TPCHTest, TPCH10) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch10.tbl", 2);
  execute_and_check(tpch_queries[9], expected_result, true);
}

// Enable once we support Subselects in Having clause
TEST_F(TPCHTest, DISABLED_TPCH11) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch11.tbl", 2);
  execute_and_check(tpch_queries[10], expected_result, true);
}

// Enable once we support IN
TEST_F(TPCHTest, DISABLED_TPCH12) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch12.tbl", 2);
  execute_and_check(tpch_queries[11], expected_result, true);
}

// Enable once we support nested expressions in Join Condition
TEST_F(TPCHTest, DISABLED_TPCH13) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch13.tbl", 2);
  execute_and_check(tpch_queries[12], expected_result, true);
}

// Enable once we support Case
TEST_F(TPCHTest, DISABLED_TPCH14) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch14.tbl", 2);
  execute_and_check(tpch_queries[13], expected_result, true);
}

// We do not support Views yet
TEST_F(TPCHTest, DISABLED_TPCH15) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch15.tbl", 2);
  execute_and_check(tpch_queries[14], expected_result, true);
}

// Enable once we support Subselects in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH16) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch16.tbl", 2);
  execute_and_check(tpch_queries[15], expected_result, true);
}

// Enable once we support Subselect in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH17) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch17.tbl", 2);
  execute_and_check(tpch_queries[16], expected_result, true);
}

// Enable once we support Subselects in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH18) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch18.tbl", 2);
  execute_and_check(tpch_queries[17], expected_result, true);
}

// Enable once we support OR in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH19) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch19.tbl", 2);
  execute_and_check(tpch_queries[18], expected_result, true);
}

// Enable once we support Subselects in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH20) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch20.tbl", 2);
  execute_and_check(tpch_queries[19], expected_result, true);
}

// Enable once we support Exists and Subselect in WHERE condition
TEST_F(TPCHTest, DISABLED_TPCH21) {
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch21.tbl", 2);
  execute_and_check(tpch_queries[20], expected_result, true);
}



}  // namespace opossum
