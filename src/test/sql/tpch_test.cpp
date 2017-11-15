#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_operator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_queries.hpp"

namespace opossum {

class TPCHTest : public ::testing::TestWithParam<size_t> {
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

  std::shared_ptr<AbstractOperator> translate_query_to_operator(const std::string query, bool optimize) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      std::cout << parse_result.errorMsg() << std::endl;
      std::cout << "ErrorLine: " << parse_result.errorLine() << std::endl;
      std::cout << "ErrorColumn: " << parse_result.errorColumn() << std::endl;
      throw std::runtime_error("Query is not valid.");
    }

    auto result_node = SQLTranslator{false}.translate_parse_result(parse_result)[0];

    if (optimize) {
      result_node = Optimizer::get().optimize(result_node);
    }

    return LQPTranslator{}.translate_node(result_node);
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
    auto result_unoptimized = schedule_query_and_return_task(query, false)->get_operator()->get_output();
    EXPECT_TABLE_EQ(result_unoptimized, expected_result, order_sensitive);

    auto result_optimized = schedule_query_and_return_task(query, true)->get_operator()->get_output();
    EXPECT_TABLE_EQ(result_optimized, expected_result, order_sensitive);
  }
};

TEST_P(TPCHTest, TPCHQueryTest) {
  const auto query_idx = GetParam();

  SCOPED_TRACE("TPC-H " + std::to_string(query_idx + 1));

  const auto query = tpch_queries[query_idx];
  const auto expected_result =
      load_table("src/test/tables/tpch/results/tpch" + std::to_string(query_idx + 1) + ".tbl", 2);

  execute_and_check(query, expected_result, true);
}

// clang-format off
INSTANTIATE_TEST_CASE_P(TPCHTestInstances, TPCHTest, ::testing::Values(
  0,
  // 1, /* // Enable once we support Subselects in WHERE condition */
  2,
  // 3, /* Enable once we support Exists and Subselects in WHERE condition */
  4,
  5,
  // 6, /* Enable once OR is supported in WHERE condition */
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
