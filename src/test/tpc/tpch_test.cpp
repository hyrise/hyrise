#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/lqp_translator.hpp"
#include "operators/abstract_operator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_translator.hpp"
#include "sql/sqlite_testrunner/sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_queries.hpp"

namespace opossum {

class TPCHTest : public BaseTestWithParam<size_t> {
 protected:
  std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;

  void SetUp() override {
    // Chosen rather arbitrarily
    const auto chunk_size = 100;

    std::vector<std::string> tpch_table_names(
        {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"});

    _sqlite_wrapper = std::make_shared<SQLiteWrapper>();

    for (const auto& tpch_table_name : tpch_table_names) {
      const auto tpch_table_path = std::string("src/test/tables/tpch/sf-0.001/") + tpch_table_name + ".tbl";
      StorageManager::get().add_table(tpch_table_name, load_table(tpch_table_path, chunk_size));
      _sqlite_wrapper->create_table_from_tbl(tpch_table_path, tpch_table_name);
    }
  }
};

TEST_P(TPCHTest, TPCHQueryTest) {
  const auto query_idx = GetParam();

  SCOPED_TRACE("TPC-H " + std::to_string(query_idx + 1));

  const auto query = tpch_queries[query_idx];
  const auto sqlite_result_table = _sqlite_wrapper->execute_query(query);

  SQLPipeline sql_pipeline{query, UseMvcc::No};
  const auto& result_table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ(result_table, sqlite_result_table, OrderSensitivity::No, TypeCmpMode::Lenient,
                  FloatComparisonMode::RelativeDifference);
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
  // 14, /* Enable once we support Subselects in WHERE condition */
  // 15, /* Enable once we support Subselects in WHERE condition */
  // 16, /* Enable once we support Subselects in WHERE condition */
  // 17, /* Enable once we support Subselects in WHERE condition */
  // 18, /* Enable once we support OR in WHERE condition */
  // 19, /* Enable once we support Subselects in WHERE condition */
  // 20 /* Enable once we support Exists and Subselect in WHERE condition */
), );  // NOLINT
// clang-format on

}  // namespace opossum
