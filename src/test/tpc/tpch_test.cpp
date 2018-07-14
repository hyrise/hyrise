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
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "sql/sqlite_testrunner/sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

using namespace std::string_literals;

namespace opossum {

class TPCHTest : public BaseTestWithParam<std::pair<const size_t, const char*>> {
 protected:
  void SetUp() override {
    _sqlite_wrapper = std::make_shared<SQLiteWrapper>();
  }

  std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;

  std::vector<std::string> tpch_table_names{{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}};
  
  std::unordered_map<size_t, std::string> scale_factor_by_query{
    {1, "0.001"},
    {2, "0.01"},
    {3, "0.001"},
    {4, "0.001"},
    {5, "0.001"},
    {6, "0.001"},
    {7, "0.001"},
    {8, "0.001"},
    {9, "0.001"},
    {10, "0.001"},
    {11, "0.01"},
    {12, "0.001"},
    {13, "0.001"},
    {14, "0.01"},
    {15, "0.01"},
    {16, "0.001"},
    {17, "0.001"},
    {18, "0.01"},
    {19, "0.001"},
    {20, "0.01"},
    {21, "0.01"},
    {22, "0.001"}
  };  
};

TEST_P(TPCHTest, TPCHQueryTest) {
  size_t query_idx;
  const char* query;
  std::tie(query_idx, query) = GetParam();
  const auto scale_factor_str = scale_factor_by_query.at(query_idx);

  // Chosen rather arbitrarily
  const auto chunk_size = 1'000;

  std::cout << "Query " << query_idx << "; Scale Factor: " << scale_factor_str << std::endl;

  /** Load tables appropriately sized for this Query */
  for (const auto& tpch_table_name : tpch_table_names) {
    const auto tpch_table_path = std::string("src/test/tables/tpch/sf-"s + scale_factor_str + "/") + tpch_table_name + ".tbl";
    StorageManager::get().add_table(tpch_table_name, load_table(tpch_table_path, chunk_size));
    _sqlite_wrapper->create_table_from_tbl(tpch_table_path, tpch_table_name);
  }

  SCOPED_TRACE("TPC-H " + std::to_string(query_idx));

  const auto sqlite_result_table = _sqlite_wrapper->execute_query(query);
  auto sql_pipeline = SQLPipelineBuilder{query}.disable_mvcc().create_pipeline();

  if (!sql_pipeline.requires_execution()) {
    sql_pipeline.get_unoptimized_logical_plans().at(0)->print();
    sql_pipeline.get_optimized_logical_plans().at(0)->print();
  } else {
    std::cout << "Cannot print plan, needs to be executed first" << std::endl;
  }

  if (query_idx == 17) {
    FAIL();
  }

  const auto result_table = sql_pipeline.get_result_table();

  // EXPECT_TABLE_EQ would crash if one table is a nullptr
  ASSERT_TRUE(result_table);
  ASSERT_TRUE(sqlite_result_table);

  EXPECT_TABLE_EQ(result_table, sqlite_result_table, OrderSensitivity::No, TypeCmpMode::Lenient,
                  FloatComparisonMode::RelativeDifference);
}

// clang-format off
INSTANTIATE_TEST_CASE_P(TPCHTestInstances, TPCHTest, ::testing::ValuesIn(tpch_queries), );  // NOLINT
// clang-format on

}  // namespace opossum
