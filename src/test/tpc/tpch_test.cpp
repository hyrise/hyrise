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

using namespace std::string_literals;  // NOLINT

namespace opossum {

class TPCHTest : public BaseTestWithParam<std::pair<const size_t, const char*>> {
 public:
  void SetUp() override { _sqlite_wrapper = std::make_shared<SQLiteWrapper>(); }

  std::shared_ptr<SQLiteWrapper> _sqlite_wrapper;

  std::vector<std::string> tpch_table_names{
      {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}};

  // Scale factors chosen so the query
  //   -> actually returns result rows (which some don't for small scale factors)
  //   -> doesn't crush a 16GB dev machine
  //   -> runs for a few seconds on a release build
  std::unordered_map<size_t, float> scale_factor_by_query{
      {1, 0.01f},   {2, 0.004f},  {3, 0.01f},  {4, 0.005f},  {5, 0.01f},    {6, 0.01f},  {7, 0.01f},  {8, 0.01f},
      {9, 0.01f},   {10, 0.02f},  {11, 0.01f}, {12, 0.01f},  {13, 0.01f},   {14, 0.01f}, {15, 0.01f}, {16, 0.01f},
      {17, 0.013f}, {18, 0.005f}, {19, 0.01f}, {20, 0.008f}, {21, 0.0075f}, {22, 0.01f}};
};

TEST_P(TPCHTest, TPCHQueryTest) {
  size_t query_idx;
  std::string query;
  std::tie(query_idx, query) = GetParam();

  /**
   * Generate the TPC-H tables with a scale factor appropriate for this query
   */
  const auto scale_factor = scale_factor_by_query.at(query_idx);

  TpchDbGenerator{scale_factor, 10'000}.generate_and_store();
  for (const auto& tpch_table_name : tpch_table_names) {
    const auto table = StorageManager::get().get_table(tpch_table_name);
    _sqlite_wrapper->create_table(*table, tpch_table_name);
  }

  SCOPED_TRACE("TPC-H " + std::to_string(query_idx));

  std::shared_ptr<const Table> sqlite_result_table, hyrise_result_table;

  auto sql_pipeline = SQLPipelineBuilder{query}.disable_mvcc().create_pipeline();

  // TPC-H 15 needs special patching as it contains a DROP VIEW that doesn't return a table as last statement
  if (query_idx == 15) {
    Assert(sql_pipeline.statement_count() == 3u, "Expected 3 statements in TPC-H 15") sql_pipeline.get_result_table();

    hyrise_result_table = sql_pipeline.get_result_tables()[1];

    // Omit the "DROP VIEW" from the SQLite query
    const auto sqlite_query = sql_pipeline.get_sql_strings()[0] + sql_pipeline.get_sql_strings()[1];
    sqlite_result_table = _sqlite_wrapper->execute_query(sqlite_query);
  } else {
    sqlite_result_table = _sqlite_wrapper->execute_query(query);
    hyrise_result_table = sql_pipeline.get_result_table();
  }

  // EXPECT_TABLE_EQ crashes if one table is a nullptr
  ASSERT_TRUE(hyrise_result_table);
  ASSERT_TRUE(sqlite_result_table);

  EXPECT_TABLE_EQ(hyrise_result_table, sqlite_result_table, OrderSensitivity::No, TypeCmpMode::Lenient,
                  FloatComparisonMode::RelativeDifference);
}

// clang-format off
INSTANTIATE_TEST_CASE_P(TPCHTestInstances, TPCHTest, ::testing::ValuesIn(tpch_queries), );  // NOLINT
// clang-format on

}  // namespace opossum
