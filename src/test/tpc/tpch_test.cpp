#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/abstract_operator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

using TPCHTestParam = std::tuple<BenchmarkItemID, bool /* use_jit */, bool /* use_prepared_statements */>;

class TPCHTest : public BaseTestWithParam<TPCHTestParam> {
 public:
  // Scale factors chosen so the query
  //   -> actually returns result rows (which some don't for small scale factors)
  //   -> doesn't crush a 16GB dev machine
  //   -> runs for a few seconds on a release build
  std::unordered_map<size_t, float> scale_factor_by_query{
      {1, 0.01f},   {2, 0.004f},  {3, 0.01f},  {4, 0.005f},  {5, 0.01f},    {6, 0.01f},  {7, 0.01f},  {8, 0.01f},
      {9, 0.01f},   {10, 0.02f},  {11, 0.01f}, {12, 0.01f},  {13, 0.01f},   {14, 0.01f}, {15, 0.01f}, {16, 0.01f},
      {17, 0.013f}, {18, 0.005f}, {19, 0.01f}, {20, 0.008f}, {21, 0.0075f}, {22, 0.01f}};

  // Helper method used to access TPCHBenchmarkItemRunner's private members
  std::string get_deterministic_query(TPCHBenchmarkItemRunner& runner, BenchmarkItemID item_id) {
    return runner._build_deterministic_query(item_id);
  }
};

TEST_P(TPCHTest, Test) {
  const auto [item_idx, use_jit, use_prepared_statements] = GetParam();  // NOLINT
  const auto tpch_idx = item_idx + 1;

  /**
   * Generate the TPC-H tables with a scale factor appropriate for this query
   */
  const auto scale_factor = scale_factor_by_query.at(tpch_idx);

  TPCHTableGenerator{scale_factor, 10'000}.generate_and_store();

  SCOPED_TRACE("TPC-H " + std::to_string(tpch_idx) + (use_jit ? " with JIT" : " without JIT") + " and " +
               (use_prepared_statements ? " with prepared statements" : " without prepared statements"));

  // The scale factor passed to the query generator will be ignored as we only use deterministic queries
  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  auto benchmark_item_runner = TPCHBenchmarkItemRunner{config, use_prepared_statements, 1.0f};
  benchmark_item_runner.on_tables_loaded();

  const auto query = get_deterministic_query(benchmark_item_runner, item_idx);

  /**
   * Pick a LQPTranslator, depending on whether we use JIT or not
   */
  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    lqp_translator = std::make_shared<JitAwareLQPTranslator>();
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }
  auto sql_pipeline = SQLPipelineBuilder{query}.with_lqp_translator(lqp_translator).create_pipeline();

  /**
   * Run the query and obtain the result tables, TPC-H 15 needs special handling
   */
  // TPC-H 15 needs special patching as it contains a DROP VIEW that doesn't return a table as last statement
  auto result_table_pair = sql_pipeline.get_result_tables();
  Assert(result_table_pair.first == SQLPipelineStatus::Success, "Unexpected pipeline status");
  std::shared_ptr<const Table> result_table;
  if (tpch_idx == 15) {
    Assert(sql_pipeline.statement_count() == 3u, "Expected 3 statements in TPC-H 15");
    result_table = result_table_pair.second[1];
  } else {
    Assert(sql_pipeline.statement_count() == 1u, "Expected single statement");
    result_table = result_table_pair.second[0];
  }

  /**
   * Test the results. These files are previous results of a known-to-be-good (i.e., validated with SQLite) execution
   * of the tests.
   */

  auto expected_table =
      load_table(std::string("resources/test_data/tbl/tpch/test-validation/q") + std::to_string(tpch_idx) + ".tbl");

  EXPECT_TABLE_EQ(result_table, expected_table, OrderSensitivity::No, TypeCmpMode::Lenient,
                  FloatComparisonMode::RelativeDifference);
}

INSTANTIATE_TEST_CASE_P(
    TPCHTestNoJITNoPreparedStatements, TPCHTest,
    // TPCHBenchmarkItemRunner{false, 1.0f} is used only to get the list of all available queries
    testing::Combine(testing::ValuesIn(TPCHBenchmarkItemRunner{
                         std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), false, 1.0f}
                                           .items()),
                     testing::ValuesIn({false}), testing::ValuesIn({false})), );  // NOLINT(whitespace/parens)

INSTANTIATE_TEST_CASE_P(
    TPCHTestNoJITPreparedStatements, TPCHTest,
    testing::Combine(testing::ValuesIn(TPCHBenchmarkItemRunner{
                         std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), false, 1.0f}
                                           .items()),
                     testing::ValuesIn({false}), testing::ValuesIn({true})), );  // NOLINT(whitespace/parens)

#if HYRISE_JIT_SUPPORT

INSTANTIATE_TEST_CASE_P(
    TPCHTestJITPreparedStatements, TPCHTest,
    testing::Combine(testing::ValuesIn(TPCHBenchmarkItemRunner{
                         std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), false, 1.0f}
                                           .items()),
                     testing::ValuesIn({true}), testing::ValuesIn({true})), );  // NOLINT(whitespace/parens)

#endif

}  // namespace opossum
