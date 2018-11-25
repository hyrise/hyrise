#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/limit.hpp"
#include "operators/table_wrapper.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_query_generator.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

using TestConfiguration = std::pair<QueryID, bool>;  // query_idx, use_jit

class TPCHTest : public BaseTestWithParam<TestConfiguration> {
 public:
  void SetUp() override {
    const auto scale_factor = 1.f;

    // Cache the generated tables so that we don't have to regenerate them
    static std::unordered_map<TpchTable, std::shared_ptr<Table>> generated_tables;

    if (generated_tables.empty()) {
      generated_tables = TpchDbGenerator{scale_factor, 10'000}.generate();
    }

    for (const auto& [table_enum, table_name] : tpch_table_names) {
      StorageManager::get().add_table(table_name, generated_tables[table_enum]);
    }

    const auto preparation_queries = TPCHQueryGenerator{}.get_preparation_queries();
    SQLPipelineBuilder{preparation_queries}.disable_mvcc().create_pipeline().get_result_table();

    SQLLogicalPlanCache::get().clear();
    SQLPhysicalPlanCache::get().clear();
  }

  static std::vector<TestConfiguration> build_combinations() {
    std::vector<TestConfiguration> combinations;
    const auto selected_queries = TPCHQueryGenerator{}.selected_queries();
    for (const auto& query_id : selected_queries) {
      combinations.emplace_back(query_id, false);
      if constexpr (HYRISE_JIT_SUPPORT) {
        combinations.emplace_back(query_id, true);
      }
    }
    return combinations;
  }
};

TEST_P(TPCHTest, TPCHQueryTest) {
  const auto [query_idx, use_jit] = GetParam();  // NOLINT
  const auto tpch_idx = query_idx + 1;
  const auto query = TPCHQueryGenerator{}.build_query(query_idx);

  SCOPED_TRACE("TPC-H " + std::to_string(tpch_idx) + (use_jit ? " with JIT" : " without JIT"));

  std::shared_ptr<const Table> result;

  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    // TPCH query 13 can currently not be run with Jit Operators because of wrong output column definitions for outer
    // Joins. See: Issue #1051 (https://github.com/hyrise/hyrise/issues/1051)
    if (tpch_idx == 13) {
      std::cerr << "Test of TPCH query 13 with JIT is currently disabled (Issue #1051)" << std::endl;
      return;
    }
    lqp_translator = std::make_shared<JitAwareLQPTranslator>();
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }
  auto sql_pipeline = SQLPipelineBuilder{query}.with_lqp_translator(lqp_translator).disable_mvcc().create_pipeline();

  // TPC-H 15 needs special patching as it contains a DROP VIEW that doesn't return a table as last statement
  if (tpch_idx == 15) {
    Assert(sql_pipeline.statement_count() == 3u, "Expected 3 statements in TPC-H 15") sql_pipeline.get_result_table();

    result = sql_pipeline.get_result_tables()[1];
  } else {
    result = sql_pipeline.get_result_table();
  }

  // EXPECT_TABLE_EQ crashes if one table is a nullptr
  ASSERT_TRUE(result);

  // The TPC-H validation tables only give a single row. Luckily, they are all ordered.
  auto wrapper = std::make_shared<TableWrapper>(result);
  auto limit = std::make_shared<Limit>(wrapper, to_expression(int64_t{1}));
  wrapper->execute();
  limit->execute();
  result = limit->get_output();

  const auto expected =
      load_table(std::string{"./src/test/tables/tpch/sf-1-validation/q"} + std::to_string(tpch_idx) + ".tbl");
  ASSERT_TRUE(check_table_equal(result, expected, OrderSensitivity::Yes, TypeCmpMode::Strict, FloatComparisonMode::RelativeDifference));
}

INSTANTIATE_TEST_CASE_P(TPCHTestInstances, TPCHTest, ::testing::ValuesIn(TPCHTest::build_combinations()), );  // NOLINT

}  // namespace opossum
