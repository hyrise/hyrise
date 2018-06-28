#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/abstract_operator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "sql/sqlite_testrunner/sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"

#include "tpch/tpch_queries.hpp"

namespace opossum {

using TestConfiguration = std::pair<const char*, bool>;

class TPCHTest : public BaseTestWithParam<std::pair<const size_t, TestConfiguration>> {
 public:
  static std::multimap<size_t, TestConfiguration> build_combinations() {
    std::multimap<size_t, TestConfiguration> combinations;
    for (auto it = tpch_queries.cbegin(); it != tpch_queries.cend(); ++it) {
      combinations.emplace(it->first, TestConfiguration{it->second, false});
      if constexpr (HYRISE_JIT_SUPPORT) {
        combinations.emplace(it->first, TestConfiguration{it->second, true});
      }
    }
    return combinations;
  }

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
  const auto [query_idx, test_configuration] = GetParam();  // NOLINT
  const auto [query, use_jit] = test_configuration;         // NOLINT

  SCOPED_TRACE("TPC-H " + std::to_string(query_idx) + " " + (use_jit ? "with JIT" : "without JIT"));

  const auto sqlite_result_table = _sqlite_wrapper->execute_query(query);

  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    if constexpr (HYRISE_JIT_SUPPORT) {
      lqp_translator = std::make_shared<JitAwareLQPTranslator>();
    }
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }

  auto sql_pipeline = SQLPipelineBuilder{query}.with_lqp_translator(lqp_translator).disable_mvcc().create_pipeline();
  const auto& result_table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ(result_table, sqlite_result_table, OrderSensitivity::No, TypeCmpMode::Lenient,
                  FloatComparisonMode::RelativeDifference);
}

INSTANTIATE_TEST_CASE_P(TPCHTestInstances, TPCHTest, ::testing::ValuesIn(TPCHTest::build_combinations()), );  // NOLINT

}  // namespace opossum
