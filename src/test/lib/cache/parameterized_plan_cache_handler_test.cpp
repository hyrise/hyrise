#include "base_test.hpp"

#include "cache/parameterized_plan_cache_handler.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "types.hpp"

namespace opossum {

class ParameterizedPlanCacheHandlerTest : public BaseTest {
 protected:
  static void SetUpTestCase() {  // called ONCE before the tests
  }

  void SetUp() override {
    _table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    _table_b = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_b", _table_b);

    _lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  }

  const std::vector<std::shared_ptr<SQLPipelineStatement>>& get_sql_pipeline_statements(SQLPipeline& sql_pipeline) {
    return sql_pipeline._get_sql_pipeline_statements();
  }

  const std::shared_ptr<AbstractLQPNode> get_unoptimized_logical_plan(std::string query, UseMvcc use_mvcc) {
    auto sql_pipeline = (use_mvcc == UseMvcc::Yes)
                            ? SQLPipelineBuilder{query}.with_lqp_cache(_lqp_cache).create_pipeline()
                            : SQLPipelineBuilder{query}.with_lqp_cache(_lqp_cache).disable_mvcc().create_pipeline();

    auto& statement = get_sql_pipeline_statements(sql_pipeline).at(0);

    auto unoptimized_lqp = statement->get_unoptimized_logical_plan();
    statement->_unoptimized_logical_plan = nullptr;

    return unoptimized_lqp;
  }

  AllTypeVariant value_from_value_expression(const std::shared_ptr<AbstractExpression>& abstract_expression) {
    const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(abstract_expression);
    DebugAssert(value_expression, "Wrong expression type provided.");
    return value_expression->value;
  }

  const std::vector<std::shared_ptr<AbstractExpression>>& extracted_values(
      ParameterizedPlanCacheHandler& cache_handler) {
    return cache_handler._extracted_values;
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<SQLLogicalPlanCache> _lqp_cache;
  const std::string _parameter_query_a = "SELECT * FROM table_a WHERE table_a.a > 1000 AND table_a.b < 458";
  const std::string _parameter_query_b = "SELECT table_a.a + 8.2 FROM table_a WHERE table_a.a > 23";
  const std::string _parameter_query_c = "SELECT table_a.a + 3, (SELECT table_a.b * 2 FROM table_a) AS b FROM table_a";
  const std::string _non_cacheble_qery_a = "SELECT table_a.a FROM table_a WHERE table_a.a > 5 AND table_a.a < 10";
  const std::string _non_cacheble_qery_b = "SELECT table_a.a FROM table_a WHERE table_a.a = 5 AND table_a.a = 6";
};

class ParameterizedPlanCacheHandlerMvccTest : public ParameterizedPlanCacheHandlerTest,
                                              public ::testing::WithParamInterface<UseMvcc> {};

INSTANTIATE_TEST_SUITE_P(MvccYesNo, ParameterizedPlanCacheHandlerMvccTest,
                         ::testing::Values(UseMvcc::Yes, UseMvcc::No));

TEST_P(ParameterizedPlanCacheHandlerMvccTest, ValueExtractionFromWhere) {
  const auto use_mvcc = GetParam();
  auto unoptimized_lqp = get_unoptimized_logical_plan(_parameter_query_a, use_mvcc);

  auto cache_duration = std::chrono::nanoseconds(0);
  auto cache_handler = ParameterizedPlanCacheHandler(_lqp_cache, unoptimized_lqp, cache_duration, use_mvcc);

  const auto cached_plan_optional = cache_handler.try_get();

  // Expect two extracted values from plan
  EXPECT_EQ(extracted_values(cache_handler).size(), 2);
  // Check if right values are extracted
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(0)), AllTypeVariant{1000});
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(1)), AllTypeVariant{458});
}

TEST_P(ParameterizedPlanCacheHandlerMvccTest, ValueExtractionFromSelect) {
  const auto use_mvcc = GetParam();
  auto unoptimized_lqp = get_unoptimized_logical_plan(_parameter_query_b, use_mvcc);

  auto cache_duration = std::chrono::nanoseconds(0);
  auto cache_handler = ParameterizedPlanCacheHandler(_lqp_cache, unoptimized_lqp, cache_duration, use_mvcc);

  const auto cached_plan_optional = cache_handler.try_get();

  // Expect two extracted values from plan
  EXPECT_EQ(extracted_values(cache_handler).size(), 2);
  // Check if right values are extracted
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(0)), AllTypeVariant{8.2});
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(1)), AllTypeVariant{23});
}

TEST_P(ParameterizedPlanCacheHandlerMvccTest, ValueExtractionFromSubquery) {
  const auto use_mvcc = GetParam();
  auto unoptimized_lqp = get_unoptimized_logical_plan(_parameter_query_c, use_mvcc);

  auto cache_duration = std::chrono::nanoseconds(0);
  auto cache_handler = ParameterizedPlanCacheHandler(_lqp_cache, unoptimized_lqp, cache_duration, use_mvcc);

  const auto cached_plan_optional = cache_handler.try_get();

  // Expect two distinct extracted values from plan
  EXPECT_EQ(extracted_values(cache_handler).size(), 4);
  // Check if right values are extracted
  // Values appear double here, due to alias (AS) nodes
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(0)), AllTypeVariant{3});
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(1)), AllTypeVariant{3});
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(2)), AllTypeVariant{2});
  EXPECT_EQ(value_from_value_expression(extracted_values(cache_handler).at(3)), AllTypeVariant{2});
}

TEST_P(ParameterizedPlanCacheHandlerMvccTest, GetCachedOptimizedLQP) {
  const auto use_mvcc = GetParam();

  auto unoptimized_lqp = get_unoptimized_logical_plan(_parameter_query_a, use_mvcc);

  auto cache_duration = std::chrono::nanoseconds(0);
  auto cache_handler = ParameterizedPlanCacheHandler(_lqp_cache, unoptimized_lqp, cache_duration, use_mvcc);

  auto cached_plan_optional = cache_handler.try_get();

  // Expect cache miss
  EXPECT_FALSE(cached_plan_optional);

  auto optimizer = Optimizer::create_default_pre_caching_optimizer();
  auto optimizer_rule_durations = std::make_shared<std::vector<OptimizerRuleMetrics>>();
  auto cacheable_plan = std::make_shared<bool>(true);
  auto optimized_lqp = optimizer->optimize(std::move(unoptimized_lqp), optimizer_rule_durations, cacheable_plan);

  EXPECT_TRUE(*cacheable_plan);
  EXPECT_TRUE((use_mvcc == UseMvcc::Yes) == lqp_is_validated(optimized_lqp));

  cache_handler.set(optimized_lqp);

  cached_plan_optional = cache_handler.try_get();

  // Expect cache to contain validated LQP
  EXPECT_TRUE(cached_plan_optional);

  // Expect cached plan to have same validation status as when written to cache
  EXPECT_TRUE((use_mvcc == UseMvcc::Yes) == lqp_is_validated(*cached_plan_optional));

  // Expect cache retrieved value to be equal to set value
  EXPECT_LQP_EQ(*cached_plan_optional, optimized_lqp);
}

TEST_P(ParameterizedPlanCacheHandlerMvccTest, DontEvictSamePlanWithDifferentMvcc) {
  const auto use_mvcc = GetParam();

  auto unoptimized_lqp = get_unoptimized_logical_plan(_parameter_query_a, use_mvcc);

  auto cache_duration = std::chrono::nanoseconds(0);
  auto cache_handler = ParameterizedPlanCacheHandler(_lqp_cache, unoptimized_lqp, cache_duration, use_mvcc);

  auto optimizer = Optimizer::create_default_pre_caching_optimizer();
  auto optimizer_rule_durations = std::make_shared<std::vector<OptimizerRuleMetrics>>();
  auto cacheable_plan = std::make_shared<bool>(true);
  auto optimized_lqp = optimizer->optimize(std::move(unoptimized_lqp), optimizer_rule_durations, cacheable_plan);

  cache_handler.set(optimized_lqp);

  auto cached_plan_optional = cache_handler.try_get();

  // Expect cache to contain validated/not validated LQP according to use_mvcc
  EXPECT_TRUE(cached_plan_optional);
  EXPECT_TRUE((use_mvcc == UseMvcc::Yes) == lqp_is_validated(*cached_plan_optional));

  // Generate plan with different MVCC setting
  const auto use_mvcc_2 = (use_mvcc == UseMvcc::Yes) ? UseMvcc::No : UseMvcc::Yes;
  DebugAssert(use_mvcc != use_mvcc_2, "Plans should have different MVCC state.");

  auto unoptimized_lqp_2 = get_unoptimized_logical_plan(_parameter_query_a, use_mvcc_2);

  auto cache_handler_2 = ParameterizedPlanCacheHandler(_lqp_cache, unoptimized_lqp_2, cache_duration, use_mvcc_2);

  auto optimized_lqp_2 = optimizer->optimize(std::move(unoptimized_lqp_2), optimizer_rule_durations, cacheable_plan);

  cache_handler_2.set(optimized_lqp_2);

  const auto cached_plan_optional_2 = cache_handler_2.try_get();

  // Expect cache to contain LQP with different validation status than before
  EXPECT_TRUE(cached_plan_optional_2);
  EXPECT_TRUE((use_mvcc_2 == UseMvcc::Yes) == lqp_is_validated(*cached_plan_optional_2));

  cached_plan_optional = cache_handler.try_get();

  // Expect previous LQP to be still cached
  EXPECT_TRUE(cached_plan_optional);
  EXPECT_TRUE((use_mvcc == UseMvcc::Yes) == lqp_is_validated(*cached_plan_optional));
}

}  // namespace opossum
