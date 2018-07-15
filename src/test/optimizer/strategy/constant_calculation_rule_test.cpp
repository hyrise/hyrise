#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/constant_calculation_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
  return opossum::SQLPipelineBuilder{query}.disable_mvcc().create_pipeline().get_unoptimized_logical_plans().at(0);
}
}  // namespace

namespace opossum {

class ConstantCalculationRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    rule = std::make_shared<ConstantCalculationRule>();

    stored_table_node = StoredTableNode::make("table_a");
    a = stored_table_node->get_column("a");
    b = stored_table_node->get_column("b");
  }

  std::shared_ptr<ConstantCalculationRule> rule;
  std::shared_ptr<StoredTableNode> stored_table_node;
  LQPColumnReference a, b;
};

TEST_F(ConstantCalculationRuleTest, ResolveExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1232 + 1 + 1";
  const auto result_node = compile_query(query);

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, result_node);

  /**
   * NOTE
   * The ProjectionNode will still contain a Column calculating 1233+1
   *    * Because it is not the job of the ConstantCalculationRule to remove redundant columns
   *    * It isn't pruned because the Optimizer (TODO(anybody)!) can't rewrite root expressions, because
   *        AbstractLQPNode::node_expressions() returns them by value.
   */

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a, b),
    PredicateNode::make(equals_(a, 1234),
      ProjectionNode::make(expression_vector(add_(1233, 1), a, b),
        stored_table_node)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ConstantCalculationRuleTest, DoesntPruneList) {
  const auto query = "SELECT * FROM table_a WHERE a IN (1, 2, 3+5, 4)";
  const auto result_node = compile_query(query);

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, result_node);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a, b),
    PredicateNode::make(not_equals_(in_(a, list_(1, 2, 8, 4)), 0),
      ProjectionNode::make(expression_vector(in_(a, list_(1, 2, 8, 4)), a, b),
        stored_table_node)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
