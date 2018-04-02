#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/constant_calculation_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"

namespace {
std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
  return opossum::SQLPipelineBuilder{query}.disable_mvcc().create_pipeline().get_unoptimized_logical_plans().at(0);
}
}  // namespace

namespace opossum {

class ConstantCalculationRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    _rule = std::make_shared<ConstantCalculationRule>();
  }

  std::shared_ptr<ConstantCalculationRule> _rule;
};

TEST_F(ConstantCalculationRuleTest, ResolveExpressionInPredicateTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1232 + 1 + 1";
  const auto result_node = compile_query(query);

  const auto resolved = StrategyBaseTest::apply_rule(_rule, result_node);

  EXPECT_EQ(resolved->type(), LQPNodeType::Projection);
  EXPECT_EQ(resolved->left_input()->type(), LQPNodeType::Projection);
  EXPECT_FALSE(resolved->right_input());

  ASSERT_EQ(resolved->left_input()->left_input()->type(), LQPNodeType::Predicate);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(resolved->left_input()->left_input());
  EXPECT_FALSE(predicate_node->right_input());
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);

  ASSERT_EQ(predicate_node->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->left_input());
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);

  const auto original_node = projection_node->left_input();

  ASSERT_TRUE(is_variant(predicate_node->value()));
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(original_node, ColumnID{0}));
  EXPECT_EQ(boost::get<AllTypeVariant>(predicate_node->value()), AllTypeVariant{1234});
}

TEST_F(ConstantCalculationRuleTest, ResolveExpressionInProjectionTest) {
  const auto query = "SELECT * FROM (SELECT 1 + 4, 6 AS a, b FROM table_a) AS foo WHERE a > 3";
  const auto result_node = compile_query(query);

  const auto resolved = StrategyBaseTest::apply_rule(_rule, result_node);

  ASSERT_EQ(resolved->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(resolved);
  EXPECT_FALSE(projection_node->right_input());
  EXPECT_EQ(projection_node->column_expressions().size(), 3u);
  EXPECT_EQ(projection_node->column_expressions()[0]->type(), ExpressionType::Literal);
  EXPECT_EQ(projection_node->column_expressions()[1]->type(), ExpressionType::Column);
  EXPECT_EQ(projection_node->column_expressions()[2]->type(), ExpressionType::Column);

  EXPECT_EQ(projection_node->left_input()->type(), LQPNodeType::Predicate);
  ASSERT_EQ(projection_node->left_input()->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node_2 = std::dynamic_pointer_cast<ProjectionNode>(projection_node->left_input()->left_input());
  EXPECT_FALSE(projection_node_2->right_input());
  EXPECT_EQ(projection_node_2->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node_2->column_expressions()[0]->type(), ExpressionType::Literal);
  EXPECT_EQ(projection_node_2->column_expressions()[1]->type(), ExpressionType::Column);

  EXPECT_EQ(projection_node_2->left_input()->type(), LQPNodeType::StoredTable);
}

TEST_F(ConstantCalculationRuleTest, ResolveMultipleExpressionsTest) {
  const auto query =
      "SELECT 100, a FROM (SELECT * FROM table_a WHERE b = 10.0 / 2.5) as table_b WHERE a = 120 + 1 + 1 + 1";
  const auto result_node = compile_query(query);

  const auto resolved = StrategyBaseTest::apply_rule(_rule, result_node);

  EXPECT_EQ(resolved->type(), LQPNodeType::Projection);
  EXPECT_EQ(resolved->left_input()->type(), LQPNodeType::Projection);
  EXPECT_FALSE(resolved->right_input());

  ASSERT_EQ(resolved->left_input()->left_input()->type(), LQPNodeType::Predicate);
  const auto predicate_node_1 = std::dynamic_pointer_cast<PredicateNode>(resolved->left_input()->left_input());
  EXPECT_FALSE(predicate_node_1->right_input());
  EXPECT_EQ(predicate_node_1->predicate_condition(), PredicateCondition::Equals);

  EXPECT_EQ(predicate_node_1->left_input()->type(), LQPNodeType::Projection);
  EXPECT_EQ(predicate_node_1->left_input()->left_input()->type(), LQPNodeType::Projection);
  EXPECT_EQ(predicate_node_1->left_input()->left_input()->left_input()->type(), LQPNodeType::Projection);

  ASSERT_EQ(predicate_node_1->left_input()->left_input()->left_input()->left_input()->type(), LQPNodeType::Predicate);
  const auto predicate_node_2 = std::dynamic_pointer_cast<PredicateNode>(
      predicate_node_1->left_input()->left_input()->left_input()->left_input());
  EXPECT_FALSE(predicate_node_2->right_input());
  EXPECT_EQ(predicate_node_2->predicate_condition(), PredicateCondition::Equals);

  const auto original_node = predicate_node_2->left_input()->left_input();

  EXPECT_EQ(predicate_node_1->column_reference(), LQPColumnReference(original_node, ColumnID{0}));
  ASSERT_TRUE(is_variant(predicate_node_1->value()));
  EXPECT_EQ(boost::get<AllTypeVariant>(predicate_node_1->value()), AllTypeVariant{123});

  EXPECT_EQ(predicate_node_2->column_reference(), LQPColumnReference(original_node, ColumnID{1}));
  ASSERT_TRUE(is_variant(predicate_node_2->value()));
  EXPECT_EQ(boost::get<AllTypeVariant>(predicate_node_2->value()), AllTypeVariant{4.f});
}

}  // namespace opossum
