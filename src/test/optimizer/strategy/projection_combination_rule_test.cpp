#include <memory>
#include <string>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/strategy/projection_combination_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "sql/sql_pipeline.hpp"
#include "storage/storage_manager.hpp"

namespace {
std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
  return opossum::SQLPipeline{query, opossum::UseMvcc::No}.get_unoptimized_logical_plans().at(0);
}
}  // namespace

namespace opossum {

class ProjectionCombinationRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));
    _rule = std::make_shared<ProjectionCombinationRule>();
  }

  std::shared_ptr<ProjectionCombinationRule> _rule;
};

TEST_F(ProjectionCombinationRuleTest, CombineProjections) {
  const auto query =
      R"(SELECT 100, foo.a
        FROM (
          SELECT *
          FROM table_a
          JOIN table_b
          ON table_a.a = table_b.a
        ) as foo;)";
  const auto result_node = compile_query(query);

  const auto combined = StrategyBaseTest::apply_rule(_rule, result_node);

  ASSERT_EQ(combined->type(), LQPNodeType::Projection);
  EXPECT_EQ(combined->left_input()->type(), LQPNodeType::Join);
  EXPECT_FALSE(combined->right_input());

  const auto projection_node = std::static_pointer_cast<ProjectionNode>(combined);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
}

TEST_F(ProjectionCombinationRuleTest, WithReorderingsAndAliases) {
  const auto query =
      R"(SELECT "20", d AS e
        FROM (
          SELECT 100, c AS d, 20, a
          FROM (
            SELECT b AS c, a
            FROM table_a
            WHERE a > 5
          ) AS foo
        ) as bar;)";
  const auto result_node = compile_query(query);

  const auto combined = StrategyBaseTest::apply_rule(_rule, result_node);

  ASSERT_EQ(combined->type(), LQPNodeType::Projection);
  EXPECT_EQ(combined->left_input()->type(), LQPNodeType::Predicate);

  const auto projection_node = std::static_pointer_cast<ProjectionNode>(combined);
  ASSERT_EQ(projection_node->column_expressions().size(), 2u);
  ASSERT_EQ(projection_node->column_expressions()[0]->type(), ExpressionType::Literal);
  ASSERT_EQ(projection_node->column_expressions()[1]->type(), ExpressionType::Column);

  EXPECT_EQ(projection_node->column_expressions()[0]->value(), AllTypeVariant{20});

  const auto original_node = projection_node->left_input()->left_input();
  EXPECT_EQ(projection_node->column_expressions()[1]->column_reference(),
            LQPColumnReference(original_node, ColumnID{1}));
}

TEST_F(ProjectionCombinationRuleTest, WithAggregate) {
  const auto query =
      R"(SELECT *
        FROM (
          SELECT SUM(a + a)
          FROM (
            SELECT *
            FROM (
              SELECT 100, a
              FROM table_a
            ) as foo
          ) as bar
        ) as baz;)";
  const auto result_node = compile_query(query);

  const auto combined = StrategyBaseTest::apply_rule(_rule, result_node);

  ASSERT_EQ(combined->type(), LQPNodeType::Projection);
  const auto projection_node_1 = std::static_pointer_cast<ProjectionNode>(combined);
  ASSERT_EQ(projection_node_1->column_expressions().size(), 1u);
  EXPECT_EQ(projection_node_1->column_expressions()[0]->type(), ExpressionType::Column);

  ASSERT_EQ(projection_node_1->left_input()->type(), LQPNodeType::Aggregate);
  const auto aggregate_node = std::static_pointer_cast<AggregateNode>(projection_node_1->left_input());
  EXPECT_EQ(projection_node_1->column_expressions()[0]->column_reference(),
            LQPColumnReference(aggregate_node, ColumnID{0}));

  ASSERT_EQ(aggregate_node->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node_2 = std::static_pointer_cast<ProjectionNode>(aggregate_node->left_input());
  ASSERT_EQ(projection_node_2->column_expressions().size(), 2u);
  ASSERT_EQ(projection_node_2->column_expressions()[0]->type(), ExpressionType::Literal);
  ASSERT_EQ(projection_node_2->column_expressions()[1]->type(), ExpressionType::Column);

  EXPECT_EQ(projection_node_2->column_expressions()[0]->value(), AllTypeVariant{100});

  const auto original_node = projection_node_2->left_input();
  EXPECT_EQ(projection_node_2->column_expressions()[1]->column_reference(),
            LQPColumnReference(original_node, ColumnID{0}));
}

TEST_F(ProjectionCombinationRuleTest, WithJoin) {
  const auto query =
      R"(SELECT *
        FROM (
          SELECT * FROM table_a WHERE b = 10.0 / 2.5
        ) AS foo
        JOIN (
          SELECT 20, a
          FROM (
            SELECT * FROM table_a WHERE b = 10.0 / 2.5
          ) AS baz
          WHERE a = 123
        ) AS bar
        ON foo.a = bar.a;)";
  const auto result_node = compile_query(query);

  const auto combined = StrategyBaseTest::apply_rule(_rule, result_node);

  EXPECT_EQ(combined->type(), LQPNodeType::Projection);
  EXPECT_EQ(combined->left_input()->type(), LQPNodeType::Join);

  ASSERT_EQ(combined->left_input()->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node_1 = std::static_pointer_cast<ProjectionNode>(combined->left_input()->left_input());
  ASSERT_EQ(projection_node_1->column_expressions().size(), 2u);
  ASSERT_EQ(projection_node_1->column_expressions()[0]->type(), ExpressionType::Column);
  ASSERT_EQ(projection_node_1->column_expressions()[1]->type(), ExpressionType::Column);

  const auto original_node_1 = projection_node_1->left_input()->left_input()->left_input();
  EXPECT_EQ(projection_node_1->column_expressions()[0]->column_reference(),
            LQPColumnReference(original_node_1, ColumnID{0}));
  EXPECT_EQ(projection_node_1->column_expressions()[1]->column_reference(),
            LQPColumnReference(original_node_1, ColumnID{1}));

  ASSERT_EQ(combined->left_input()->right_input()->left_input()->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node_2 =
      std::static_pointer_cast<ProjectionNode>(combined->left_input()->right_input()->left_input()->left_input());
  ASSERT_EQ(projection_node_2->column_expressions().size(), 2u);
  ASSERT_EQ(projection_node_2->column_expressions()[0]->type(), ExpressionType::Column);
  ASSERT_EQ(projection_node_2->column_expressions()[1]->type(), ExpressionType::Column);

  const auto original_node_2 = projection_node_2->left_input()->left_input()->left_input();
  EXPECT_EQ(projection_node_2->column_expressions()[0]->column_reference(),
            LQPColumnReference(original_node_2, ColumnID{0}));
  EXPECT_EQ(projection_node_2->column_expressions()[1]->column_reference(),
            LQPColumnReference(original_node_2, ColumnID{1}));
}

}  // namespace opossum
