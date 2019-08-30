#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/in_expression_rewrite_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class InExpressionRewriteRuleTest : public StrategyBaseTest {
  void SetUp() override {
    node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "col"}});
    col = lqp_column_(node->get_column("col"));

    single_element_in_expression = in_(col, list_(1));
    five_element_in_expression = in_(col, list_(1, 2, 3, 4, 5));
    five_element_not_in_expression = not_in_(col, list_(1, 2, 3, 4, 5));
    duplicate_element_in_expression = in_(col, list_(1, 2, 1));
    different_types_in_expression = in_(col, list_(1, 2.0f));
    null_in_expression = in_(col, list_(1, NULL_VALUE));

    auto hundred_elements = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (auto i = 0; i < 100; ++i) hundred_elements.emplace_back(value_(i));
    hundred_element_in_expression = std::make_shared<InExpression>(PredicateCondition::In, col, std::make_shared<ListExpression>(hundred_elements));
  }

 public:
  std::shared_ptr<MockNode> node;
  std::shared_ptr<AbstractExpression> col, single_element_in_expression, five_element_in_expression, five_element_not_in_expression, hundred_element_in_expression, duplicate_element_in_expression, different_types_in_expression, null_in_expression;
};

TEST_F(InExpressionRewriteRuleTest, ExpressionEvaluatorStrategy) {
  // With the ExpressionEvaluator strategy chosen, no modifications should be made
  auto rule = std::make_shared<InExpressionRewriteRule>();
  rule->strategy = InExpressionRewriteRule::Strategy::ExpressionEvaluator;

  {
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_not_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(hundred_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);
  }
}

TEST_F(InExpressionRewriteRuleTest, DisjunctionStrategy) {
  // Note that the order of predicates is technically undefined as we use an ExpressionUnorderedSet.
  // However, the actual order seems stable enough for a test.
  auto rule = std::make_shared<InExpressionRewriteRule>();
  rule->strategy = InExpressionRewriteRule::Strategy::Disjunction;

  {
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    const auto expected_lqp = PredicateNode::make(equals_(col, 1), node);
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    // clang-format off
    const auto expected_lqp =
      UnionNode::make(UnionMode::All,
        UnionNode::make(UnionMode::All,
          UnionNode::make(UnionMode::All,
            UnionNode::make(UnionMode::All,
              PredicateNode::make(equals_(col, 5), node),
              PredicateNode::make(equals_(col, 4), node)),
            PredicateNode::make(equals_(col, 3), node)),
          PredicateNode::make(equals_(col, 2), node)),
        PredicateNode::make(equals_(col, 1), node));
    // clang-format on

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_not_in_expression, node);
    EXPECT_THROW(StrategyBaseTest::apply_rule(rule, input_lqp), std::logic_error);
  }

  {
    const auto input_lqp = PredicateNode::make(duplicate_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    // clang-format off
    const auto expected_lqp =
      UnionNode::make(UnionMode::All,
        PredicateNode::make(equals_(col, 2), node),
        PredicateNode::make(equals_(col, 1), node));
    // clang-format on
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(different_types_in_expression, node);
    EXPECT_THROW(StrategyBaseTest::apply_rule(rule, input_lqp), std::logic_error);
  }

  {
    // Generally, we could get the rule to throw out predicates that compare to NULL, but since those queries are
    // stupid anyway, we don't write the additional code. Test that it does not break:
    const auto input_lqp = PredicateNode::make(null_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    // clang-format off
    const auto expected_lqp =
      UnionNode::make(UnionMode::All,
        PredicateNode::make(equals_(col, NULL_VALUE), node),
        PredicateNode::make(equals_(col, 1), node));
    // clang-format on
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }
}

TEST_F(InExpressionRewriteRuleTest, JoinStrategy) {
  auto rule = std::make_shared<InExpressionRewriteRule>();
  rule->strategy = InExpressionRewriteRule::Strategy::Join;

  {
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    const auto column_definitions = TableColumnDefinitions{{"right_values", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->append({1});
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_({static_table_node, ColumnID{0}});
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    const auto column_definitions = TableColumnDefinitions{{"right_values", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->append({1});
    table->append({2});
    table->append({3});
    table->append({4});
    table->append({5});
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_({static_table_node, ColumnID{0}});
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_not_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    const auto column_definitions = TableColumnDefinitions{{"right_values", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->append({1});
    table->append({2});
    table->append({3});
    table->append({4});
    table->append({5});
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_({static_table_node, ColumnID{0}});
    const auto expected_lqp = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(col, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }

  // We do not test duplicate_element_in_expression, as the correctness of the join strategy does not depend on
  // duplicate elimination. We don't see any potential in eliminating duplicates, as we have not seen any, yet.

  {
    const auto input_lqp = PredicateNode::make(different_types_in_expression, node);
    EXPECT_THROW(StrategyBaseTest::apply_rule(rule, input_lqp), std::logic_error);
  }

  {
    const auto input_lqp = PredicateNode::make(null_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    const auto column_definitions = TableColumnDefinitions{{"right_values", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->append({1});
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_({static_table_node, ColumnID{0}});
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }
}

TEST_F(InExpressionRewriteRuleTest, AutoStrategy) {
  auto rule = std::make_shared<InExpressionRewriteRule>();
  rule->strategy = InExpressionRewriteRule::Strategy::Auto;

  {
    // Disjunction for single element
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    const auto expected_lqp = PredicateNode::make(equals_(col, 1), node);
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    // ExpressionEvaluator for five elements
    const auto input_lqp = PredicateNode::make(five_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);
  }

  {
    // ExpressionEvaluator for differing types
    const auto input_lqp = PredicateNode::make(different_types_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);
  }

  {
    // Join for 100 elements
    const auto input_lqp = PredicateNode::make(hundred_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    const auto column_definitions = TableColumnDefinitions{{"right_values", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    for (auto i = 0; i < 100; ++i) table->append({i});
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_({static_table_node, ColumnID{0}});
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }

  {
    // Disjunction for two elements, even if one is NULL
    const auto input_lqp = PredicateNode::make(null_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    // clang-format off
    const auto expected_lqp =
      UnionNode::make(UnionMode::All,
        PredicateNode::make(equals_(col, NULL_VALUE), node),
        PredicateNode::make(equals_(col, 1), node));
    // clang-format on
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }
}

}
