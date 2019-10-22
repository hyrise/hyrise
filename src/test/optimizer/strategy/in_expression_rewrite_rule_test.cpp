#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/in_expression_rewrite_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class InExpressionRewriteRuleTest : public StrategyBaseTest {
  void SetUp() override {
    node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "col_a"}, {DataType::Float, "col_b"}});
    col_a = lqp_column_(node->get_column("col_a"));
    col_b = lqp_column_(node->get_column("col_b"));

    single_element_in_expression = in_(col_a, list_(1));
    five_element_in_expression = in_(col_a, list_(1, 2, 3, 4, 5));
    five_element_not_in_expression = not_in_(col_a, list_(1, 2, 3, 4, 5));
    duplicate_element_in_expression = in_(col_a, list_(1, 2, 1));
    different_types_on_left_and_right_side_expression = in_(col_b, list_(1, 2));
    different_types_on_right_side_expression = in_(col_a, list_(1, 2.0f));
    null_in_expression = in_(col_a, list_(1, NULL_VALUE));

    auto hundred_elements = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (auto i = 0; i < 100; ++i) hundred_elements.emplace_back(value_(i));
    hundred_element_in_expression = std::make_shared<InExpression>(PredicateCondition::In, col_a,
                                                                   std::make_shared<ListExpression>(hundred_elements));
  }

 public:
  std::shared_ptr<MockNode> node;
  std::shared_ptr<AbstractExpression> col_a, col_b, single_element_in_expression, five_element_in_expression,
      five_element_not_in_expression, hundred_element_in_expression, duplicate_element_in_expression,
      different_types_on_left_and_right_side_expression, different_types_on_right_side_expression, null_in_expression;
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
  auto rule = std::make_shared<InExpressionRewriteRule>();
  rule->strategy = InExpressionRewriteRule::Strategy::Disjunction;

  {
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    const auto expected_lqp = PredicateNode::make(equals_(col_a, 1), node);
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(five_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    // Can't use EXPECT_LQP_EQ here, because ExpressionUnorderedSet produces a non-deterministic order of predicates
    auto values_found_in_predicates = std::vector<int>{};

    // Checks that a given node is a predicate of the form `col_a = x` where x is an int and will be added to
    // values_found_in_predicates
    const auto verify_predicate_node = [&](const auto& node) {
      ASSERT_EQ(node->type, LQPNodeType::Predicate);
      auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      ASSERT_TRUE(predicate_node);
      auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
      ASSERT_TRUE(predicate);
      EXPECT_EQ(predicate->left_operand(), col_a);
      ASSERT_EQ(predicate->right_operand()->type, ExpressionType::Value);
      values_found_in_predicates.emplace_back(
          boost::get<int>(dynamic_cast<ValueExpression&>(*predicate->right_operand()).value));
    };

    auto current_node = result_lqp;
    for (auto union_node_idx = 0; union_node_idx < 4; ++union_node_idx) {
      ASSERT_EQ(current_node->type, LQPNodeType::Union);
      auto union_node = std::dynamic_pointer_cast<UnionNode>(current_node);
      ASSERT_TRUE(union_node);
      EXPECT_EQ(union_node->union_mode, UnionMode::All);

      verify_predicate_node(union_node->right_input());

      current_node = union_node->left_input();
    }
    // After checking four union nodes, the last node has predicates on both sides
    verify_predicate_node(current_node);

    std::sort(values_found_in_predicates.begin(), values_found_in_predicates.end());
    const auto expected_values = std::vector<int>{1, 2, 3, 4, 5};
    EXPECT_EQ(values_found_in_predicates, expected_values);
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
        PredicateNode::make(equals_(col_a, 2), node),
        PredicateNode::make(equals_(col_a, 1), node));
    // clang-format on
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    const auto input_lqp = PredicateNode::make(different_types_on_left_and_right_side_expression, node);
    EXPECT_THROW(StrategyBaseTest::apply_rule(rule, input_lqp), std::logic_error);
  }

  {
    const auto input_lqp = PredicateNode::make(different_types_on_right_side_expression, node);
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
        PredicateNode::make(equals_(col_a, NULL_VALUE), node),
        PredicateNode::make(equals_(col_a, 1), node));
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
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col_a, right_col), node, static_table_node);

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
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col_a, right_col), node, static_table_node);

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
    const auto expected_lqp =
        JoinNode::make(JoinMode::AntiNullAsTrue, equals_(col_a, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }

  // We do not test duplicate_element_in_expression, as the correctness of the join strategy does not depend on
  // duplicate elimination. We don't see any potential in eliminating duplicates, as we have not seen any, yet.

  {
    const auto input_lqp = PredicateNode::make(different_types_on_left_and_right_side_expression, node);
    EXPECT_THROW(StrategyBaseTest::apply_rule(rule, input_lqp), std::logic_error);
  }

  {
    const auto input_lqp = PredicateNode::make(different_types_on_right_side_expression, node);
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
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col_a, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }
}

TEST_F(InExpressionRewriteRuleTest, AutoStrategy) {
  auto rule = std::make_shared<InExpressionRewriteRule>();

  {
    // Disjunction for single element
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    const auto expected_lqp = PredicateNode::make(equals_(col_a, 1), node);
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
    const auto input_lqp = PredicateNode::make(different_types_on_right_side_expression, node);
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
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col_a, right_col), node, static_table_node);

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
        PredicateNode::make(equals_(col_a, NULL_VALUE), node),
        PredicateNode::make(equals_(col_a, 1), node));
    // clang-format on
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }
}

}  // namespace opossum
