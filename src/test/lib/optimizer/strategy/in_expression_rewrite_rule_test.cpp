#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/in_expression_rewrite_rule.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "storage/table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class InExpressionRewriteRuleTest : public StrategyBaseTest {
  void SetUp() override {
    // col_a has 1000 entries across 200 values linearly distributed between 1 and 200
    node = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "col_a"}, {DataType::Float, "col_b"}, {DataType::String, "col_c"}},
        1000,
        {{GenericHistogram<int32_t>::with_single_bin(1, 200, 1000, 200),
          GenericHistogram<float>::with_single_bin(1.0f, 50.0f, 100, 10),
          GenericHistogram<pmr_string>::with_single_bin("a", "z", 1, 1000)}});
    col_a = node->get_column("col_a");
    col_b = node->get_column("col_b");
    col_c = node->get_column("col_c");

    many_row_node =
        create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "col_large"}}, 10'000'000,
                                         {GenericHistogram<int32_t>::with_single_bin(1, 10'000'000, 1, 10'000'000)});
    col_large = many_row_node->get_column("col_large");

    single_element_in_expression = in_(col_a, list_(1));
    two_element_functional_in_expression = in_(substr_(col_c, 1, 5), list_("85669", "86197"));
    five_element_in_expression = in_(col_a, list_(1, 2, 3, 4, 5));
    five_element_not_in_expression = not_in_(col_a, list_(1, 2, 3, 4, 5));
    duplicate_element_in_expression = in_(col_a, list_(1, 2, 1));
    different_types_on_left_and_right_side_expression = in_(col_b, list_(1, 2));
    different_types_on_right_side_expression = in_(col_a, list_(1, 2.0f));
    null_in_expression = in_(col_a, list_(1, NULL_VALUE));

    auto hundred_elements = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (auto index = 0; index < 100; ++index) {
      hundred_elements.emplace_back(value_(index));
    }
    hundred_element_in_expression = std::make_shared<InExpression>(PredicateCondition::In, col_a,
                                                                   std::make_shared<ListExpression>(hundred_elements));
    hundred_element_in_expression_large_input = std::make_shared<InExpression>(
        PredicateCondition::In, col_large, std::make_shared<ListExpression>(hundred_elements));
  }

 public:
  // Can't use EXPECT_LQP_EQ for disjunction rewrites for multiple elements, because ExpressionUnorderedSet produces
  // a non-deterministic order of predicates
  bool check_disjunction(std::shared_ptr<AbstractLQPNode> result_lqp, std::vector<int> expected_values) {
    auto values_found_in_predicates = std::vector<int>{};

    // Checks that a given node is a predicate of the form `col_a = x` where x is an int and will be added to
    // values_found_in_predicates
    const auto verify_predicate_node = [&](const auto& node) {
      EXPECT_EQ(node->type, LQPNodeType::Predicate);
      auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      EXPECT_TRUE(predicate_node);
      auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
      EXPECT_TRUE(predicate);
      EXPECT_EQ(predicate->left_operand(), col_a);
      EXPECT_EQ(predicate->right_operand()->type, ExpressionType::Value);
      values_found_in_predicates.emplace_back(
          boost::get<int>(dynamic_cast<ValueExpression&>(*predicate->right_operand()).value));
    };

    for (auto union_node_idx = size_t{0}; union_node_idx < expected_values.size() - 1; ++union_node_idx) {
      EXPECT_EQ(result_lqp->type, LQPNodeType::Union);
      auto union_node = std::dynamic_pointer_cast<UnionNode>(result_lqp);
      EXPECT_TRUE(union_node);
      EXPECT_EQ(union_node->set_operation_mode, SetOperationMode::All);

      verify_predicate_node(union_node->right_input());

      result_lqp = union_node->left_input();
    }
    // After checking expected_values.size() - 1 union nodes, the last node has predicates on both sides
    verify_predicate_node(result_lqp);

    std::sort(values_found_in_predicates.begin(), values_found_in_predicates.end());

    return values_found_in_predicates == expected_values;
  }

  std::shared_ptr<MockNode> node, many_row_node;
  std::shared_ptr<AbstractExpression> col_a, col_b, col_c, col_large, single_element_in_expression,
      two_element_functional_in_expression, five_element_in_expression, five_element_not_in_expression,
      hundred_element_in_expression, hundred_element_in_expression_large_input, duplicate_element_in_expression,
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

    EXPECT_TRUE(check_disjunction(result_lqp, {1, 2, 3, 4, 5}));
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
      UnionNode::make(SetOperationMode::All,
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
      UnionNode::make(SetOperationMode::All,
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
    const auto right_col = lqp_column_(static_table_node, ColumnID{0});
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
    const auto right_col = lqp_column_(static_table_node, ColumnID{0});
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
    const auto right_col = lqp_column_(static_table_node, ColumnID{0});
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
    const auto right_col = lqp_column_(static_table_node, ColumnID{0});
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col_a, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }
}

TEST_F(InExpressionRewriteRuleTest, AutoStrategy) {
  auto rule = std::make_shared<InExpressionRewriteRule>();

  const auto cardinality_estimator = CardinalityEstimator{};

  {
    // Disjunction for single element
    const auto input_lqp = PredicateNode::make(single_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    const auto expected_lqp = PredicateNode::make(equals_(col_a, 1), node);
    EXPECT_LQP_EQ(result_lqp, expected_lqp);

    EXPECT_FLOAT_EQ(cardinality_estimator.estimate_cardinality(result_lqp), 1000.f / 200 * 1);
  }

  {
    // ExpressionEvaluator for five elements
    const auto input_lqp = PredicateNode::make(five_element_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    EXPECT_EQ(result_lqp, input_lqp);

    // No cardinality check here, as an IN expression with 5 elements will not be touched (see
    // MAX_ELEMENTS_FOR_DISJUNCTION and MIN_ELEMENTS_FOR_JOIN). These InExpressions are currently not supported by the
    // CardinalityEstimator.
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
    for (auto index = int32_t{0}; index < 100; ++index) {
      table->append({index});
    }
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_(static_table_node, ColumnID{0});
    const auto expected_lqp = JoinNode::make(JoinMode::Semi, equals_(col_a, right_col), node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);

    EXPECT_NEAR(cardinality_estimator.estimate_cardinality(result_lqp), 1000.f / 200 * 100, 10);
  }

  {
    // Join for 100 elements even if table is large
    const auto input_lqp = PredicateNode::make(hundred_element_in_expression_large_input, many_row_node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    const auto column_definitions = TableColumnDefinitions{{"right_values", DataType::Int, false}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    for (auto index = int32_t{0}; index < 100; ++index) {
      table->append({index});
    }
    const auto static_table_node = StaticTableNode::make(table);
    const auto right_col = lqp_column_(static_table_node, ColumnID{0});
    const auto expected_lqp =
        JoinNode::make(JoinMode::Semi, equals_(col_large, right_col), many_row_node, static_table_node);

    EXPECT_LQP_EQ(result_lqp, expected_lqp);
    EXPECT_TABLE_EQ_UNORDERED(static_cast<StaticTableNode&>(*result_lqp->right_input()).table, table);
  }

  {
    // Disjunction for two elements, even if one is NULL
    const auto input_lqp = PredicateNode::make(null_in_expression, node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);
    // clang-format off
    const auto expected_lqp =
      UnionNode::make(SetOperationMode::All,
        PredicateNode::make(equals_(col_a, NULL_VALUE), node),
        PredicateNode::make(equals_(col_a, 1), node));
    // clang-format on
    EXPECT_LQP_EQ(result_lqp, expected_lqp);
  }

  {
    // Disjunction for five elements, if table is large
    const auto input_lqp = PredicateNode::make(five_element_in_expression, many_row_node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    EXPECT_TRUE(check_disjunction(result_lqp, {1, 2, 3, 4, 5}));
  }

  {
    // ExpressionEvaluator, despite table is large and elements below threshold if FunctionExpression contained
    const auto input_lqp = PredicateNode::make(two_element_functional_in_expression, many_row_node);
    const auto result_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

    EXPECT_EQ(result_lqp, input_lqp);
  }
}

}  // namespace hyrise
