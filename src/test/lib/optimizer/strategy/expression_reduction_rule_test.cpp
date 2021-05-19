#include <memory>

#include "base_test.hpp"
#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "expression/arithmetic_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/expression_reduction_rule.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExpressionReductionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"},
                                                           {DataType::Int, "b"},
                                                           {DataType::Int, "c"},
                                                           {DataType::Int, "d"},
                                                           {DataType::Int, "e"},
                                                           {DataType::String, "s"}});

    // Create two objects for each expression to make sure the algorithm tests for expression equality, not for pointer
    // equality
    a = equals_(mock_node->get_column("a"), 0);
    a2 = equals_(mock_node->get_column("a"), 0);
    b = equals_(mock_node->get_column("b"), 0);
    b2 = equals_(mock_node->get_column("b"), 0);
    c = equals_(mock_node->get_column("c"), 0);
    c2 = equals_(mock_node->get_column("c"), 0);
    d = equals_(mock_node->get_column("d"), 0);
    d2 = equals_(mock_node->get_column("d"), 0);
    e = equals_(mock_node->get_column("e"), 0);
    e2 = equals_(mock_node->get_column("e"), 0);
    s = mock_node->get_column("s");

    mock_node_for_join = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    a_join = equals_(mock_node_for_join->get_column("a"), 0);

    rule = std::make_shared<ExpressionReductionRule>();
  }

  std::shared_ptr<MockNode> mock_node, mock_node_for_join;
  std::shared_ptr<AbstractExpression> a, b, c, d, e, a_join;
  std::shared_ptr<LQPColumnExpression> s;
  std::shared_ptr<AbstractExpression> a2, b2, c2, d2, e2;
  std::shared_ptr<ExpressionReductionRule> rule;
};

TEST_F(ExpressionReductionRuleTest, ReduceDistributivity) {
  // clang-format off
  auto expression_a = std::shared_ptr<AbstractExpression>(or_(and_(a, b), and_(a2, b2)));
  auto expression_b = std::shared_ptr<AbstractExpression>(or_(and_(a2, b), b2));
  auto expression_c = std::shared_ptr<AbstractExpression>(or_(and_(a, and_(c, b)), and_(and_(d2, a2), e2)));
  auto expression_d = std::shared_ptr<AbstractExpression>(or_(or_(and_(a2, and_(b, c)), and_(a, b)), or_(and_(and_(and_(a, d2), b), a), and_(b2, and_(a, e)))));  // NOLINT
  auto expression_e = std::shared_ptr<AbstractExpression>(or_(and_(a2, b), or_(and_(c, d), c)));
  auto expression_f = std::shared_ptr<AbstractExpression>(and_(or_(a2, b), and_(a2, b)));
  auto expression_g = std::shared_ptr<AbstractExpression>(a);
  auto expression_h = std::shared_ptr<AbstractExpression>(and_(a, b));
  auto expression_i = std::shared_ptr<AbstractExpression>(or_(a, b));
  auto expression_j = std::shared_ptr<AbstractExpression>(and_(value_(1), or_(and_(a, b), and_(a2, b2))));
  // clang-format on

  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_a), *and_(a, b));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_b), *and_(b, a2));

  // (a AND c AND b) OR (d AND a AND e) -->
  // a AND (c AND b) OR (e AND d)
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_c), *and_(a, or_(and_(c, b), and_(e, d))));

  // (a AND b AND c) OR (a AND b) OR (a AND d AND b AND a) OR (b AND a AND e) -->
  // a AND b AND (c OR d OR e)
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_d), *and_(and_(a2, b), or_(or_(c, d), e)));

  // Expressions that aren't modified
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_e), *or_(or_(and_(a2, b), and_(c, d)), c));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_f), *and_(and_(or_(a, b), a), b));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_g), *a);
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_h), *and_(a, b));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_i), *or_(a, b));

  // Reduction works recursively
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_j), *and_(value_(1), and_(a, b)));
}

TEST_F(ExpressionReductionRuleTest, ReduceConstantExpression) {
  auto expression_a = std::shared_ptr<AbstractExpression>(add_(1, add_(3, 4)));
  ExpressionReductionRule::reduce_constant_expression(expression_a);
  EXPECT_EQ(*expression_a, *value_(8));

  auto expression_b = std::shared_ptr<AbstractExpression>(not_equals_(in_(a, list_(1, 2, add_(5, 3), 4)), 0));
  ExpressionReductionRule::reduce_constant_expression(expression_b);
  EXPECT_EQ(*expression_b, *not_equals_(in_(a, list_(1, 2, 8, 4)), 0));

  auto expression_c = std::shared_ptr<AbstractExpression>(in_(a, list_(5)));
  ExpressionReductionRule::reduce_constant_expression(expression_c);
  EXPECT_EQ(*expression_c, *in_(a, list_(5)));
}

TEST_F(ExpressionReductionRuleTest, RewriteLikeWildcard) {
  auto get_rewritten_like_predicate = [&] (std::shared_ptr<AbstractExpression> expression,
                                           bool check_left_input_is_unchanged = true) {
    auto predicate_node = PredicateNode::make(expression, mock_node);
    ExpressionReductionRule::rewrite_like_prefix_wildcard(predicate_node, expression);

    if (check_left_input_is_unchanged) {
      EXPECT_EQ(predicate_node->left_input(), mock_node);
    }

    return std::tuple<const std::shared_ptr<AbstractLQPNode>,
                      const std::shared_ptr<AbstractExpression>>{predicate_node, expression};
  };

  // Test LIKE patterns where a rewrite to simple comparison is possible
  const auto [predicate_node_a, expression_a] = get_rewritten_like_predicate(like_(s, "RED%"));
  EXPECT_EQ(*expression_a, *between_upper_exclusive_(s, "RED", "REE"));

  const auto [predicate_node_b, expression_b] = get_rewritten_like_predicate(like_(concat_(s, s), "RED%"));
  EXPECT_EQ(*expression_b, *between_upper_exclusive_(concat_(s, s), "RED", "REE"));

  // Test LIKE patterns where a NOT LIKE is rewritten to `< OR >=`
  const auto [predicate_node_c, expression_c] = get_rewritten_like_predicate(not_like_(s, "RED%"));
  EXPECT_EQ(*expression_c, *or_(less_than_(s, "RED"), greater_than_equals_(s, "REE")));

  // Test LIKE patterns where a rewrite to BETWEEN UPPER EXCLUSIVE is NOT possible
  const auto [predicate_node_d, expression_d] = get_rewritten_like_predicate(like_(s, "%RED_E%"));
  EXPECT_EQ(*expression_d, *like_(s, "%RED_E%"));

  const auto [predicate_node_e, expression_e] = get_rewritten_like_predicate(like_(s, "%"));
  EXPECT_EQ(*expression_e, *like_(s, "%"));

  const auto [predicate_node_f, expression_f] = get_rewritten_like_predicate(like_(s, "%RED"));
  EXPECT_EQ(*expression_f, *like_(s, "%RED"));

  const auto [predicate_node_g, expression_g] = get_rewritten_like_predicate(like_(s, "R_D%"));
  EXPECT_EQ(*expression_g, *like_(s, "R_D%"));

  const auto [predicate_node_h, expression_h] = get_rewritten_like_predicate(like_(s, "RE\x7F%"));
  EXPECT_EQ(*expression_h, *like_(s, "RE\x7F%"));

  const auto [predicate_node_i, expression_i] = get_rewritten_like_predicate(like_(s, "%RED%"));
  EXPECT_EQ(*expression_i, *like_(s, "%RED%"));

  // Test that non-LIKE expressions remain unaltered
  const auto [predicate_node_j, expression_j] = get_rewritten_like_predicate(greater_than_(s, "RED%"));
  EXPECT_EQ(*expression_j, *greater_than_(s, "RED%"));

  // Test LIKE patterns where a the LIKE predicate is kept, but a prior between-predicate is inserted
  const auto [predicate_node_k, expression_k] = get_rewritten_like_predicate(like_(s, "RED%BLUE%"), false);
  EXPECT_EQ(*expression_k, *like_(s, "RED%BLUE%"));
  const auto& inserted_predicate_node = dynamic_cast<PredicateNode&>(*predicate_node_k->left_input());
  EXPECT_EQ(*inserted_predicate_node.predicate(), *between_upper_exclusive_(s, "RED", "REE"));
  EXPECT_EQ(predicate_node_k->left_input()->left_input(), mock_node);
}

TEST_F(ExpressionReductionRuleTest, RemoveDuplicateAggregate) {
  const auto table_definition = TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, true}};
  const auto table = Table::create_dummy_table(table_definition);
  Hyrise::get().storage_manager.add_table("agg_table", table);
  const auto stored_table_node = StoredTableNode::make("agg_table");

  const auto col_a = lqp_column_(stored_table_node, ColumnID{0});
  const auto col_b = lqp_column_(stored_table_node, ColumnID{1});

  {
    // SELECT SUM(a), COUNT(a), AVG(a) -> SUM(a), COUNT(a), SUM(a) / COUNT(a) AS AVG(a)
    // clang-format off
    const auto input_lqp = AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_a), avg_(col_a)),                                            // NOLINT
                             stored_table_node);

    const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(a)", "AVG(a)"};
    // The cast will become unnecessary once #1799 is fixed
    const auto expected_lqp = AliasNode::make(expression_vector(sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))), expected_aliases,  // NOLINT
                                ProjectionNode::make(expression_vector(sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))),             // NOLINT
                                  AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_a)), stored_table_node)));                            // NOLINT
    // clang-format on

    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT SUM(a), COUNT(*), AVG(a) -> SUM(a), COUNT(*), SUM(a) / COUNT(*) AS AVG(a) as a is not NULLable
    // clang-format off
    const auto input_lqp = AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_star_(stored_table_node), avg_(col_a)),                                            // NOLINT
                             stored_table_node);

    const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(*)", "AVG(a)"};
    // The cast will become unnecessary once #1799 is fixed
    const auto expected_lqp = AliasNode::make(expression_vector(sum_(col_a), count_star_(stored_table_node), div_(cast_(sum_(col_a), DataType::Double), count_star_(stored_table_node))), expected_aliases,  // NOLINT
                                ProjectionNode::make(expression_vector(sum_(col_a), count_star_(stored_table_node), div_(cast_(sum_(col_a), DataType::Double), count_star_(stored_table_node))),             // NOLINT
                                  AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_star_(stored_table_node)),                                                  // NOLINT
                                    stored_table_node)));
    // clang-format on

    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT SUM(a), COUNT(a), AVG(a) GROUP BY b -> SUM(a), COUNT(a), SUM(a) / COUNT(a) AS AVG(a) GROUP BY b
    // clang-format off
    const auto input_lqp = AggregateNode::make(expression_vector(col_b), expression_vector(sum_(col_a), count_(col_a), avg_(col_a)),                                              // NOLINT
                             stored_table_node);

    const auto expected_aliases = std::vector<std::string>{"b", "SUM(a)", "COUNT(a)", "AVG(a)"};
    const auto expected_lqp = AliasNode::make(expression_vector(col_b, sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))), expected_aliases,  // NOLINT
                                ProjectionNode::make(expression_vector(col_b, sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))),             // NOLINT
                                  AggregateNode::make(expression_vector(col_b), expression_vector(sum_(col_a), count_(col_a)), stored_table_node)));                              // NOLINT
    // clang-format on

    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT SUM(b), COUNT(*), AVG(b) stays unmodified as b is NULLable
    // clang-format off
    const auto input_lqp = AggregateNode::make(expression_vector(), expression_vector(sum_(col_b), count_star_(stored_table_node), avg_(col_b)),  // NOLINT
                             stored_table_node);
    // clang-format on

    const auto expected_lqp = input_lqp->deep_copy();
    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT COUNT(*) stays unmodified as it cannot be further reduced
    // clang-format off
    const auto join_node = JoinNode::make(JoinMode::Inner, equals_(col_a, col_b),
                             stored_table_node,
                             stored_table_node);
    const auto input_lqp = AggregateNode::make(expression_vector(), expression_vector(count_star_(join_node)),  // NOLINT
                             join_node);
    // clang-format on

    const auto expected_lqp = input_lqp->deep_copy();
    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT SUM(a), COUNT(b), AVG(a) stays unmodified as COUNT(b) is unrelated
    // clang-format off
    const auto input_lqp = AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_b), avg_(col_a)),  // NOLINT
                             stored_table_node);
    // clang-format on

    const auto expected_lqp = input_lqp->deep_copy();
    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT SUM(a), COUNT(a) + 1, AVG(a) + 2 -> SUM(a), COUNT(a) + 1, SUM(a) / COUNT(a) + 2 AS AVG(a) + 2
    // clang-format off
    const auto input_lqp = ProjectionNode::make(expression_vector(sum_(col_a), add_(count_(col_a), 1), add_(avg_(col_a), 2)),       // NOLINT
                             AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_a), avg_(col_a)),   // NOLINT
                              stored_table_node));

    const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(a) + 1", "AVG(a) + 2"};
    const auto expected_lqp = AliasNode::make(expression_vector(sum_(col_a), add_(count_(col_a), 1), add_(div_(cast_(sum_(col_a), DataType::Double), count_(col_a)), 2)), expected_aliases,  // NOLINT
                                ProjectionNode::make(expression_vector(sum_(col_a), add_(count_(col_a), 1), add_(div_(cast_(sum_(col_a), DataType::Double), count_(col_a)), 2)),             // NOLINT
                                  ProjectionNode::make(expression_vector(sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))),                             // NOLINT
                                    AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_a)),                                                                  // NOLINT
                                      stored_table_node))));
    // clang-format on

    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  {
    // SELECT SUM(a), COUNT(a) AS foo, AVG(a) AS bar -> SUM(a), COUNT(a) AS foo, SUM(a) / COUNT(a) AS bar
    const auto aliases = std::vector<std::string>{"SUM(a)", "foo", "bar"};

    // clang-format off
    const auto input_lqp = AliasNode::make(expression_vector(sum_(col_a), count_(col_a), avg_(col_a)), aliases,
                             AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_a), avg_(col_a)),                                 // NOLINT
                              stored_table_node));

    const auto expected_lqp = AliasNode::make(expression_vector(sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))), aliases,  // NOLINT
                                ProjectionNode::make(expression_vector(sum_(col_a), count_(col_a), div_(cast_(sum_(col_a), DataType::Double), count_(col_a))),    // NOLINT
                                  AggregateNode::make(expression_vector(), expression_vector(sum_(col_a), count_(col_a)),                                         // NOLINT
                                    stored_table_node)));
    // clang-format on

    const auto actual_lqp = apply_rule(rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(ExpressionReductionRuleTest, ApplyToLQP) {
  const auto a_and_b = and_(a, b);
  const auto a_and_c = and_(a, c);

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(a_and_b, a_and_c),
    PredicateNode::make(like_(s, "RED%"),
      PredicateNode::make(equals_(3, add_(4, 3)),
        mock_node)));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(and_(a, or_(b, c)),
    PredicateNode::make(between_upper_exclusive_(s, "RED", "REE"),
      PredicateNode::make(equals_(3, 7),
        mock_node)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
