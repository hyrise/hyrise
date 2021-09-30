#include <memory>

#include "base_test.hpp"
#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "expression/arithmetic_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/strategy/like_rewrite_rule.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LikeRewriteRuleTest : public StrategyBaseTest {
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

    rule = std::make_shared<LikeRewriteRule>();
  }

  std::shared_ptr<MockNode> mock_node, mock_node_for_join;
  std::shared_ptr<AbstractExpression> a, b, c, d, e, a_join;
  std::shared_ptr<LQPColumnExpression> s;
  std::shared_ptr<AbstractExpression> a2, b2, c2, d2, e2;
  std::shared_ptr<LikeRewriteRule> rule;
};

TEST_F(LikeRewriteRuleTest, RewriteLikePrefixWildcard) {
  // Test LIKE patterns where a rewrite to simple comparison is possible
  auto expression_a = std::shared_ptr<AbstractExpression>(like_(s, "RED%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_a);
  EXPECT_EQ(*expression_a, *between_upper_exclusive_(s, "RED", "REE"));

  auto expression_b = std::shared_ptr<AbstractExpression>(not_like_(s, "RED%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_b);
  EXPECT_EQ(*expression_b, *or_(less_than_(s, "RED"), greater_than_equals_(s, "REE")));

  auto expression_c = std::shared_ptr<AbstractExpression>(like_(concat_(s, s), "RED%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_c);
  EXPECT_EQ(*expression_c, *between_upper_exclusive_(concat_(s, s), "RED", "REE"));

  auto expression_d = std::shared_ptr<AbstractExpression>(like_(s, "%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_d);
  EXPECT_EQ(*expression_d, *like_(s, "%"));

  auto expression_e = std::shared_ptr<AbstractExpression>(like_(s, "%RED"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_e);
  EXPECT_EQ(*expression_e, *like_(s, "%RED"));

  auto expression_f = std::shared_ptr<AbstractExpression>(like_(s, "R_D%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_f);
  EXPECT_EQ(*expression_f, *like_(s, "R_D%"));

  auto expression_g = std::shared_ptr<AbstractExpression>(like_(s, "RE\x7F%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_g);
  EXPECT_EQ(*expression_g, *like_(s, "RE\x7F%"));

  // LIKE patterns with two wildcards remove and insert PredicateNodes. Thus, we need PredicateNodes for such cases.
  auto expression_h = std::shared_ptr<AbstractExpression>(like_(s, "RED%E%"));
  auto predicate_node = PredicateNode::make(expression_h, mock_node);

  const auto root_node = LogicalPlanRootNode::make();
  root_node->set_left_input(predicate_node);
  LikeRewriteRule::rewrite_like_prefix_wildcard(predicate_node, expression_h);

  auto new_predicate_node = root_node->left_input();
  ASSERT_NE(new_predicate_node, predicate_node);
  EXPECT_EQ(*(new_predicate_node->node_expressions.at(0)), *like_(s, "RED%"));
  EXPECT_EQ(*(new_predicate_node->left_input()->node_expressions.at(0)), *like_(s, "%E%"));

  // Test that non-LIKE expressions remain unaltered
  auto expression_i = std::shared_ptr<AbstractExpression>(greater_than_(s, "RED%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_i);
  EXPECT_EQ(*expression_i, *greater_than_(s, "RED%"));

  // Test that more than two wildcards remain unaltered

  auto expression_j = std::shared_ptr<AbstractExpression>(like_(s, "RED%E%F%"));
  LikeRewriteRule::rewrite_like_prefix_wildcard(nullptr, expression_j);
  EXPECT_EQ(*expression_j, *like_(s, "RED%E%F%"));
}

TEST_F(LikeRewriteRuleTest, ApplyToLQP) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(like_(s, "RED%"),
    PredicateNode::make(like_(s, "2009:%Japan%"),
        mock_node));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  // clang-format off
  const auto expected_lqp =
    PredicateNode::make(between_upper_exclusive_(s, "RED", "REE"),
      PredicateNode::make(like_(s, "2009:%"),
        PredicateNode::make(like_(s, "%Japan%"),
        mock_node)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
