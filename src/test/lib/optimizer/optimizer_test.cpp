#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/join_graph_statistics_cache.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class OptimizerTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "node_a");
    a = node_a->get_column("a");
    b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}}, "node_b");
    x = node_b->get_column("x");
    y = node_b->get_column("y");

    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}}, "node_c");
    u = node_c->get_column("u");

    subquery_lqp_a = LimitNode::make(to_expression(1), node_b);
    subquery_a = lqp_subquery_(subquery_lqp_a);
    const auto correlated_parameter = correlated_parameter_(ParameterID{0}, b);
    // clang-format off
    subquery_lqp_b =
    LimitNode::make(to_expression(1),
      PredicateNode::make(greater_than_(u, correlated_parameter),
        node_c));
    // clang-format on
    subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, correlated_parameter));
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<LQPColumnExpression> a, b, x, y, u;
  std::shared_ptr<AbstractLQPNode> subquery_lqp_a, subquery_lqp_b;
  std::shared_ptr<LQPSubqueryExpression> subquery_a, subquery_b;
};

TEST_F(OptimizerTest, RequiresOwnership) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto optimizer = Optimizer::create_default_optimizer();
  // Does not move the LQP into the optimizer
  EXPECT_THROW(optimizer->optimize(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsValidOutputsOutOfPlan) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto out_of_plan_node = LimitNode::make(value_(10), lqp->left_input());

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsValidOutputsInDifferentPlan) {
  // LimitNode is part of an uncorrelated and a correlated subquery.

  // clang-format off
  auto lqp =
  PredicateNode::make(greater_than_(a, subquery_b),
    PredicateNode::make(less_than_(a, lqp_subquery_(subquery_lqp_b)),
      node_a));
  // clang-format on

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsCorrectNumberOfInputs) {
  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(b, 1),
    ProjectionNode::make(expression_vector(add_(b, subquery_a)),
      PredicateNode::make(greater_than_(a, subquery_b),
        node_a)));
  // clang-format on

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsOriginalNodeInSamePlan) {
  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(x, 1),
    ProjectionNode::make(expression_vector(add_(b, subquery_a)),
      PredicateNode::make(greater_than_(a, subquery_b),
        node_a)));
  // clang-format on

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AllowsSubqueryReuse) {
  subquery_lqp_a->set_left_input(nullptr);
  // clang-format off
  const auto aggregate_node =
  AggregateNode::make(expression_vector(), expression_vector(min_(x), max_(x)),
    PredicateNode::make(between_inclusive_(y, 1, 3),
      node_b));

  const auto min_x =
  ProjectionNode::make(expression_vector(min_(x)),
    aggregate_node);
  const auto subquery_min_x = lqp_subquery_(min_x);

  const auto max_x =
  ProjectionNode::make(expression_vector(max_(x)),
    aggregate_node);
  const auto subquery_max_x = lqp_subquery_(max_x);

  auto lqp =
  PredicateNode::make(between_inclusive_(a, subquery_min_x, subquery_max_x),
    node_a);
  // clang-format on

  Optimizer::validate_lqp(lqp);
}

TEST_F(OptimizerTest, VerifiesResults) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  // While the Asserts* tests checked the different features of validate_lqp, this test checks that a rule that breaks
  // an LQP throws an assertion.
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto optimizer = Optimizer{};

  class LQPBreakingRule : public AbstractRule {
   public:
    explicit LQPBreakingRule(const std::shared_ptr<AbstractExpression>& out_of_plan_expression)
        : _out_of_plan_expression(out_of_plan_expression) {}

    std::string name() const override {
      return "LQPBreakingRule";
    }

   protected:
    void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override {
      // Change the `b` expression in the projection to `u`, which is not part of the input LQP.
      const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(lqp_root->left_input());
      if (!projection_node) {
        return;
      }
      projection_node->node_expressions[0] = _out_of_plan_expression;
    }

    std::shared_ptr<AbstractExpression> _out_of_plan_expression;
  };

  optimizer.add_rule(std::make_unique<LQPBreakingRule>(u));

  EXPECT_THROW(optimizer.optimize(std::move(lqp)), std::logic_error);
}

TEST_F(OptimizerTest, OptimizesSubqueries) {
  /**
   * Test that the Optimizer's rules reach subqueries
   */

  std::unordered_set<std::shared_ptr<AbstractLQPNode>> nodes;

  // A "rule" that just collects the nodes it was applied to
  class MockRule : public AbstractRule {
   public:
    explicit MockRule(std::unordered_set<std::shared_ptr<AbstractLQPNode>>& init_nodes) : nodes(init_nodes) {}

    std::string name() const override {
      return "MockRule";
    }

   protected:
    void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override {
      visit_lqp(lqp_root, [&](const auto& node) {
        nodes.emplace(node);
        return LQPVisitation::VisitInputs;
      });
    }

    std::unordered_set<std::shared_ptr<AbstractLQPNode>>& nodes;
  };

  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto rule = std::make_unique<MockRule>(nodes);

  Optimizer optimizer{};
  optimizer.add_rule(std::move(rule));

  optimizer.optimize(std::move(lqp));

  // Test that the optimizer has reached all nodes (the number includes all nodes created above and the root nodes
  // created by the optimizer for the lqp and each subquery)
  EXPECT_EQ(nodes.size(), 11u);
}

TEST_F(OptimizerTest, OptimizesSubqueriesExactlyOnce) {
  /**
   * This one is important. An LQP can contain the same Subquery multiple times (see the LQP constructed below).
   * We need make sure that each LQP is only optimized once and that all SubqueryExpressions point to the same LQP after
   * optimization.
   * 
   * This is not just "nice to have". If we do not do this properly and multiple SubqueryExpressions point to the same LQP
   * then we would potentially break all the other SubqueryExpressions while moving nodes around when optimizing a single
   * one of them.
   */

  // clang-format off
  /** Initialise an LQP that contains the same subquery expression twice */
  auto lqp = std::static_pointer_cast<AbstractLQPNode>(
  PredicateNode::make(greater_than_(add_(b, subquery_a), 2),
    ProjectionNode::make(expression_vector(add_(b, subquery_a)),
      PredicateNode::make(greater_than_(a, subquery_b),
        node_a))));
  // clang-format on

  /**
   * 1. Optional: Deep copy the LQP, thereby making its SubqueryExpressions point to different copies of the same LQP
   *    (The optimizer should incidentally make them point to the same LQP again)
   */
  lqp = lqp->deep_copy();
  {
    auto predicate_node_a = std::dynamic_pointer_cast<PredicateNode>(lqp);
    ASSERT_TRUE(predicate_node_a);
    auto subquery_a_a = std::dynamic_pointer_cast<LQPSubqueryExpression>(
        predicate_node_a->predicate()->arguments.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_a);
    auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(lqp->left_input());
    ASSERT_TRUE(projection_node);
    auto subquery_a_b =
        std::dynamic_pointer_cast<LQPSubqueryExpression>(projection_node->node_expressions.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_b);

    EXPECT_NE(subquery_a_a->lqp, subquery_a_b->lqp);
  }

  /**
   * 2. Optimize the LQP with a telemetric Rule
   */

  // A "rule" that counts how often it was `apply_to()`ed. We use this to check whether SubqueryExpressions pointing to
  // the same LQP before optimization still point to the same (though `deep_copy()`ed) LQP afterwards.
  auto counter = size_t{0};

  class MockRule : public AbstractRule {
   public:
    explicit MockRule(size_t& init_counter) : counter(init_counter) {}

    std::string name() const override {
      return "MockRule";
    }

    size_t& counter;

   protected:
    void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override {
      ++counter;
    }
  };

  auto rule = std::make_unique<MockRule>(counter);

  auto optimizer = Optimizer{};
  optimizer.add_rule(std::move(rule));

  const auto optimized_lqp = optimizer.optimize(std::move(lqp));
  lqp = nullptr;

  /**
   * 3. Check that the rule was invoked 3 times. Once for the main LQP, once for subquery_a and once for subquery_b.
   */
  EXPECT_EQ(counter, 3);

  /**
   * 4. Check that now - after optimizing - both SubqueryExpressions using subquery_lqp_a point to the same LQP object
   *    again.
   */
  {
    auto predicate_node_a = std::dynamic_pointer_cast<PredicateNode>(optimized_lqp);
    ASSERT_TRUE(predicate_node_a);
    auto subquery_a_a = std::dynamic_pointer_cast<LQPSubqueryExpression>(
        predicate_node_a->predicate()->arguments.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_a);
    auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(optimized_lqp->left_input());
    ASSERT_TRUE(projection_node);
    auto subquery_a_b =
        std::dynamic_pointer_cast<LQPSubqueryExpression>(projection_node->node_expressions.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_b);
    auto predicate_node_b = std::dynamic_pointer_cast<PredicateNode>(optimized_lqp->left_input()->left_input());
    ASSERT_TRUE(predicate_node_b);
    auto subquery_b_a =
        std::dynamic_pointer_cast<LQPSubqueryExpression>(predicate_node_b->predicate()->arguments.at(1));
    ASSERT_TRUE(subquery_b_a);

    EXPECT_EQ(subquery_a_a->lqp, subquery_a_b->lqp);
    EXPECT_LQP_EQ(subquery_b_a->lqp, subquery_lqp_b);
  }
}

TEST_F(OptimizerTest, PollutedCardinalityEstimationCache) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  // If the caches of the CardinalityEstimator are filled after applying a rule, later estimations might be incorrect.
  const auto cardinality_estimator = std::make_shared<CardinalityEstimator>();
  auto optimizer = Optimizer{std::make_shared<CostEstimatorLogical>(cardinality_estimator)};

  class MockRule : public AbstractRule {
   public:
    std::string name() const override {
      return "MockRule";
    }

   protected:
    void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override {}
  };

  optimizer.add_rule(std::make_unique<MockRule>());

  // All caches are empty, nothing happens.
  {
    auto lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    optimizer.optimize(std::move(lqp));
  }

  // Unempty `join_graph_statistics_cache`.
  auto& estimation_cache = cardinality_estimator->cardinality_estimation_cache;
  auto vertex_indices = JoinGraphStatisticsCache::VertexIndexMap{};
  auto predicate_indices = JoinGraphStatisticsCache::PredicateIndexMap{};
  estimation_cache.join_graph_statistics_cache.emplace(std::move(vertex_indices), std::move(predicate_indices));
  {
    auto lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    EXPECT_THROW(optimizer.optimize(std::move(lqp)), std::logic_error);
  }

  // All caches are empty again, nothing happens.
  estimation_cache.join_graph_statistics_cache.reset();
  {
    auto lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    optimizer.optimize(std::move(lqp));
  }

  // Unempty `statistics_by_lqp`.
  estimation_cache.statistics_by_lqp.emplace();
  {
    auto lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
    EXPECT_THROW(optimizer.optimize(std::move(lqp)), std::logic_error);
  }
}

}  // namespace hyrise
