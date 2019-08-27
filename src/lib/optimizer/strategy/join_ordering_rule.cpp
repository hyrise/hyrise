#include "join_ordering_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/greedy_operator_ordering.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

void JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  DebugAssert(cost_estimator, "JoinOrderingRule requires cost estimator to be set");

  /**
   * Dispatch _perform_join_ordering_recursively() and fix the column order afterwards, since changing join order might
   * have changed it
   */

  Assert(root->type == LQPNodeType::Root, "JoinOrderingRule needs root to hold onto");

  const auto expected_column_order = root->column_expressions();

  auto result_lqp = _perform_join_ordering_recursively(root->left_input());

  // Join ordering might change the output column order, let's fix that
  if (!expressions_equal(expected_column_order, result_lqp->column_expressions())) {
    result_lqp = ProjectionNode::make(expected_column_order, result_lqp);
  }

  root->set_left_input(result_lqp);
}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_perform_join_ordering_recursively(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  /**
   * Try to build a JoinGraph for the current subplan
   *    -> if that fails, continue to try it with the node's inputs
   *    -> if that works
   *        -> invoke a JoinOrderingAlgorithm on that JoinGraph
   *        -> look for more JoinGraphs below the JoinGraph's vertices
   */

  const auto join_graph = JoinGraph::build_from_lqp(lqp);
  if (!join_graph) {
    _recurse_to_inputs(lqp);
    return lqp;
  }

  /**
   * Setup Cardinality and Cost Estimation caches
   *
   * Since Join Ordering Algorithms will issue many cost/cardinality estimation requests, caching is crucial to
   * optimization performance.
   * Since JoinOrderingAlgorithms build plans bottom up and are constrained to the predicates and vertices in the
   * JoinGraph, we can enable the corresponding cache policies.
   */
  const auto caching_cost_estimator = cost_estimator->new_instance();
  caching_cost_estimator->guarantee_bottom_up_construction();
  caching_cost_estimator->cardinality_estimator->guarantee_join_graph(*join_graph);

  /**
   * Select and call the actual Join Ordering Algorithm
   * Simple heuristic: Use DpCcp for any query with less than X tables and GOO for everything more complex
   */
  // TODO(anybody) Increase X once our costing/cardinality estimation is faster/uses internal caching
  auto result_lqp = std::shared_ptr<AbstractLQPNode>{};
  if (join_graph->vertices.size() < 9) {
    result_lqp = DpCcp{}(*join_graph, caching_cost_estimator);  // NOLINT - doesn't like `{}()`
  } else {
    result_lqp = GreedyOperatorOrdering{}(*join_graph, caching_cost_estimator);  // NOLINT - doesn't like `{}()`
  }

  for (const auto& vertex : join_graph->vertices) {
    _recurse_to_inputs(vertex);
  }

  return result_lqp;
}

void JoinOrderingRule::_recurse_to_inputs(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  if (lqp->left_input()) lqp->set_left_input(_perform_join_ordering_recursively(lqp->left_input()));
  if (lqp->right_input()) lqp->set_right_input(_perform_join_ordering_recursively(lqp->right_input()));
}

}  // namespace opossum
