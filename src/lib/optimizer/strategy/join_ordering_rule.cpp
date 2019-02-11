#include "join_ordering_rule.hpp"

#include "cost_model/cost_estimation_cache.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/greedy_operator_ordering.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/estimation_caches.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/table_statistics2.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string JoinOrderingRule::name() const { return "JoinOrderingRule"; }

void JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root,
                                const std::shared_ptr<AbstractCostEstimator>& cost_estimator) const {
  /**
   * Dispatch _perform_join_ordering_recursively() and fix the column order afterwards, since changing join order might
   * have changed it
   */

  Assert(root->type == LQPNodeType::Root, "JoinOrderingRule needs root to hold onto");

  const auto expected_column_order = root->column_expressions();

  auto result_lqp = _perform_join_ordering_recursively(root->left_input(), cost_estimator);

  // Join ordering might change the output column order, let's fix that
  if (!expressions_equal(expected_column_order, result_lqp->column_expressions())) {
    result_lqp = ProjectionNode::make(expected_column_order, result_lqp);
  }

  root->set_left_input(result_lqp);
}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_perform_join_ordering_recursively(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<AbstractCostEstimator>& cost_estimator) const {
  /**
   * Try to build a JoinGraph starting for the current subplan
   *    -> if that fails, continue to try it with the node's inputs
   *    -> if that works
   *        -> call DpCcp on that JoinGraph
   *        -> look for more JoinGraphs below the JoinGraph's vertices
   */

  const auto join_graph = JoinGraph::build_from_lqp(lqp);
  if (!join_graph) {
    _recurse_to_inputs(lqp, cost_estimator);
    return lqp;
  }

  // Setup caches used for Cardinalitiy and Cost estimations
  const auto cost_estimation_cache = std::make_shared<CostEstimationCache>();
  const auto cardinality_estimation_cache = std::make_shared<CardinalityEstimationCache>();
  cardinality_estimation_cache->join_statistics_cache.emplace(JoinStatisticsCache::from_join_graph(*join_graph));
  cardinality_estimation_cache->plan_statistics_cache.emplace();


  // Simple heuristic: Use DpCcp for any query with less than X tables and GOO for everything more complex
  // TODO(anybody) Increase X once our costing/cardinality estimation is faster/uses internal caching
  auto result_lqp = std::shared_ptr<AbstractLQPNode>{};
  if (join_graph->vertices.size() < 9) {
    result_lqp = DpCcp{}(*join_graph, cost_estimator, cost_estimation_cache, cardinality_estimation_cache);  // NOLINT - doesn't like `{}()`
  } else {
    result_lqp = GreedyOperatorOrdering{}(*join_graph, cost_estimator, cost_estimation_cache, cardinality_estimation_cache);  // NOLINT - doesn't like `{}()`
  }

  for (const auto& vertex : join_graph->vertices) {
    _recurse_to_inputs(vertex, cost_estimator);
  }

  return result_lqp;
}

void JoinOrderingRule::_recurse_to_inputs(const std::shared_ptr<AbstractLQPNode>& lqp,
                                          const std::shared_ptr<AbstractCostEstimator>& cost_estimator) const {
  if (lqp->left_input())
    lqp->set_left_input(_perform_join_ordering_recursively(lqp->left_input(), cost_estimator));
  if (lqp->right_input())
    lqp->set_right_input(_perform_join_ordering_recursively(lqp->right_input(), cost_estimator));
}

}  // namespace opossum
