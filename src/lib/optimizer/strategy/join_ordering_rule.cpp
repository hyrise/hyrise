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

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

std::shared_ptr<AbstractLQPNode> perform_join_ordering_recursively(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  const auto recurse_to_inputs = [&](const auto& node) {
    if (node->left_input()) {
      node->set_left_input(perform_join_ordering_recursively(node->left_input(), cost_estimator));
    }

    if (node->right_input()) {
      node->set_right_input(perform_join_ordering_recursively(node->right_input(), cost_estimator));
    }
  };

  /**
   * Try to build a JoinGraph for the current subplan
   *    -> if that fails, continue to try it with the node's inputs
   *    -> if that works
   *        -> invoke a JoinOrderingAlgorithm on that JoinGraph
   *        -> look for more JoinGraphs below the JoinGraph's vertices
   */

  const auto join_graph = JoinGraph::build_from_lqp(lqp);
  if (!join_graph) {
    recurse_to_inputs(lqp);
    return lqp;
  }

  /**
   * Setup Cardinality and Cost Estimation caches.
   *
   * As join ordering algorithms issue many cost/cardinality estimation requests, caching is crucial to optimization
   * performance. We can enable the corresponding cache policies because join ordering algorithms build plans bottom-up
   * and are constrained to the predicates and vertices in the JoinGraph.
   */
  const auto caching_cost_estimator = cost_estimator->new_instance();
  caching_cost_estimator->guarantee_bottom_up_construction();
  caching_cost_estimator->cardinality_estimator->guarantee_join_graph(*join_graph);

  /**
   * Select and call the actual ioin ordering algorithm. Simple heuristic: Use DpCcp for any query with less than X
   * tables and GOO for everything more complex.
   */
  // TODO(anybody) Increase X once our cost/cardinality estimation is faster/uses internal caching.
  auto result_lqp = std::shared_ptr<AbstractLQPNode>{};
  DebugAssert(!join_graph->vertices.empty(), "There should be nodes in the join graph.");
  if (join_graph->vertices.size() == 1) {
    // A JoinGraph with only one vertex is no actual join and needs no ordering.
    result_lqp = lqp;
  } else if (join_graph->vertices.size() < 9) {
    result_lqp = DpCcp{}(*join_graph, caching_cost_estimator);
  } else {
    result_lqp = GreedyOperatorOrdering{}(*join_graph, caching_cost_estimator);
  }

  for (const auto& vertex : join_graph->vertices) {
    recurse_to_inputs(vertex);
  }

  return result_lqp;
}

}  // namespace

namespace hyrise {

std::string JoinOrderingRule::name() const {
  static const auto name = std::string{"JoinOrderingRule"};
  return name;
}

void JoinOrderingRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  DebugAssert(cost_estimator, "JoinOrderingRule requires cost estimator to be set");

  /**
   * Dispatch _perform_join_ordering_recursively() and fix the column order afterwards, since changing join order might
   * have changed it.
   */

  Assert(lqp_root->type == LQPNodeType::Root, "JoinOrderingRule needs root to hold onto.");

  const auto expected_column_order = lqp_root->output_expressions();

  auto result_lqp = perform_join_ordering_recursively(lqp_root->left_input(), cost_estimator);

  // Join ordering might change the output column order, let us fix that.
  if (!expressions_equal(expected_column_order, result_lqp->output_expressions())) {
    result_lqp = ProjectionNode::make(expected_column_order, result_lqp);
  }

  lqp_root->set_left_input(result_lqp);
}

}  // namespace hyrise
