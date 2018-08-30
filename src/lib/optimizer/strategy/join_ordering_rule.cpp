#include "join_ordering_rule.hpp"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/dp_ccp.hpp"
#include "optimizer/join_graph.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinOrderingRule::JoinOrderingRule(const std::shared_ptr<AbstractCostModel>& cost_model) : _cost_model(cost_model) {}

std::string JoinOrderingRule::name() const { return "JoinOrderingRule"; }

bool JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  /**
   * Dispatch _perform_join_ordering_recursively() and fix the column order afterwards
   */

  Assert(root->type == LQPNodeType::Root, "JoinOrderingRule needs root to hold onto");

  const auto expected_column_order = root->column_expressions();

  auto result_lqp = _perform_join_ordering_recursively(root->left_input());

  // Join ordering might change the output column order, let's fix that
  if (!expressions_equal(expected_column_order, result_lqp->column_expressions())) {
    result_lqp = ProjectionNode::make(expected_column_order, result_lqp);
  }

  root->set_left_input(result_lqp);

  // Figuring out whether JoinOrderingRule changed to LQP is hard, and it should be applied only once, anyway.
  // So, return false.
  return false;
}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_perform_join_ordering_recursively(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  /**
   * Try to build a JoinGraph starting from the current node
   *    -> if that fails, continue to try it with the node's inputs
   *    -> if that works
   *        -> call DpCcp on that JoinGraph
   *        -> look for more JoinGraphs below the JoinGraph's vertices
   */

  const auto join_graph = JoinGraph::from_lqp(lqp);
  if (!join_graph) {
    _recurse_to_inputs(lqp);
    return lqp;
  }

  auto result_lqp = DpCcp{_cost_model}(*join_graph);  // NOLINT - doesn't like `{}()`

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
