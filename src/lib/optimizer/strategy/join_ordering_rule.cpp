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
  Assert(root->type == LQPNodeType::Root, "JoinOrderingRule needs root to hold onto");

  const auto expected_column_order = root->column_expressions();

  auto result_lqp = _traverse_and_perform_join_ordering(root->left_input());

  // Join ordering might change the output column order, let's fix that
  if (!expressions_equal(expected_column_order, result_lqp->column_expressions())) {
    result_lqp = ProjectionNode::make(expected_column_order, result_lqp);
  }

  root->set_left_input(result_lqp);

  return false;
}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_traverse_and_perform_join_ordering(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  const auto join_graph = JoinGraph::from_lqp(lqp);
  if (!join_graph) {
    _apply_traverse_to_inputs(lqp);
    return lqp;
  }

  auto result_lqp = DpCcp{_cost_model}(*join_graph);  // NOLINT - doesn't like `{}()`

  for (const auto& vertex : join_graph->vertices) {
    _apply_traverse_to_inputs(vertex);
  }
  return result_lqp;
}

void JoinOrderingRule::_apply_traverse_to_inputs(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  if (lqp->left_input()) lqp->set_left_input(_traverse_and_perform_join_ordering(lqp->left_input()));
  if (lqp->right_input()) lqp->set_right_input(_traverse_and_perform_join_ordering(lqp->right_input()));
}

}  // namespace opossum
