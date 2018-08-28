#include "join_ordering_rule.hpp"

#include "optimizer/join_graph.hpp"
#include "utils/assert.hpp"
#include "optimizer/dp_ccp.hpp"

namespace opossum {

JoinOrderingRule::JoinOrderingRule(const std::shared_ptr<AbstractCostModel>& cost_model):
  _cost_model(cost_model) {}

std::string JoinOrderingRule::name() const {
  return "JoinOrderingRule";
}

bool JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "JoinOrderingRule needs root to hold onto");
  root->set_left_input(_traverse(root->left_input()));
  return false;
}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_traverse(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  const auto join_graph = JoinGraph::from_lqp(lqp);
  if (!join_graph) {
    _traverse_inputs(lqp);
    return lqp;
  }

  const auto result_lqp = DpCcp{_cost_model}(*join_graph);

  for (const auto& vertex : join_graph->vertices) {
    _traverse_inputs(vertex);
  }

  return result_lqp;
}

void JoinOrderingRule::_traverse_inputs(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  if (lqp->left_input()) lqp->set_left_input(_traverse(lqp->left_input()));
  if (lqp->right_input()) lqp->set_right_input(_traverse(lqp->right_input()));
}

}  // namespace opossum
