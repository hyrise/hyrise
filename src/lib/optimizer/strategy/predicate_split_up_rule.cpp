#include "predicate_split_up_rule.hpp"

#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

PredicateSplitUpRule::PredicateSplitUpRule(const bool should_split_disjunction)
    : _should_split_disjunction(should_split_disjunction) {}

void PredicateSplitUpRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateSplitUpRule needs root to hold onto");

  auto predicate_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
  visit_lqp(root, [&](const auto& sub_node) {
    if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(sub_node)) {
      predicate_nodes.emplace_back(predicate_node);
    }
    return LQPVisitation::VisitInputs;
  });

  // _split_conjunction() and _split_disjunction() split up logical expressions by calling each other recursively
  for (const auto& predicate_node : predicate_nodes) {
    if (!_split_conjunction(predicate_node)) {
      // If there is no conjunction at the top level, try to split disjunction first
      _split_disjunction(predicate_node);
    }
  }
}

bool PredicateSplitUpRule::_split_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
  const auto flat_conjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::And);
  if (flat_conjunction.size() <= 1) {
    return false;
  }

  /**
   * Split up PredicateNode with conjunctive chain (e.g., `PredicateNode(a AND b AND c)`) as its scan expression into
   * multiple consecutive PredicateNodes (e.g. `PredicateNode(c) -> PredicateNode(b) -> PredicateNode(a)`).
   */
  for (const auto& predicate_expression : flat_conjunction) {
    const auto& new_predicate_node = PredicateNode::make(predicate_expression);
    lqp_insert_node(predicate_node, LQPInputSide::Left, new_predicate_node);
    _split_disjunction(new_predicate_node);
  }
  lqp_remove_node(predicate_node);

  return true;
}

void PredicateSplitUpRule::_split_disjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
  if (!_should_split_disjunction) {
    return;
  }

  const auto flat_disjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::Or);
  if (flat_disjunction.size() <= 1) {
    return;
  }

  /**
   * Split up PredicateNode with disjunctive chain (e.g., `PredicateNode(a OR b OR c)`) as their scan expression into
   * n-1 consecutive UnionNodes and n PredicateNodes.
   */
  auto previous_union_node = UnionNode::make(UnionMode::Positions);
  const auto left_input = predicate_node->left_input();
  lqp_replace_node(predicate_node, previous_union_node);

  auto new_predicate_node = PredicateNode::make(flat_disjunction[0], left_input);
  previous_union_node->set_left_input(new_predicate_node);
  _split_conjunction(new_predicate_node);

  new_predicate_node = PredicateNode::make(flat_disjunction[1], left_input);
  previous_union_node->set_right_input(new_predicate_node);
  _split_conjunction(new_predicate_node);

  for (auto disjunction_idx = size_t{2}; disjunction_idx < flat_disjunction.size(); ++disjunction_idx) {
    const auto& predicate_expression = flat_disjunction[disjunction_idx];
    auto next_union_node = UnionNode::make(UnionMode::Positions);
    lqp_insert_node(previous_union_node, LQPInputSide::Right, next_union_node);

    new_predicate_node = PredicateNode::make(predicate_expression, left_input);
    next_union_node->set_right_input(new_predicate_node);
    _split_conjunction(new_predicate_node);

    previous_union_node = next_union_node;
  }
}

}  // namespace opossum
