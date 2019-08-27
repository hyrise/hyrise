#include "predicate_split_up_rule.hpp"

#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

bool PredicateSplitUpRule::splitAnd(const std::shared_ptr<AbstractLQPNode>& root) const {
  /**
  * Step 1:
  *    - Collect PredicateNodes that can be split up into multiple ones into `predicate_nodes_to_flat_conjunctions`
  */
  auto predicate_nodes_to_flat_conjunctions =
      std::vector<std::pair<std::shared_ptr<PredicateNode>, std::vector<std::shared_ptr<AbstractExpression>>>>{};

  visit_lqp(root, [&](const auto& sub_node) {
    if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(sub_node)) {
      const auto flat_conjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::And);

      if (flat_conjunction.size() > 1) {
        predicate_nodes_to_flat_conjunctions.emplace_back(predicate_node, flat_conjunction);
      }
    }

    return LQPVisitation::VisitInputs;
  });

  if (predicate_nodes_to_flat_conjunctions.empty()) {
    return false;
  }

  /**
   * Step 2:
   *    - Split up qualifying PredicateNodes into multiple consecutive PredicateNodes. We have to do this in a
   *      second pass because manipulating the LQP within `visit_lqp()`, while theoretically possible, is prone to
   *      bugs.
   */
  for (const auto& [predicate_node, flat_conjunction] : predicate_nodes_to_flat_conjunctions) {
    for (const auto& predicate_expression : flat_conjunction) {
      lqp_insert_node(predicate_node, LQPInputSide::Left, PredicateNode::make(predicate_expression));
    }
    lqp_remove_node(predicate_node);
  }

  return true;
}

bool PredicateSplitUpRule::splitOr(const std::shared_ptr<AbstractLQPNode>& root) const {
  /**
   * Step 1:
   *    - Collect PredicateNodes that can be split up into multiple ones into `predicate_nodes_to_flat_disjunctions`
   */
  auto predicate_nodes_to_flat_disjunctions =
      std::vector<std::pair<std::shared_ptr<PredicateNode>, std::vector<std::shared_ptr<AbstractExpression>>>>{};

  visit_lqp(root, [&](const auto& sub_node) {
    if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(sub_node)) {
      const auto flat_disjunction = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::Or);

      if (flat_disjunction.size() > 1) {
        predicate_nodes_to_flat_disjunctions.emplace_back(predicate_node, flat_disjunction);
      }
    }

    return LQPVisitation::VisitInputs;
  });

  if (predicate_nodes_to_flat_disjunctions.empty()) {
    return false;
  }

  /**
   * Step 2:
   *    - Split up qualifying PredicateNodes into n-1 consecutive UnionNodes and n PredicateNodes. We have to do this in
   *      a second pass because manipulating the LQP within `visit_lqp()`, while theoretically possible, is prone to
   *      bugs.
   */
  for (const auto& [predicate_node, flat_disjunction] : predicate_nodes_to_flat_disjunctions) {
    auto previous_union_node = UnionNode::make(UnionMode::Positions);
    const auto left_input = predicate_node->left_input();
    lqp_replace_node(predicate_node, previous_union_node);
    previous_union_node->set_left_input(PredicateNode::make(flat_disjunction[0], left_input));
    previous_union_node->set_right_input(PredicateNode::make(flat_disjunction[1], left_input));

    for (auto disjunction_idx = size_t{2}; disjunction_idx < flat_disjunction.size(); ++disjunction_idx) {
      const auto& predicate_expression = flat_disjunction[disjunction_idx];
      auto next_union_node = UnionNode::make(UnionMode::Positions);
      lqp_insert_node(previous_union_node, LQPInputSide::Right, next_union_node);
      next_union_node->set_right_input(PredicateNode::make(predicate_expression, left_input));
      previous_union_node = next_union_node;
    }
  }

  return true;
}

void PredicateSplitUpRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateSplitUpRule needs root to hold onto");

  while (splitAnd(root) || splitOr(root)) {
  }  // Split nested predicates as long as possible
}

}  // namespace opossum
