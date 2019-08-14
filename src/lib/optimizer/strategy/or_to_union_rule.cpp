#include <expression/expression_utils.hpp>

#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "or_to_union_rule.hpp"

namespace opossum {

std::string OrToUnionRule::name() const { return "Or to Union Rule"; }

void OrToUnionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type == LQPNodeType::Predicate) {
    const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
    const auto predicate = predicate_node->predicate();

    const auto flat_disjunction = flatten_logical_expressions(predicate, LogicalOperator::Or);
    if (flat_disjunction.size() == 2) {
      const auto union_node = UnionNode::make(UnionMode::Positions);
      const auto left_input = node->left_input();
      lqp_replace_node(node, union_node);
      union_node->set_left_input(PredicateNode::make(flat_disjunction[0], left_input));
      union_node->set_right_input(PredicateNode::make(flat_disjunction[1], left_input));
      _apply_to_inputs(union_node);
    } else {
      _apply_to_inputs(node);
    }
  } else {
    _apply_to_inputs(node);
  }
}

}  // namespace opossum
