#include "predicate_pushdown_rule.hpp"
#include "all_parameter_variant.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "operators/operator_scan_predicate.hpp"

namespace opossum {

std::string PredicatePushdownRule::name() const { return "Predicate Pushdown Rule"; }

bool PredicatePushdownRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type != LQPNodeType::Predicate) return _apply_to_inputs(node);

  // Only predicates with exactly one output are currently supported.
  const auto outputs = node->outputs();
  if (outputs.empty() || outputs.size() > 1) return _apply_to_inputs(node);

  // Find a join node
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

  auto input = node->left_input();
  while (input->type == LQPNodeType::Predicate) {
    input = input->left_input();
  }

  if (input->type == LQPNodeType::Join) {
    const auto join_node = std::dynamic_pointer_cast<JoinNode>(input);

    if (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Cross)
      return _apply_to_inputs(node);

    const auto move_to_left = expression_evaluable_on_lqp(predicate_node->predicate, *join_node->left_input());
    const auto move_to_right = expression_evaluable_on_lqp(predicate_node->predicate, *join_node->right_input());

    if (!move_to_left && !move_to_right) return _apply_to_inputs(node);

    lqp_remove_node(node);

    if (move_to_left) {
      const auto previous_left_input = join_node->left_input();
      join_node->set_left_input(node);
      node->set_left_input(previous_left_input);
    } else {
      const auto previous_right_input = join_node->right_input();
      join_node->set_right_input(node);
      node->set_left_input(previous_right_input);
    }

    return true;

  } else if (input->type == LQPNodeType::Sort) {
    // push always down if other node is a sort node
    const auto sort_node = std::dynamic_pointer_cast<SortNode>(input);

    lqp_remove_node(node);
    const auto previous_left_input = sort_node->left_input();

    sort_node->set_left_input(node);
    node->set_left_input(previous_left_input);

    return true;
  }
  return false;
}

}  // namespace opossum
