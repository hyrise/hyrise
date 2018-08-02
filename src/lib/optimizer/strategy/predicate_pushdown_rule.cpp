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

void push_down(const std::shared_ptr<AbstractLQPNode>& node, std::shared_ptr<AbstractLQPNode> input) {
  DebugAssert(node->left_input() && !node->right_input(), "This helper can only push down if there is a single input");

  lqp_remove_node(node);
  const auto previous_left_input = input->left_input();

  input->set_left_input(node);
  node->set_left_input(previous_left_input);
}

bool PredicatePushdownRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type != LQPNodeType::Predicate) return _apply_to_inputs(node);

  // Only predicates with exactly one output are currently supported.
  const auto outputs = node->outputs();
  if (outputs.empty() || outputs.size() > 1) return _apply_to_inputs(node);

  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

  auto input = node->left_input();
  while (input->type == LQPNodeType::Predicate) {
    // First, try to push down the predicates that come below. That keeps the predicate order intact.
    if (_apply_to_inputs(node)) return true;

    // We gave that predicate node the chance to be pushed down, but it didn't want to. Now we ignore it.
    // We only move past it if we can get past a non-predicate node.
    input = input->left_input();
  }

  if (input->type == LQPNodeType::Join) {
    const auto join_node = std::dynamic_pointer_cast<JoinNode>(input);

    if (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Cross) {
      return _apply_to_inputs(node);
    }

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
    // always push down if other node is a sort node
    push_down(node, input);
    return true;
  } else if (input->type == LQPNodeType::Projection) {
    // push below projection if the projection does not generate the column(s) that we are scanning on
    const auto projection_input = input->left_input();
    for (const auto& predicate_argument : predicate_node->predicate->arguments) {
      const auto& expressions_before_projection = projection_input->column_expressions();
      if (!expressions_contain(predicate_argument, expressions_before_projection)) {
        // This column was created by the projection, so we can't push below it
        return false;
      }
    }
    push_down(node, input);
    return true;
  }

  return false;
}

}  // namespace opossum
