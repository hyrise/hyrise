#include "projection_combination_rule.hpp"

#include <memory>
#include <string>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

std::string ProjectionCombinationRule::name() const { return "Projection Combination Rule"; }

bool ProjectionCombinationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() != LQPNodeType::Projection) {
    return _apply_to_inputs(node);
  }

  std::vector<std::shared_ptr<ProjectionNode>> projection_nodes;

  // Gather subsequent ProjectionNodes
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    projection_nodes.emplace_back(std::static_pointer_cast<ProjectionNode>(current_node));

    DebugAssert(current_node->left_input(), "LQP invalid: ProjectionNode must have a left input.");
    DebugAssert(!current_node->right_input(), "LQP invalid: ProjectionNode can not have a right input.");

    current_node = current_node->left_input();

    if (current_node->outputs().size() > 1) {
      break;
    }
  }

  if (projection_nodes.size() > 1) {
    const auto combined_projection_node = _combine_projection_nodes(projection_nodes);
    _apply_to_inputs(combined_projection_node);
    return true;
  }

  return _apply_to_inputs(node);
}

std::shared_ptr<ProjectionNode> ProjectionCombinationRule::_combine_projection_nodes(
    std::vector<std::shared_ptr<ProjectionNode>>& projection_nodes) const {
  // Store original input and outputs
  auto left_input = projection_nodes.back()->left_input();
  const auto outputs = projection_nodes.front()->outputs();
  const auto input_sides = projection_nodes.front()->get_input_sides();

  auto column_expressions = std::vector<std::shared_ptr<LQPExpression>>();
  for (const auto expression : projection_nodes.front()->column_expressions()) {
    // If the expression is no reference to another column, we can just add it.
    if (expression->type() != ExpressionType::Column) {
      column_expressions.push_back(expression);
      continue;
    }

    // If the expression IS a reference to another column,
    // we have to check if the referenced column was created by one of the other ProjectionNodes
    // that we are combining, and then instead add the referenced column here.
    auto column_reference_replaced = false;
    auto iter =
        std::find(projection_nodes.begin(), projection_nodes.end(), expression->column_reference().original_node());
    if (iter != projection_nodes.end()) {
      column_expressions.push_back(
          (*iter)->column_expressions().at(expression->column_reference().original_column_id()));
      column_reference_replaced = true;
    }

    // If we didn't find the referenced column, it is somewhere else in the tree, so we can leave the
    // column reference in the combined ProjectionNode
    if (!column_reference_replaced) {
      column_expressions.push_back(expression);
    }
  }

  auto projection_node = std::make_shared<ProjectionNode>(column_expressions);

  // Ensure that outputs and input are chained back to the new node correctly
  left_input->outputs();
  projection_node->set_left_input(left_input);

  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], projection_node);
  }

  return projection_node;
}

}  // namespace opossum
