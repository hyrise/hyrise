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
    return _apply_to_children(node);
  }

  // auto tree_altered = false;
  std::vector<std::shared_ptr<ProjectionNode>> projection_nodes;

  // Gather subsequent ProjectionNodes
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    projection_nodes.emplace_back(std::static_pointer_cast<ProjectionNode>(current_node));

    // If current node has multiple or no children, end the combination here
    if (current_node->right_child() || !current_node->left_child()) {
      break;
    }

    current_node = current_node->left_child();

    if (current_node->parents().size() > 1) {
      break;
    }
  }

  if (projection_nodes.size() > 1) {
    const auto combined_projection_node = _combine_projections(projection_nodes);
    _apply_to_children(combined_projection_node);
    return true;
  }

  return _apply_to_children(node);
}

std::shared_ptr<ProjectionNode> ProjectionCombinationRule::_combine_projections(
    std::vector<std::shared_ptr<ProjectionNode>>& projections) const {
  // Store original child and parents
  auto child = projections.back()->left_child();
  const auto parents = projections.front()->parents();
  const auto child_sides = projections.front()->get_child_sides();

  auto column_expressions = std::vector<std::shared_ptr<LQPExpression>>();
  for (auto& projection : projections) {
    projection->remove_from_tree();

    for (const auto new_expression : projection->column_expressions()) {
      // Make sure to avoid duplicate Expressions from all consecutive ProjectionNodes
      if (std::find_if(column_expressions.begin(), column_expressions.end(),
                       [&new_expression](std::shared_ptr<LQPExpression> const& expression) {
                         return *expression == *new_expression;
                       }) == column_expressions.end()) {
        column_expressions.push_back(new_expression);
      }
    }
  }

  auto projection_node = std::make_shared<ProjectionNode>(column_expressions);

  // Ensure that parents and child are chained back to the new node correctly
  projection_node->set_left_child(child);

  for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
    parents[parent_idx]->set_child(child_sides[parent_idx], projection_node);
  }

  return projection_node;
}

}  // namespace opossum
