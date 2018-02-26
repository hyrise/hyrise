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

  std::vector<std::shared_ptr<ProjectionNode>> projection_nodes;

  // Gather subsequent ProjectionNodes
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    projection_nodes.emplace_back(std::static_pointer_cast<ProjectionNode>(current_node));

    DebugAssert(current_node->left_child(), "LQP invalid: ProjectionNode must have a left child.");
    DebugAssert(!current_node->right_child(), "LQP invalid: ProjectionNode can not have a right child.");

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
  auto left_child = projections.back()->left_child();
  const auto parents = projections.front()->parents();
  const auto child_sides = projections.front()->get_child_sides();

  auto column_expressions = std::vector<std::shared_ptr<LQPExpression>>();
  for (const auto expression : projections.front()->column_expressions()) {
    // If the expression is no reference to another column, we can just add it.
    if (expression->type() != ExpressionType::Column) {
      column_expressions.push_back(expression);
      continue;
    }

    // If the expression IS a reference to another column,
    // we have to check if the referenced column is part of one of the other ProjectionNodes that we are combining,
    // and then instead add the referenced column here.
    auto column_reference_replaced = false;
    auto iter = projections.begin() + 1;
    while (!column_reference_replaced && iter != projections.end()) {
      const auto column_references = (*iter)->output_column_references();
      for (auto column_id = ColumnID{0}; column_id < column_references.size(); ++column_id) {
        if (expression->column_reference() == column_references.at(column_id)) {
          column_expressions.push_back((*iter)->column_expressions().at(column_id));
          column_reference_replaced = true;
          break;
        }
      }
      ++iter;
    }

    // If we didn't find the referenced column, it is somewhere else in the tree, so we can leave the
    // column reference in the combined ProjectionNode
    if (!column_reference_replaced) {
      column_expressions.push_back(expression);
    }
  }

  auto projection_node = std::make_shared<ProjectionNode>(column_expressions);

  // Ensure that parents and child are chained back to the new node correctly
  left_child->clear_parents();
  projection_node->set_left_child(left_child);

  for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
    parents[parent_idx]->set_child(child_sides[parent_idx], projection_node);
  }

  return projection_node;
}

}  // namespace opossum
