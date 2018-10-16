#include "column_pruning_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string ColumnPruningRule::name() const { return "Column Pruning Rule"; }

bool ColumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Collect the columns that are used in expressions somewhere in the LQP.
  // This EXCLUDES columns that are merely forwarded by Projections throughout the LQP
  auto actually_used_columns = _collect_actually_used_columns(lqp);

  // The output columns of the plan are always considered to be referenced (i.e., we cannot prune them)
  const auto output_columns = lqp->column_expressions();
  actually_used_columns.insert(output_columns.begin(), output_columns.end());

  // Search for ProjectionNodes that forward the unused columns and prune them accordingly
  _prune_columns_in_projections(lqp, actually_used_columns);

  // Search the plan for leaf nodes and prune all columns from them that are not referenced
  return _prune_columns_from_leaves(lqp, actually_used_columns);
}

ExpressionUnorderedSet ColumnPruningRule::_collect_actually_used_columns(const std::shared_ptr<AbstractLQPNode>& lqp) {
  auto consumed_columns = ExpressionUnorderedSet{};

  // Search an expression for referenced columns
  const auto collect_consumed_columns_from_expression = [&](const auto& expression) {
    visit_expression(expression, [&](const auto& sub_expression) {
      if (sub_expression->type == ExpressionType::LQPColumn) {
        consumed_columns.emplace(sub_expression);
      }
      return ExpressionVisitation::VisitArguments;
    });
  };

  // Search the entire LQP for columns used in AbstractLQPNode::node_expressions(), i.e. columns that are necessary for
  // the "functioning" of the LQP.
  // For ProjectionNodes, ignore forwarded columns (since they would include all columns and we wouldn't be able to
  // prune) by only searching the arguments of expression.
  visit_lqp(lqp, [&](const auto& node) {
    for (const auto& expression : node->node_expressions()) {
      if (node->type == LQPNodeType::Projection) {
        for (const auto& argument : expression->arguments) {
          collect_consumed_columns_from_expression(argument);
        }
      } else {
        collect_consumed_columns_from_expression(expression);
      }
    }
    return LQPVisitation::VisitInputs;
  });

  return consumed_columns;
}

bool ColumnPruningRule::_prune_columns_from_leaves(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                   const ExpressionUnorderedSet& referenced_columns) {
  auto lqp_changed = false;

  // Collect all parents of leaves and on which input side their leave is
  // (if a node has two leaves as inputs, it will be collected twice)
  auto leaf_parents = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, LQPInputSide>>{};
  visit_lqp(lqp, [&](auto& node) {
    for (const auto input_side : {LQPInputSide::Left, LQPInputSide::Right}) {
      const auto input = node->input(input_side);
      if (input && input->input_count() == 0) leaf_parents.emplace_back(node, input_side);
    }

    // Do not visit the ProjectionNode we may have just inserted, that would lead to infinite recursion
    return LQPVisitation::VisitInputs;
  });

  // Insert ProjectionNodes that prune unused columns between the leaves and their parents
  for (const auto& parent_and_leaf_input_side : leaf_parents) {
    const auto& parent = parent_and_leaf_input_side.first;
    const auto& leaf_input_side = parent_and_leaf_input_side.second;
    const auto leaf = parent->input(leaf_input_side);

    // Collect all columns from the leaf that are actually referenced
    auto referenced_leaf_columns = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (const auto& expression : leaf->column_expressions()) {
      if (referenced_columns.find(expression) != referenced_columns.end()) {
        referenced_leaf_columns.emplace_back(expression);
      }
    }

    if (leaf->column_expressions().size() == referenced_leaf_columns.size()) continue;

    // We cannot have a ProjectionNode that outputs no columns, so let's avoid that
    if (referenced_leaf_columns.empty()) continue;

    // If a leaf outputs columns that are never used, prune those columns by inserting a ProjectionNode that only
    // contains the used columns
    lqp_insert_node(parent, leaf_input_side, ProjectionNode::make(referenced_leaf_columns));
    lqp_changed = true;
  }

  return lqp_changed;
}

void ColumnPruningRule::_prune_columns_in_projections(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                      const ExpressionUnorderedSet& referenced_columns) {
  /**
   * Prune otherwise unused columns that are forwarded by ProjectionNodes
   */

  // First collect all the ProjectionNodes. Don't prune while visiting because visit_lqp() can't deal with nodes being
  // replaced
  auto projection_nodes = std::vector<std::shared_ptr<ProjectionNode>>{};
  visit_lqp(lqp, [&](auto& node) {
    if (node->type != LQPNodeType::Projection) return LQPVisitation::VisitInputs;
    projection_nodes.emplace_back(std::static_pointer_cast<ProjectionNode>(node));
    return LQPVisitation::VisitInputs;
  });

  // Replace ProjectionNodes with pruned ProjectionNodes if necessary
  for (const auto& projection_node : projection_nodes) {
    auto referenced_projection_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (const auto& expression : projection_node->node_expressions()) {
      // We keep all non-column expressions
      if (expression->type != ExpressionType::LQPColumn) {
        referenced_projection_expressions.emplace_back(expression);
      } else if (referenced_columns.find(expression) != referenced_columns.end()) {
        referenced_projection_expressions.emplace_back(expression);
      }
    }

    if (projection_node->node_expressions().size() == referenced_projection_expressions.size()) {
      // No columns to prune
      continue;
    }

    // We cannot have a ProjectionNode that outputs no columns
    if (referenced_projection_expressions.empty()) {
      lqp_remove_node(projection_node);
    } else {
      projection_node->expressions = referenced_projection_expressions;
    }
  }
}

}  // namespace opossum
