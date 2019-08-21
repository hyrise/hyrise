#include "column_pruning_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/update_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

void ColumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Collect the columns that are used in expressions somewhere in the LQP.
  // This EXCLUDES columns that are merely forwarded by Projections throughout the LQP
  auto actually_used_columns = _collect_actually_used_columns(lqp);

  // The output columns of the plan are always considered to be referenced (i.e., we cannot prune them)
  const auto output_columns = lqp->column_expressions();
  actually_used_columns.insert(output_columns.begin(), output_columns.end());

  // Search for ProjectionNodes that forward the unused columns and prune them accordingly
  _prune_columns_in_projections(lqp, actually_used_columns);

  // Search the plan for leaf nodes and prune all columns from them that are not referenced
  _prune_columns_from_leaves(lqp, actually_used_columns);
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

  // Search the entire LQP for columns used in AbstractLQPNode::node_expressions, i.e. columns that are necessary for
  // the "functioning" of the LQP.
  visit_lqp(lqp, [&](const auto& node) {
    switch (node->type) {
      // For the vast majority of node types, recursing through AbstractLQPNode::node_expression
      // correctly yields all Columns used by this node.
      case LQPNodeType::Aggregate:
      case LQPNodeType::Alias:
      case LQPNodeType::CreateTable:
      case LQPNodeType::CreatePreparedPlan:
      case LQPNodeType::CreateView:
      case LQPNodeType::DropView:
      case LQPNodeType::DropTable:
      case LQPNodeType::DummyTable:
      case LQPNodeType::Join:
      case LQPNodeType::Limit:
      case LQPNodeType::Predicate:
      case LQPNodeType::Root:
      case LQPNodeType::Sort:
      case LQPNodeType::StaticTable:
      case LQPNodeType::StoredTable:
      case LQPNodeType::Union:
      case LQPNodeType::Validate:
      case LQPNodeType::Mock: {
        for (const auto& expression : node->node_expressions) {
          collect_consumed_columns_from_expression(expression);
        }
      } break;

      // For ProjectionNodes, ignore forwarded columns (since they would include all columns and we wouldn't be able to
      // prune) by only searching the arguments of expression.
      case LQPNodeType::Projection: {
        for (const auto& expression : node->node_expressions) {
          for (const auto& argument : expression->arguments) {
            collect_consumed_columns_from_expression(argument);
          }
        }
      } break;

      // No pruning of the input columns to Delete, Update and Insert, they need them all.
      case LQPNodeType::Delete:
      case LQPNodeType::Insert:
      case LQPNodeType::Update: {
        const auto& left_input_expressions = node->left_input()->column_expressions();
        consumed_columns.insert(left_input_expressions.begin(), left_input_expressions.end());

        if (node->right_input()) {
          const auto& right_input_expressions = node->right_input()->column_expressions();
          consumed_columns.insert(right_input_expressions.begin(), right_input_expressions.end());
        }
      } break;
    }

    return LQPVisitation::VisitInputs;
  });

  return consumed_columns;
}

void ColumnPruningRule::_prune_columns_from_leaves(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                   const ExpressionUnorderedSet& referenced_columns) {
  // Collect all parents of leaves and on which input side their leave is
  // (if a node has two leaves as inputs, it will be collected twice)
  auto leaf_parents = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, LQPInputSide>>{};
  visit_lqp(lqp, [&](auto& node) {
    // Early out for non-leaves
    if (node->input_count() != 0) {
      return LQPVisitation::VisitInputs;
    }

    auto pruned_column_ids = std::vector<ColumnID>{};
    for (const auto& expression : node->column_expressions()) {
      if (referenced_columns.find(expression) != referenced_columns.end()) {
        continue;
      }
      const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      pruned_column_ids.emplace_back(column_expression->column_reference.original_column_id());
    }

    // We cannot create a Table without columns - since Chunks rely on their first column to determine their row count
    if (pruned_column_ids.size() == node->column_expressions().size() && !pruned_column_ids.empty()) {
      pruned_column_ids.pop_back();
    }

    if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node)) {
      stored_table_node->set_pruned_column_ids(pruned_column_ids);
    } else if (const auto mock_node = std::dynamic_pointer_cast<MockNode>(node)) {
      mock_node->set_pruned_column_ids(pruned_column_ids);
    } else {
      // We don't know how to prune columns from this leaf-node type (CreateViewNode, etc.), so do nothing
    }

    return LQPVisitation::VisitInputs;
  });
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
    for (const auto& expression : projection_node->node_expressions) {
      // We keep all non-column expressions
      if (expression->type != ExpressionType::LQPColumn) {
        referenced_projection_expressions.emplace_back(expression);
      } else if (referenced_columns.find(expression) != referenced_columns.end()) {
        referenced_projection_expressions.emplace_back(expression);
      }
    }

    if (projection_node->node_expressions.size() == referenced_projection_expressions.size()) {
      // No columns to prune
      continue;
    }

    // We cannot have a ProjectionNode that outputs no columns
    if (referenced_projection_expressions.empty()) {
      lqp_remove_node(projection_node);
    } else {
      projection_node->node_expressions = referenced_projection_expressions;
    }
  }
}

}  // namespace opossum
