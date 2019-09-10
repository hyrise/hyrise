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

namespace {
ExpressionUnorderedSet columns_actually_used_by_node(const std::shared_ptr<AbstractLQPNode>& node) {
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

  return consumed_columns;
}

void gather_required_expressions(const std::shared_ptr<AbstractLQPNode>& node, std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  const auto additional_columns = columns_actually_used_by_node(node);
  auto& required_expressions = required_expressions_by_node[node];
  required_expressions.insert(additional_columns.begin(), additional_columns.end());

  for (const auto& input : {node->left_input(), node->right_input()}) {
    if (!input) continue;

    // Make sure the entry exists, then insert all expressions that the current node needs
    required_expressions_by_node[input];
    for (const auto& required_expression : required_expressions) {
      // Add the columns needed here (and above) if they come from the input node
      if (input->find_column_id(*required_expression)) {
        required_expressions_by_node[input].emplace(required_expression);
      }
    }

    gather_required_expressions(input, required_expressions_by_node);
  }
}

// TODO make sure that diamond is tested

}

void ColumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // TODO Doc that we do not use visit_lqp here
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;

  // Add top-level columns that need to be included as they are the actual output
  required_expressions_by_node[lqp].insert(lqp->column_expressions().begin(), lqp->column_expressions().end());

  gather_required_expressions(lqp, required_expressions_by_node);

  // Now, go through the LQP and perform all prunings. This time, it is sufficient to look at each node once.
  visit_lqp(lqp, [&](const auto& node) {
    const auto& required_expressions = required_expressions_by_node[node];
    if (node->type == LQPNodeType::StoredTable) {
      // Prune all unused columns from a StoredTableNode
      auto pruned_column_ids = std::vector<ColumnID>{};
      for (const auto& expression : node->column_expressions()) {
        if (required_expressions.find(expression) != required_expressions.end()) {
          continue;
        }

        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        pruned_column_ids.emplace_back(column_expression->column_reference.original_column_id());
      }

      auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
      stored_table_node->set_pruned_column_ids(pruned_column_ids);
    }

    if (node->type == LQPNodeType::Join) {
      auto left_input_needed = false;
      auto right_input_needed = false;

      // TODO check all outputs
      for (const auto& required_expression : required_expressions_by_node[node->outputs()[0]]) {
        if (expression_evaluable_on_lqp(required_expression, *node->left_input())) left_input_needed = true;
        if (expression_evaluable_on_lqp(required_expression, *node->right_input())) right_input_needed = true;
      }
      std::cout << node->description() << " - left " << left_input_needed << " right " << right_input_needed << std::endl;
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
