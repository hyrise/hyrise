#include "column_pruning_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
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

void prune_join_node(const std::shared_ptr<AbstractLQPNode>& node, std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  // Sometimes, joins are not actually used to combine data but only to check the existence of a tuple in a second
  // table. Example: SELECT c_name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'GERMANY'
  // These joins will be rewritten to semi joins. However, we can only do this if the join is on a unique/primary
  // key column as non-unique joins could possibly emit a matching line more than once.

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  if (join_node->join_mode != JoinMode::Inner) return;  // TODO test this

  // Check whether the left/right inputs are actually needed by following operators
  auto left_input_is_used = false;
  auto right_input_is_used = false;
  for (const auto& output : node->outputs()) {
    for (const auto& required_expression : required_expressions_by_node[output]) {
      if (expression_evaluable_on_lqp(required_expression, *node->left_input())) left_input_is_used = true;
      if (expression_evaluable_on_lqp(required_expression, *node->right_input())) right_input_is_used = true;
    }
  }
  DebugAssert(left_input_is_used || right_input_is_used, "Did not expect a useless join");
  if (left_input_is_used && right_input_is_used) return;

  // Check whether the join predicates operate on unique columns.
  auto left_joins_on_unique_column = false;
  auto right_joins_on_unique_column = false;

  const auto& join_predicates = join_node->join_predicates();
  for (const auto& join_predicate : join_predicates) {
    const auto is_unique_column = [](const auto& expression) {
      const auto& column = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      if (!column) return false;

      const auto& column_reference = column->column_reference;
      const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column_reference.original_node());
      if (!stored_table_node) return false;

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      for (const auto& table_constraint : table->get_unique_constraints()) {
        // This currently does not handle multi-column constraints, but that should be easy to add once needed.
        if (table_constraint.columns.size() > 1) continue;
        if (table_constraint.columns[0] == column_reference.original_column_id()) {
          return true;
        }
      }
      return false;
    };

    const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
    if (predicate->predicate_condition != PredicateCondition::Equals) return;
    const auto& left_operand = predicate->left_operand();
    const auto& right_operand = predicate->right_operand();

    if (!left_joins_on_unique_column) {
      left_joins_on_unique_column = is_unique_column(left_operand);
    }
    if (!right_joins_on_unique_column) {
      right_joins_on_unique_column = is_unique_column(right_operand);
    }
  }

  if (!left_input_is_used && left_joins_on_unique_column) {
    join_node->join_mode = JoinMode::Semi;
    const auto temp = join_node->left_input();
    join_node->set_left_input(join_node->right_input());
    join_node->set_right_input(temp);
  }

  if (!right_input_is_used && right_joins_on_unique_column) {
    join_node->join_mode = JoinMode::Semi;
  }
}

// TODO make sure that diamond is tested

}

void ColumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Pruning happens in two steps: In a first walk through the LQP, we identify the expressions needed by each node.
  // Next, we walk through all identified nodes and perform the pruning.
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;

  // Add top-level columns that need to be included as they are the actual output
  required_expressions_by_node[lqp].insert(lqp->column_expressions().begin(), lqp->column_expressions().end());

  // Recursively walk through the LQP. We cannot use visit_lqp as we explicitly need to take each path through the LQP.
  // The right side of a diamond might require additional columns - if we only visited each node once, we might miss
  // those. However, if this every becomes a performance issue, we might come up with a better traversal algorithm.
  gather_required_expressions(lqp, required_expressions_by_node);

  // Now, go through the LQP and perform all prunings. This time, it is sufficient to look at each node once.
  for (const auto& [node, required_expressions] : required_expressions_by_node) {
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
      prune_join_node(node, required_expressions_by_node);
    }
  }
}

}  // namespace opossum
