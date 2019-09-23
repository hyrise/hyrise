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
void gather_expressions_not_computed_by_expression_evaluator(
    const std::shared_ptr<AbstractExpression>& expression,
    const std::vector<std::shared_ptr<AbstractExpression>>& input_expressions,
    ExpressionUnorderedSet& required_expressions, const bool top_level = true) {
  if (std::find_if(input_expressions.begin(), input_expressions.end(),
                   [&expression](const auto& other) { return *expression == *other; }) != input_expressions.end()) {
    // This expression has already been computed and does not need to be computed again. Only add it if it is not a
    // top-level (i.e., forwarded) expression
    if (!top_level) required_expressions.emplace(expression);
    return;
  }

  if (expression->type == ExpressionType::Aggregate || expression->type == ExpressionType::LQPColumn) {
    // These cannot be computed by the expression evaluator and are expected to be provided by the input
    required_expressions.emplace(expression);
    return;
  }

  for (const auto& argument : expression->arguments) {
    gather_expressions_not_computed_by_expression_evaluator(argument, input_expressions, required_expressions, false);
  }
}

ExpressionUnorderedSet gather_required_expressions(const std::shared_ptr<AbstractLQPNode>& node) {
  // Gathers all expressions required by THIS node, i.e., expressions needed by the node to do its job. For example, a
  // PredicateNode `a < 3` requires the LQPColumn a.
  auto required_expressions = ExpressionUnorderedSet{};

  switch (node->type) {
    // For the vast majority of node types, AbstractLQPNode::node_expression holds all expressions required by this
    // node.
    case LQPNodeType::Alias:
    case LQPNodeType::CreateTable:
    case LQPNodeType::CreatePreparedPlan:
    case LQPNodeType::CreateView:
    case LQPNodeType::DropView:
    case LQPNodeType::DropTable:
    case LQPNodeType::DummyTable:
    case LQPNodeType::Limit:
    case LQPNodeType::Root:
    case LQPNodeType::Sort:
    case LQPNodeType::StaticTable:
    case LQPNodeType::StoredTable:
    case LQPNodeType::Union:
    case LQPNodeType::Validate:
    case LQPNodeType::Mock: {
      for (const auto& expression : node->node_expressions) {
        required_expressions.emplace(expression);
      }
    } break;

    // For aggregate nodes, we need the group by columns and the arguments to the aggregate functions
    case LQPNodeType::Aggregate: {
      const auto& aggregate_node = static_cast<AggregateNode&>(*node);

      // Tracks whether we have already added a column. For COUNT(*) without GROUP BY, we might otherwise end up
      // without any expressions.
      auto has_at_least_one_expression = false;

      for (auto expression_idx = size_t{0}; expression_idx < node->node_expressions.size(); ++expression_idx) {
        const auto& expression = node->node_expressions[expression_idx];

        if (expression_idx < aggregate_node.aggregate_expressions_begin_idx) {
          // This is a group by expression that is required from the input
          required_expressions.emplace(expression);
          has_at_least_one_expression = true;
        } else {
          // This is an aggregate expression - we need its argument
          DebugAssert(expression->type == ExpressionType::Aggregate, "Expected AggregateExpression");
          if (expression->arguments.empty()) {
            // Argument is empty (i.e., COUNT(*)).
            continue;
          }

          required_expressions.emplace(expression->arguments[0]);

          has_at_least_one_expression = true;
        }
      }

      if (!has_at_least_one_expression) {
        // Unsuccessful - add the first expression
        required_expressions.emplace(node->left_input()->column_expressions()[0]);
      }
    } break;

    // For ProjectionNodes, collect all expressions that
    //   (1) were already computed and are re-used as arguments in this projection
    //   (2) cannot be computed (i.e., Aggregate and LQPColumn inputs)
    // As PredicateNodes use the ExpressionEvaluator, they have the same requirements.
    case LQPNodeType::Predicate:
    case LQPNodeType::Projection: {
      for (const auto& expression : node->node_expressions) {
        gather_expressions_not_computed_by_expression_evaluator(expression, node->left_input()->column_expressions(),
                                                                required_expressions);
      }
    } break;

    // For Joins, collect the expressions used on the left and right sides of the join expressions
    case LQPNodeType::Join: {
      const auto& join_node = static_cast<JoinNode&>(*node);
      for (const auto& predicate : join_node.join_predicates()) {
        DebugAssert(predicate->type == ExpressionType::Predicate && predicate->arguments.size() == 2,
                    "Expected binary predicate for join");
        required_expressions.emplace(predicate->arguments[0]);
        required_expressions.emplace(predicate->arguments[1]);
      }
    } break;

    // No pruning of the input columns to Delete, Update and Insert, they need them all.
    case LQPNodeType::Delete:
    case LQPNodeType::Insert:
    case LQPNodeType::Update: {
      const auto& left_input_expressions = node->left_input()->column_expressions();
      required_expressions.insert(left_input_expressions.begin(), left_input_expressions.end());

      if (node->right_input()) {
        const auto& right_input_expressions = node->right_input()->column_expressions();
        required_expressions.insert(right_input_expressions.begin(), right_input_expressions.end());
      }
    } break;
  }

  return required_expressions;
}

void recursive_gather_required_expressions(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t>& outputs_visited_by_node) {
  const auto additional_columns = gather_required_expressions(node);
  auto& required_expressions = required_expressions_by_node[node];
  required_expressions.insert(additional_columns.begin(), additional_columns.end());

  ++outputs_visited_by_node[node];
  if (outputs_visited_by_node[node] < node->output_count()) return;

  // Once all nodes that may require columns from this node (i.e., this node's outputs) have been visited, we can
  // recurse into this node's inputs.
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

    recursive_gather_required_expressions(input, required_expressions_by_node, outputs_visited_by_node);
  }
}

void prune_join_node(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  // Sometimes, joins are not actually used to combine data but only to check the existence of a tuple in a second
  // table. Example: SELECT c_name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'GERMANY'
  // These joins will be rewritten to semi joins. However, we can only do this if the join is on a unique/primary
  // key column as non-unique joins could possibly emit a matching line more than once.

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  if (join_node->join_mode != JoinMode::Inner) return;

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
      const auto& stored_table_node =
          std::dynamic_pointer_cast<const StoredTableNode>(column_reference.original_node());
      if (!stored_table_node) return false;

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      for (const auto& table_constraint : table->get_soft_unique_constraints()) {
        // This currently does not handle multi-column constraints, but that should be easy to add once needed.
        if (table_constraint.columns.size() > 1) continue;
        if (table_constraint.columns[0] == column_reference.original_column_id()) {
          return true;
        }
      }
      return false;
    };

    const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
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

void prune_projection_node(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) {
  // Iterate over the ProjectionNode's expressions and add them to the pruned expressions if at least one output node
  // requires them
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);

  auto new_node_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  new_node_expressions.reserve(projection_node->node_expressions.size());

  for (auto expression_iter = projection_node->node_expressions.begin();
       expression_iter != projection_node->node_expressions.end(); ++expression_iter) {
    for (const auto& output : node->outputs()) {
      const auto& required_expressions = required_expressions_by_node[output];
      if (std::find_if(required_expressions.begin(), required_expressions.end(), [&expression_iter](const auto& other) {
            return **expression_iter == *other;
          }) != required_expressions.end()) {
        new_node_expressions.emplace_back(*expression_iter);
        break;
      }
    }
  }

  projection_node->node_expressions = new_node_expressions;
}

}  // namespace

void ColumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // For each node, required_expressions_by_node will hold the expressions either needed by this node or by one of its
  // successors (i.e., nodes to which this node is an input). After collecting this information, we walk through all
  // identified nodes and perform the pruning.
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;

  // Add top-level columns that need to be included as they are the actual output
  required_expressions_by_node[lqp].insert(lqp->column_expressions().begin(), lqp->column_expressions().end());

  // Recursively walk through the LQP. We cannot use visit_lqp as we explicitly need to take each path through the LQP.
  // The right side of a diamond might require additional columns - if we only visited each node once, we might miss
  // those. However, we track how many of a node's outputs we have already visited and recurse only once we have seen
  // all of them. That way, the performance should be similar to that of visit_lqp.
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> outputs_visited_by_node;
  recursive_gather_required_expressions(lqp, required_expressions_by_node, outputs_visited_by_node);

  // Now, go through the LQP and perform all prunings. This time, it is sufficient to look at each node once.
  for (const auto& [node, required_expressions] : required_expressions_by_node) {
    switch (node->type) {
      case LQPNodeType::StoredTable: {
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

        if (pruned_column_ids.size() == stored_table_node->column_expressions().size()) {
          // Cannot prune all columns
          pruned_column_ids.resize(pruned_column_ids.size() - 1);
        }

        stored_table_node->set_pruned_column_ids(pruned_column_ids);
      } break;

      case LQPNodeType::Join: {
        prune_join_node(node, required_expressions_by_node);
      } break;

      case LQPNodeType::Projection: {
        prune_projection_node(node, required_expressions_by_node);
      } break;

      default:
        break;  // Node cannot be pruned
    }
  }
}

}  // namespace opossum
