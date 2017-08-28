#include "ast_to_operator_translator.hpp"

#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node_test.hpp"

namespace opossum {

// singleton
ASTToOperatorTranslator &ASTToOperatorTranslator::get() {
  static ASTToOperatorTranslator instance;
  return instance;
}

ASTToOperatorTranslator::ASTToOperatorTranslator() {
  /**
   * Build a mapping from an ASTNodeType to a function that takes an ASTNode of such type and translates it into a set
   * of operators and returns the root of them. We prefer this over a virtual AbstractASTNode::translate() call in order
   * to keep the translation code in one place, i.e., this file.
   */

  _operator_factory[ASTNodeType::StoredTable] =
      std::bind(&ASTToOperatorTranslator::_translate_stored_table_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Predicate] =
      std::bind(&ASTToOperatorTranslator::_translate_predicate_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Projection] =
      std::bind(&ASTToOperatorTranslator::_translate_projection_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Sort] =
      std::bind(&ASTToOperatorTranslator::_translate_sort_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Join] =
      std::bind(&ASTToOperatorTranslator::_translate_join_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Aggregate] =
      std::bind(&ASTToOperatorTranslator::_translate_aggregate_node, this, std::placeholders::_1);
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::translate_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto it = _operator_factory.find(node->type());
  DebugAssert(it != _operator_factory.end(), "No factory for ASTNodeType.");
  return it->second(node);
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_stored_table_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_predicate_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_operator = translate_node(node->left_child());
  auto table_scan_node = std::dynamic_pointer_cast<PredicateNode>(node);
  return std::make_shared<TableScan>(input_operator, table_scan_node->column_id(), table_scan_node->scan_type(),
                                     table_scan_node->value(), table_scan_node->value2());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_projection_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_operator = translate_node(node->left_child());
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);
  return std::make_shared<Projection>(input_operator, projection_node->column_expressions());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_sort_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_operator = translate_node(node->left_child());
  auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  return std::make_shared<Sort>(input_operator, sort_node->column_id(), sort_node->ascending());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_join_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_left_operator = translate_node(node->left_child());
  const auto input_right_operator = translate_node(node->right_child());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

  if (join_node->join_mode() == JoinMode::Cross) {
    return std::make_shared<Product>(input_left_operator, input_right_operator);
  }

  // Forcing conversion from optional<std::string> to bool
  DebugAssert(static_cast<bool>(join_node->scan_type()), "Cannot translate Join without ScanType");
  return std::make_shared<JoinNestedLoopA>(input_left_operator, input_right_operator, join_node->join_column_ids(),
                                           *(join_node->scan_type()), join_node->join_mode());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_operator = translate_node(node->left_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  auto aggregates = aggregate_node->aggregates();
  auto groupby_columns = aggregate_node->groupby_column_ids();

  auto aggregate_input_operator = input_operator;

  /**
   * 1. Handle arithmetic expressions in aggregate functions via Projection.
   *
   * We only need a Projection before the aggregate if the function argument is an arithmetic expression.
   */
  auto need_projection = false;

  // Check if there are any arithmetic expressions.
  for (const auto &aggregate : aggregates) {
    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionIdentifier, "Expression is not a function.");

    const auto &function_arg_expr = (aggregate.expr->expression_list())[0];

    if (function_arg_expr->is_arithmetic_operator()) {
      need_projection = true;
      break;
    }
  }

  // If there are, create Projection with GROUP BY columns and arithmetic expressions.
  if (need_projection) {
    // TODO(tim): BLOCKING - these columnref exprs need the original column name.
    Projection::ColumnExpressions column_expressions = ExpressionNode::create_column_identifiers(groupby_columns);
    column_expressions.reserve(groupby_columns.size() + aggregates.size());

    // The Projection will only select columns used in the Aggregate, i.e., GROUP BY columns and expressions.
    // Unused columns are skipped – therefore, the ColumnIDs might change.
    // In fact, they will be the first columns of the Projection.
    for (uint16_t column_id = 0u; column_id < groupby_columns.size(); column_id++) {
      groupby_columns[column_id] = ColumnID{column_id};
    }

    // Counter to generate unique aliases for arithmetic expressions.
    auto current_column_id = static_cast<uint16_t>(groupby_columns.size());

    for (auto &aggregate : aggregates) {
      DebugAssert(aggregate.expr->type() == ExpressionType::FunctionIdentifier, "Expression is not a function.");

      // Add original expression of the function to the Projection.
      column_expressions.emplace_back((aggregate.expr->expression_list())[0]);

      // Create a ColumnReference expression for the column id of the Projection.
      const auto column_ref_expr = ExpressionNode::create_column_identifier(ColumnID{current_column_id});
      current_column_id++;

      // Change the expression list of the expression representing the aggregate.
      aggregate.expr->set_expression_list({column_ref_expr});
    }

    aggregate_input_operator = std::make_shared<Projection>(aggregate_input_operator, column_expressions);
  }

  /**
   * 2. Build Aggregate
   */
  std::vector<AggregateDefinition> aggregate_definitions;
  aggregate_definitions.reserve(aggregates.size());

  for (const auto &aggregate : aggregates) {
    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionIdentifier,
                "Only functions are supported in Aggregates");

    const auto aggregate_function_type = aggregate_function_to_string.right.at(aggregate.expr->name());
    const auto column_id = (aggregate.expr->expression_list())[0]->column_id();
    aggregate_definitions.emplace_back(column_id, aggregate_function_type, aggregate.alias);
  }

  return std::make_shared<Aggregate>(aggregate_input_operator, aggregate_definitions, groupby_columns);
}

}  // namespace opossum
