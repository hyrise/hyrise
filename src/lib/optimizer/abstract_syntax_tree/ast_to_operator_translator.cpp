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
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "projection_node.hpp"

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
  return std::make_shared<TableScan>(input_operator, table_scan_node->column_name(), table_scan_node->scan_type(),
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
  return std::make_shared<Sort>(input_operator, sort_node->column_name(), sort_node->ascending());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_join_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_left_operator = translate_node(node->left_child());
  const auto input_right_operator = translate_node(node->right_child());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

  if (join_node->join_mode() == JoinMode::Cross) {
    return std::make_shared<Product>(input_left_operator, input_right_operator, join_node->prefix_left(),
                                     join_node->prefix_right());
  }

  // Forcing conversion from optional<std::string> to bool
  DebugAssert(static_cast<bool>(join_node->scan_type()), "Cannot translate Join without ScanType");
  return std::make_shared<JoinNestedLoopA>(input_left_operator, input_right_operator, join_node->join_column_names(),
                                           *(join_node->scan_type()), join_node->join_mode(), join_node->prefix_left(),
                                           join_node->prefix_right());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_operator = translate_node(node->left_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  const auto &aggregates = aggregate_node->aggregates();

  auto out_operator = input_operator;

  /**
   * 1. Handle arithmetic expressions in aggregate functions via Projection.
   * Supports only one level of arithmetics, i.e. SUM(a*b) is fine, but SUM(a*b+c) is not.
   *
   * In Hyrise, only Projections are supposed to be able to handle arithmetic expressions.
   * Therefore, if we encounter an expression within an aggregate function, we have to execute
   * one or multiple Projections first. The Aggregate will work with the output columns of that Projection.
   */
  std::vector<std::string> expr_aliases;
  expr_aliases.reserve(aggregates.size());

  Projection::ColumnExpressions expressions;

  // We only need a Projection before the aggregate if the function arg is an arithmetic expr.
  auto needs_projection = false;

  for (const auto &aggregate : aggregates) {
    const auto &expr = aggregate.expr;
    DebugAssert(expr->type() == ExpressionType::FunctionIdentifier, "Expression is not a function.");

    const auto &function_arg_expr = (expr->expression_list())[0];

    if (function_arg_expr->is_operand()) {
      expr_aliases.emplace_back(function_arg_expr->name());
    } else if (function_arg_expr->is_arithmetic_operator()) {
      needs_projection = true;

      // TODO(mp): Support more complex expressions.
      DebugAssert(function_arg_expr->left_child()->is_operand(), "Left child is not a literal or column ref.");
      DebugAssert(function_arg_expr->right_child()->is_operand(), "Right child is not a literal or column ref.");

      // Generate a temporary column name for the expression.
      // Make sure that the generated column name does not exist in the input.
      auto alias = function_arg_expr->to_expression_string();
      DebugAssert(!node->left_child()->has_output_column(alias), "Expression String is already used as ColumnName");

      expressions.emplace_back(function_arg_expr);
      expr_aliases.emplace_back(alias);
    } else {
      Fail("Expression is neither operand nor arithmetic expression.");
    }
  }

  if (needs_projection) {
    out_operator = std::make_shared<Projection>(out_operator, expressions);
  }

  /**
   * 2. Build Aggregate
   */
  std::vector<AggregateDefinition> aggregate_definitions;
  aggregate_definitions.reserve(aggregates.size());
  for (size_t aggregate_idx = 0; aggregate_idx < aggregates.size(); ++aggregate_idx) {
    const auto &aggregate = aggregates[aggregate_idx];

    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionIdentifier,
                "Only functions are supported in Aggregates");
    const auto aggregate_function_type = aggregate_function_to_string.right.at(aggregate.expr->name());

    aggregate_definitions.emplace_back(expr_aliases[aggregate_idx], aggregate_function_type, aggregate.alias);
  }
  out_operator = std::make_shared<Aggregate>(out_operator, aggregate_definitions, aggregate_node->groupby_columns());

  return out_operator;
}

}  // namespace opossum
