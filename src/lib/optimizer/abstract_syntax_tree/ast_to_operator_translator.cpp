#include "ast_to_operator_translator.hpp"

#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"

namespace opossum {

// singleton
ASTToOperatorTranslator &ASTToOperatorTranslator::get() {
  static ASTToOperatorTranslator instance;
  return instance;
}

ASTToOperatorTranslator::ASTToOperatorTranslator() {
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
  return std::make_shared<Projection>(
      input_operator, ExpressionNode::create_column_references(node->output_column_ids(), node->output_column_names()));
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
  return std::make_shared<JoinNestedLoopA>(input_left_operator, input_right_operator, join_node->join_column_ids(),
                                           join_node->scan_type(), join_node->join_mode());
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractASTNode> &input_node) const {
  const auto input_operator = translate_node(input_node->left_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(input_node);
  const auto &aggregates = aggregate_node->aggregates();

  auto aggregate_input_operator = input_operator;

  /**
   * 1. Handle arithmetic expressions in aggregate functions via Projection.
   */
  std::vector<ColumnID> expr_aliases;
  expr_aliases.reserve(aggregates.size());

  Projection::ColumnExpressions column_expressions;
  column_expressions.reserve(aggregates.size());

  // We only need a Projection before the aggregate if the function argument is an arithmetic expression.
  auto need_projection = false;

  // TODO(tim): is it better to create copies (even though they might not be needed),
  // or iterate twice (if they are needed)?
  for (const auto &aggregate : aggregates) {
    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionReference, "Expression is not a function.");

    const auto &function_arg_expr = (aggregate.expr->expression_list())[0];

    if (function_arg_expr->is_arithmetic_operator()) {
      need_projection = true;
    }

    column_expressions.emplace_back(function_arg_expr);
  }

  if (need_projection) {
    aggregate_input_operator = std::make_shared<Projection>(aggregate_input_operator, column_expressions);
  }

  /**
   * 2. Build Aggregate
   */
  std::vector<AggregateDefinition> aggregate_definitions;
  aggregate_definitions.reserve(aggregates.size());

  for (uint16_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
    const auto &aggregate = aggregates[aggregate_idx];

    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionReference,
                "Only functions are supported in Aggregates");
    const auto aggregate_function_type = aggregate_function_to_string.right.at(aggregate.expr->name());

    aggregate_definitions.emplace_back(ColumnID{aggregate_idx}, aggregate_function_type, aggregate.alias);
  }

  return std::make_shared<Aggregate>(aggregate_input_operator, aggregate_definitions,
                                     aggregate_node->groupby_columns());
}

}  // namespace opossum
