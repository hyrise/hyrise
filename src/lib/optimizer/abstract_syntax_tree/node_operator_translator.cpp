#include "node_operator_translator.hpp"

#include <memory>

#include "constant_mappings.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"

namespace opossum {

// singleton
NodeOperatorTranslator &NodeOperatorTranslator::get() {
  static NodeOperatorTranslator instance;
  return instance;
}

NodeOperatorTranslator::NodeOperatorTranslator() {
  _operator_factory[AstNodeType::Table] =
      std::bind(&NodeOperatorTranslator::translate_table_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Predicate] =
      std::bind(&NodeOperatorTranslator::translate_table_scan_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Projection] =
      std::bind(&NodeOperatorTranslator::translate_projection_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Sort] =
      std::bind(&NodeOperatorTranslator::translate_order_by_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Aggregate] =
      std::bind(&NodeOperatorTranslator::translate_aggregate_node, this, std::placeholders::_1);
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_node(std::shared_ptr<AbstractAstNode> node) const {
  auto it = _operator_factory.find(node->type());

  Assert(it != _operator_factory.end(), "No factory for AstNodeType.");

  return it->second(node);
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto table_node = std::dynamic_pointer_cast<TableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_scan_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());
  auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  return std::make_shared<TableScan>(input_operator, predicate_node->column_name(), predicate_node->scan_type(),
                                     predicate_node->value(), predicate_node->value2());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_projection_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());
  return std::make_shared<Projection>(input_operator, node->output_columns());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_order_by_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());

  auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  return std::make_shared<Sort>(input_operator, sort_node->column_name(), sort_node->asc());
}

std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_aggregate_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  const auto &aggregates = aggregate_node->aggregates();

  std::shared_ptr<AbstractOperator> out_operator = input_operator;

  /**
   * Handle arithmetic expressions in aggregate functions via Projection. Support only one level
   * of arithmetics, i.e. SUM(a*b) is fine SUM(a*b+c) is not
   */
  std::vector<std::string> expr_aliases;
  expr_aliases.reserve(aggregates.size());

  Projection::ProjectionDefinitions definitions;
  definitions.reserve(aggregates.size());

  auto alias_index = 0;

  for (const auto &aggregate : aggregates) {
    const auto &expr = aggregate.expr;
    Assert(expr->type() == ExpressionType::FunctionReference, "Expression is not a function.");

    const auto &arithmetic_expr = std::dynamic_pointer_cast<ExpressionNode>(expr->expression_list()->at(0));
    Assert(static_cast<bool>(arithmetic_expr), "First item of expression_list is not an expression.");

    Assert(arithmetic_expr->is_arithmetic(), "Expression is not an arithmetic expression.");

    auto left_operand = std::dynamic_pointer_cast<ExpressionNode>(arithmetic_expr->left());
    Assert(static_cast<bool>(left_operand), "Left child of arithmetic expression is not an expression.");
    auto right_operand = std::dynamic_pointer_cast<ExpressionNode>(arithmetic_expr->right());
    Assert(static_cast<bool>(right_operand), "Right child of arithmetic expression is not an expression.");

    Assert(left_operand->type() == ExpressionType::Literal || left_operand->type() == ExpressionType::ColumnReference,
           "Left child is not a literal or column ref.");
    Assert(right_operand->type() == ExpressionType::Literal || right_operand->type() == ExpressionType::ColumnReference,
           "Right child is not a literal or column ref.");

    auto alias = "alias" + std::to_string(alias_index);
    alias_index++;

    definitions.emplace_back(arithmetic_expr->to_expression_string(), "float", alias);
    expr_aliases.emplace_back(alias);
  }

  out_operator = std::make_shared<Projection>(out_operator, definitions);

  /**
   * Build Aggregate
   */
  std::vector<AggregateDefinition> aggregate_definitions;
  aggregate_definitions.reserve(aggregates.size());
  for (size_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
    const auto &aggregate = aggregates[aggregate_idx];

    Assert(aggregate.expr->type() == ExpressionType::FunctionReference, "Only functions are supported in Aggregates");
    const auto aggregate_function = string_to_aggregate_function.at(aggregate.expr->name());

    aggregate_definitions.emplace_back(expr_aliases[aggregate_idx], aggregate_function, aggregate.alias);
  }
  out_operator = std::make_shared<Aggregate>(out_operator, aggregate_definitions, std::vector<std::string>());

  return out_operator;
}

}  // namespace opossum
