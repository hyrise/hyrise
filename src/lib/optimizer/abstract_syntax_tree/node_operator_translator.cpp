#include "node_operator_translator.hpp"

#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
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
NodeOperatorTranslator &NodeOperatorTranslator::get() {
  static NodeOperatorTranslator instance;
  return instance;
}

NodeOperatorTranslator::NodeOperatorTranslator() {
  _operator_factory[ASTNodeType::StoredTable] =
      std::bind(&NodeOperatorTranslator::translate_table_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Predicate] =
      std::bind(&NodeOperatorTranslator::translate_table_scan_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Projection] =
      std::bind(&NodeOperatorTranslator::translate_projection_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Sort] =
      std::bind(&NodeOperatorTranslator::translate_order_by_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Join] =
      std::bind(&NodeOperatorTranslator::translate_join_node, this, std::placeholders::_1);
  _operator_factory[ASTNodeType::Aggregate] =
      std::bind(&NodeOperatorTranslator::translate_aggregate_node, this, std::placeholders::_1);
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto it = _operator_factory.find(node->type());

  if (it == _operator_factory.end()) {
    throw std::runtime_error("No factory for ASTNodeType.");
  }

  return it->second(node);
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_scan_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto input_operator = translate_node(node->left_child());
  auto table_scan_node = std::dynamic_pointer_cast<PredicateNode>(node);
  return std::make_shared<TableScan>(input_operator, table_scan_node->column_name(), table_scan_node->scan_type(),
                                     table_scan_node->value(), table_scan_node->value2());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_projection_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto input_operator = translate_node(node->left_child());
  return std::make_shared<Projection>(input_operator, node->output_column_names());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_order_by_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto input_operator = translate_node(node->left_child());
  auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  return std::make_shared<Sort>(input_operator, sort_node->column_name(), sort_node->ascending());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_join_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto input_left_operator = translate_node(node->left_child());
  auto input_right_operator = translate_node(node->right_child());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  return std::make_shared<JoinHash>(input_left_operator, input_right_operator, join_node->join_column_names(),
                                    join_node->scan_type(), join_node->join_mode(), join_node->prefix_left(),
                                    join_node->prefix_right());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_aggregate_node(
    std::shared_ptr<AbstractASTNode> node) const {
  auto input_operator = translate_node(node->left_child());

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

    auto left_operand = std::dynamic_pointer_cast<ExpressionNode>(arithmetic_expr->left_child());
    Assert(static_cast<bool>(left_operand), "Left child of arithmetic expression is not an expression.");
    auto right_operand = std::dynamic_pointer_cast<ExpressionNode>(arithmetic_expr->right_child());
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
