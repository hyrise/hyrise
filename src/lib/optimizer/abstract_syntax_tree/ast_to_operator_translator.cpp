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
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"

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
  const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  auto input_operator = translate_node(node->left_child());

  /**
   * Go through all the order descriptions and create a sort operator for each of them.
   * Iterate in reverse because the sort operator does not support multiple columns, and instead relies on stable sort.
   * We therefore sort by the n+1-th column before sorting by the n-th column.
   */
  std::shared_ptr<AbstractOperator> result_operator;
  const auto &definitions = sort_node->order_by_definitions();
  for (auto it = definitions.rbegin(); it != definitions.rend(); it++) {
    const auto &definition = *it;
    result_operator = std::make_shared<Sort>(input_operator, definition.column_id, definition.order_by_mode);
    input_operator = result_operator;
  }

  return result_operator;
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_join_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_left_operator = translate_node(node->left_child());
  const auto input_right_operator = translate_node(node->right_child());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

  if (join_node->join_mode() == JoinMode::Cross) {
    return std::make_shared<Product>(input_left_operator, input_right_operator);
  }

  DebugAssert(static_cast<bool>(join_node->join_column_ids()), "Cannot translate Join without join column ids.");
  DebugAssert(static_cast<bool>(join_node->scan_type()), "Cannot translate Join without ScanType.");
  return std::make_shared<JoinNestedLoopA>(input_left_operator, input_right_operator, join_node->join_mode(),
                                           *(join_node->join_column_ids()), *(join_node->scan_type()));
}

std::shared_ptr<AbstractOperator> ASTToOperatorTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractASTNode> &node) const {
  const auto input_operator = translate_node(node->left_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  auto aggregate_expressions = aggregate_node->aggregate_expressions();
  auto groupby_columns = aggregate_node->groupby_column_ids();

  auto aggregate_input_operator = input_operator;

  /**
   * 1. Handle arithmetic expressions in aggregate functions via Projection.
   *
   * In Hyrise, only Projections are supposed to be able to handle arithmetic expressions.
   * Therefore, if we encounter an expression within an aggregate function, we have to execute a Projection first.
   * The Aggregate will work with the output columns of that Projection.
   *
   * We only need a Projection before the aggregate if the function argument is an arithmetic expression.
   */
  auto need_projection = false;

  // Check if there are any arithmetic expressions.
  for (const auto &aggregate_expression : aggregate_expressions) {
    DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression is not a function.");

    // Check whether the expression that is the argument of the aggregate function, e.g. `a+b` in `SUM(a+b)` is, as in
    // this case, an arithmetic expression and therefore needs a Projection before the aggregate is performed. If all
    // Aggregates are in the style of `COUNT(b)` or there are just groupby columns, there is no need for a Projection.
    const auto &function_arg_expr = (aggregate_expression->expression_list())[0];
    if (function_arg_expr->is_arithmetic_operator()) {
      need_projection = true;
      break;
    }
  }

  /**
   * If there are arithmetic expressions create a Projection with:
   *
   *  - arithmetic expressions,
   *  - columns used in aggregate functions, and
   *  - GROUP BY columns
   *
   *  TODO(anybody): this might result in the same columns being created multiple times. Improve.
   */
  if (need_projection) {
    Projection::ColumnExpressions column_expressions = Expression::create_column_identifiers(groupby_columns);
    column_expressions.reserve(groupby_columns.size() + aggregate_expressions.size());

    // The Projection will only select columns used in the Aggregate, i.e., GROUP BY columns and expressions.
    // Unused columns are skipped – therefore, the ColumnIDs might change.
    // GROUP BY columns will be the first columns of the Projection.
    for (ColumnID column_id{0}; column_id < groupby_columns.size(); column_id++) {
      groupby_columns[column_id] = column_id;
    }

    // Aggregates will get consecutive ColumnIDs.
    auto current_column_id = static_cast<ColumnID::base_type>(groupby_columns.size());

    for (auto &aggregate_expression : aggregate_expressions) {
      DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression is not a function.");

      // Add original expression of the function to the Projection.
      column_expressions.emplace_back((aggregate_expression->expression_list())[0]);

      // Create a ColumnReference expression for the column id of the Projection.
      const auto column_ref_expr = Expression::create_column_identifier(ColumnID{current_column_id});
      current_column_id++;

      // Change the expression list of the expression representing the aggregate.
      aggregate_expression->set_expression_list({column_ref_expr});
    }

    aggregate_input_operator = std::make_shared<Projection>(aggregate_input_operator, column_expressions);
  }

  /**
   * 2. Build Aggregate
   */
  std::vector<AggregateDefinition> aggregate_definitions;
  aggregate_definitions.reserve(aggregate_expressions.size());

  for (const auto &aggregate_expression : aggregate_expressions) {
    DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Only functions are supported in Aggregates");

    const auto aggregate_function_type = aggregate_expression->aggregate_function();
    const auto column_id = (aggregate_expression->expression_list())[0]->column_id();
    aggregate_definitions.emplace_back(column_id, aggregate_function_type, aggregate_expression->alias());
  }

  return std::make_shared<Aggregate>(aggregate_input_operator, aggregate_definitions, groupby_columns);
}

}  // namespace opossum
