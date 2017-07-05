#include "sql_query_node_translator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"
#include "optimizer/abstract_syntax_tree/expression_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"
#include "storage/storage_manager.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

using hsql::Expr;
using hsql::JoinDefinition;
using hsql::SelectStatement;
using hsql::SQLStatement;

namespace opossum {

SQLQueryNodeTranslator::SQLQueryNodeTranslator() {}

SQLQueryNodeTranslator::~SQLQueryNodeTranslator() {}

std::vector<std::shared_ptr<AbstractNode>> SQLQueryNodeTranslator::translate_parse_result(
    const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractNode>> result_nodes;
  const std::vector<SQLStatement*>& statements = result.getStatements();

  for (const SQLStatement* stmt : statements) {
    auto result_node = translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::translate_statement(const SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect: {
      const SelectStatement& select = (const SelectStatement&)statement;
      return _translate_select(select);
    }
    case hsql::kStmtPrepare: {
      // TODO(tim): what to return?
    }
    default:
      throw std::runtime_error("Translating statement failed.");
  }
}

std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::_translate_select(const SelectStatement& select) {
  // SQL Order of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause
  // 2. WHERE clause
  // 3. GROUP BY clause
  // 4. HAVING clause
  // 5. SELECT clause
  // 6. ORDER BY clause

  // Translate FROM.
  auto current_result_node = _translate_table_ref(*select.fromTable);

  // Translate WHERE.
  if (select.whereClause) {
    current_result_node = _translate_filter_expr(*select.whereClause, current_result_node);
  }

  // TODO(torpedro): Transform GROUP BY.
  // TODO(torpedro): Transform HAVING.

  // Translate SELECT list.
  // TODO(torpedro): Handle DISTINCT.
  current_result_node = _translate_projection(*select.selectList, current_result_node);

  // Translate ORDER BY.
  if (select.order != nullptr) {
    current_result_node = _translate_order_by(*select.order, current_result_node);
  }

  // TODO(torpedro): Translate LIMIT/TOP.

  return current_result_node;
}

// TODO(tim): JoinNode
// std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::_translate_join(const JoinDefinition& join) {
//  // Get left and right sub tables.
//  if (!_translate_table_ref(*join.left)) {
//    return false;
//  }
//  auto left_task = _plan.back();
//
//  if (!_translate_table_ref(*join.right)) {
//    return false;
//  }
//  auto right_task = _plan.back();
//
//  // Determine join condition.
//  const Expr& condition = *join.condition;
//  std::pair<std::string, std::string> columns(condition.expr->name, condition.expr2->name);
//  std::string op;
//  if (!_translate_filter_op(condition, &op)) {
//    throw std::runtime_error("Can not handle JOIN condition.");
//    return false;
//  }
//
//  // Determine join mode.
//  JoinMode mode;
//  switch (join.type) {
//    case hsql::kJoinInner:
//      mode = Inner;
//      break;
//    case hsql::kJoinOuter:
//      mode = Outer;
//      break;
//    case hsql::kJoinLeft:
//      mode = Left;
//      break;
//    case hsql::kJoinRight:
//      mode = Right;
//      break;
//    case hsql::kJoinNatural:
//      mode = Natural;
//      break;
//    case hsql::kJoinCross:
//      mode = Cross;
//      break;
//    default:
//      throw std::runtime_error("Unable to handle join type.");
//      return false;
//  }
//
//  // In Opossum, the join requires a prefix.
//  std::string prefix_left = std::string(join.left->getName()) + ".";
//  std::string prefix_right = std::string(join.right->getName()) + ".";
//
//  // TODO(torpedro): Optimize join type selection.
//  auto join = std::make_shared<JoinNestedLoopA>(left_task->get_operator(), right_task->get_operator(), columns, op,
//                                                mode, prefix_left, prefix_right);
//  auto task = std::make_shared<OperatorTask>(join);
//  left_task->set_as_predecessor_of(task);
//  right_task->set_as_predecessor_of(task);
//  _plan.add_task(task);
//  return true;
//}

std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::_translate_table_ref(const hsql::TableRef& table) {
  switch (table.type) {
    case hsql::kTableName: {
      return std::make_shared<TableNode>(table.name);
    }
    case hsql::kTableSelect: {
      return _translate_select(*table.select);
    }
    case hsql::kTableJoin: {
      // TODO(tim)
      //      return _translate_join(*table.join);
      throw std::runtime_error("Join not supported at the moment.");
    }
    case hsql::kTableCrossProduct: {
      // TODO(tim)
      //      return _translate_cross_product(*table.join);
      throw std::runtime_error("Unable to translate table cross product.");
    }
  }
  throw std::runtime_error("Unable to translate source table.");
}

const std::string SQLQueryNodeTranslator::_get_column_name(const hsql::Expr& expr) const {
  std::string name = "";
  if (expr.hasTable()) {
    name += std::string(expr.table) + ".";
  }

  name += expr.name;

  if (name.empty()) {
    throw std::runtime_error("Column name is empty.");
  }

  return name;
}

const AllTypeVariant SQLQueryNodeTranslator::_translate_literal(const hsql::Expr& expr) {
  switch (expr.type) {
    case hsql::kExprLiteralInt:
      return expr.ival;
    case hsql::kExprLiteralFloat:
      return expr.fval;
    case hsql::kExprLiteralString:
      return expr.name;
    default:
      throw std::runtime_error("Could not translate literal: expression type not supported.");
  }
}

std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::_translate_filter_expr(
    const hsql::Expr& expr, const std::shared_ptr<AbstractNode>& input_node) {
  if (!expr.isType(hsql::kExprOperator)) {
    throw std::runtime_error("Filter expression clause has to be of type operator!");
  }

  // If the expression is a nested expression, recursively resolve.
  // TODO(tim): implement OR.
  if (expr.opType == hsql::kOpAnd) {
    auto filter_node = _translate_filter_expr(*expr.expr, input_node);
    return _translate_filter_expr(*expr.expr2, filter_node);
  }

  // TODO(tim): move to function / global namespace / whatever.
  // TODO(tim): handle IN with join
  std::unordered_map<hsql::OperatorType, ScanType> operator_to_filter_type = {
      {hsql::kOpEquals, ScanType::OpEquals},       {hsql::kOpNotEquals, ScanType::OpNotEquals},
      {hsql::kOpGreater, ScanType::OpGreaterThan}, {hsql::kOpGreaterEq, ScanType::OpGreaterThanEquals},
      {hsql::kOpLess, ScanType::OpLessThan},       {hsql::kOpLessEq, ScanType::OpLessThanEquals},
      {hsql::kOpBetween, ScanType::OpBetween},     {hsql::kOpLike, ScanType::OpLike},
  };

  auto it = operator_to_filter_type.find(expr.opType);
  if (it == operator_to_filter_type.end()) {
    throw std::runtime_error("Filter expression clause operator is not yet supported.");
  }

  auto scan_type = it->second;

  // TODO(torpedro): Handle BETWEEN.


  std::shared_ptr<ExpressionNode> expressionNode = _translate_expression(expr);

  expressionNode->print();

  Expr* column_expr = (expr.expr->isType(hsql::kExprColumnRef)) ? expr.expr : expr.expr2;
  if (!column_expr->isType(hsql::kExprColumnRef)) {
    throw std::runtime_error("Unsupported filter: we must have a column reference on either side of the expression.");
  }

  const auto column_name = _get_column_name(*column_expr);

  Expr* other_expr = (column_expr == expr.expr) ? expr.expr2 : expr.expr;
  const AllTypeVariant value = _translate_literal(*other_expr);

  auto table_scan_node = std::make_shared<TableScanNode>(column_name, scan_type, value);
  table_scan_node->set_left(input_node);

  return table_scan_node;
}

std::shared_ptr<ExpressionNode> SQLQueryNodeTranslator::_translate_expression(const hsql::Expr& expr) {

  // TODO(Sven): move to function / global namespace / whatever.
  std::unordered_map<hsql::OperatorType, ExpressionType> operator_to_expression_type = {
    {hsql::kOpPlus, ExpressionType::ExpressionPlus},
    {hsql::kOpMinus, ExpressionType::ExpressionMinus},


    {hsql::kOpAsterisk, ExpressionType::ExpressionAsterisk},
    {hsql::kOpSlash, ExpressionType::ExpressionSlash},

    {hsql::kOpPercentage, ExpressionType::ExpressionPercentage},
    {hsql::kOpCaret, ExpressionType::ExpressionCaret},

    {hsql::kOpBetween, ExpressionType::ExpressionBetween},

    {hsql::kOpEquals, ExpressionType::ExpressionEquals},
    {hsql::kOpNotEquals, ExpressionType::ExpressionNotEquals},
    {hsql::kOpLess, ExpressionType::ExpressionLess},
    {hsql::kOpLessEq, ExpressionType::ExpressionLessEq},
    {hsql::kOpGreater, ExpressionType::ExpressionGreater},
    {hsql::kOpGreaterEq, ExpressionType::ExpressionGreaterEq},

    {hsql::kOpLike, ExpressionType::ExpressionLike},
    {hsql::kOpNotLike, ExpressionType::ExpressionNotLike}
  };

  // TODO(Sven): move to function / global namespace / whatever.
  std::unordered_map<hsql::ExprType, ExpressionType> expr_to_expression_type = {
    {hsql::kExprLiteralFloat, ExpressionType::ExpressionLiteral},
    {hsql::kExprLiteralString, ExpressionType::ExpressionLiteral},
    {hsql::kExprLiteralInt, ExpressionType::ExpressionLiteral},
    {hsql::kExprStar, ExpressionType::ExpressionStar},
    {hsql::kExprParameter, ExpressionType::ExpressionParameter},
    {hsql::kExprColumnRef, ExpressionType::ExpressionColumnReference},
    {hsql::kExprFunctionRef, ExpressionType::ExpressionFunctionReference},
    {hsql::kExprOperator, ExpressionType::ExpressionOperator},
    {hsql::kExprSelect, ExpressionType::ExpressionSelect},
    {hsql::kExprHint, ExpressionType::ExpressionHint}
  };

  auto expressionType = expr_to_expression_type.at(expr.type);

  std::shared_ptr<ExpressionNode> node;
  if (expressionType == ExpressionOperator) {
    auto operatorType = operator_to_expression_type.at(expr.opType);
    node = std::make_shared<ExpressionNode>(operatorType);
  } else {
    node = std::make_shared<ExpressionNode>(expressionType);
  }

  if (expr.expr) {
    auto left = _translate_expression(*expr.expr);
    node->set_left(left);
  }

  if (expr.expr2) {
    auto right = _translate_expression(*expr.expr2);
    node->set_right(right);
  }

  return node;

}

std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::_translate_projection(
    const std::vector<hsql::Expr*>& expr_list, const std::shared_ptr<AbstractNode>& input_node) {
  std::vector<std::string> columns;
  for (const Expr* expr : expr_list) {
    // TODO(tim): expressions
    if (expr->isType(hsql::kExprColumnRef)) {
      columns.push_back(_get_column_name(*expr));
    } else if (expr->isType(hsql::kExprStar)) {
      // Resolve '*' by getting the output columns of the input node.
      auto input_columns = input_node->output_columns();
      columns.insert(columns.end(), input_columns.begin(), input_columns.end());
    } else {
      throw std::runtime_error("Projection only supports columns to be selected.");
    }
  }

  auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left(input_node);

  return projection_node;
}

std::shared_ptr<AbstractNode> SQLQueryNodeTranslator::_translate_order_by(
    const std::vector<hsql::OrderDescription*> order_list, const std::shared_ptr<AbstractNode>& input_node) {
  if (order_list.empty()) {
    return input_node;
  }

  std::shared_ptr<AbstractNode> current_result_node = input_node;

  // Go through all the order descriptions and create a sort node for each of them.
  // Iterate in reverse because the sort operator does not support multiple columns,
  // and instead relies on stable sort. We therefore sort by the n+1-th column before sorting by the n-th column.
  for (auto it = order_list.rbegin(); it != order_list.rend(); ++it) {
    auto order_description = *it;
    const auto& order_expr = *order_description->expr;

    // TODO(tim): handle non-column refs
    Assert(order_expr.isType(hsql::kExprColumnRef), "Can only order by columns for now.");

    const auto column_name = _get_column_name(order_expr);
    const auto asc = order_description->type == hsql::kOrderAsc;

    auto sort_node = std::make_shared<SortNode>(column_name, asc);
    sort_node->set_left(current_result_node);
    current_result_node = sort_node;
  }

  return current_result_node;
}

}  // namespace opossum
