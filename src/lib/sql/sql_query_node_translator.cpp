#include "sql_query_node_translator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/expression_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "sql/sql_expression_translator.hpp"
#include "storage/storage_manager.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

using hsql::Expr;
using hsql::ExprType;
using hsql::JoinDefinition;
using hsql::SelectStatement;
using hsql::SQLStatement;

namespace opossum {

SQLQueryNodeTranslator::SQLQueryNodeTranslator() {}

SQLQueryNodeTranslator::~SQLQueryNodeTranslator() {}

std::vector<std::shared_ptr<AbstractASTNode>> SQLQueryNodeTranslator::translate_parse_result(
    const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractASTNode>> result_nodes;
  const std::vector<SQLStatement*>& statements = result.getStatements();

  for (const SQLStatement* stmt : statements) {
    auto result_node = translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::translate_statement(const SQLStatement& statement) {
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

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_select(const SelectStatement& select) {
  // SQL Order of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause
  // 2. WHERE clause
  // 3. GROUP BY clause
  // 4. HAVING clause
  // 5. SELECT clause
  // 6. ORDER BY clause

  auto current_result_node = _translate_table_ref(*select.fromTable);

  if (select.whereClause) {
    current_result_node = _translate_filter_expr(*select.whereClause, current_result_node);
  }

  // TODO(torpedro): Handle DISTINCT.
  Assert(select.selectList != nullptr, "SELECT list needs to exist");
  Assert(!select.selectList->empty(), "SELECT list needs to have entries");

  // If the query has a GROUP BY clause or if it has aggregates, we do not need a top-level projection
  // because all elements must either be aggregate functions or columns of the GROUP BY clause,
  // so the Aggregate operator will handle them.
  auto is_aggregate = select.groupBy != nullptr;
  if (!is_aggregate) {
    for (auto* expr : *select.selectList) {
      if (expr->isType(hsql::kExprFunctionRef)) {
        is_aggregate = true;
        break;
      }
    }
  }

  if (is_aggregate) {
    current_result_node = _translate_aggregate(select.groupBy, *select.selectList, current_result_node);
  } else {
    current_result_node = _translate_projection(*select.selectList, current_result_node);
  }

  // Translate ORDER BY.
  if (select.order != nullptr) {
    current_result_node = _translate_order_by(*select.order, current_result_node);
  }

  // TODO(torpedro): Translate LIMIT/TOP.

  return current_result_node;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_join(const JoinDefinition& join) {
  auto left_node = _translate_table_ref(*join.left);
  auto right_node = _translate_table_ref(*join.right);

  const Expr& condition = *join.condition;
  std::pair<std::string, std::string> column_names(condition.expr->name, condition.expr2->name);

  // Joins currently only support one simple condition (i.e., not multiple conditions).
  // TODO(tim): move to function / global namespace / whatever.
  std::unordered_map<hsql::OperatorType, ScanType> operator_to_filter_type = {
      {hsql::kOpEquals, ScanType::OpEquals},       {hsql::kOpNotEquals, ScanType::OpNotEquals},
      {hsql::kOpGreater, ScanType::OpGreaterThan}, {hsql::kOpGreaterEq, ScanType::OpGreaterThanEquals},
      {hsql::kOpLess, ScanType::OpLessThan},       {hsql::kOpLessEq, ScanType::OpLessThanEquals},
      {hsql::kOpBetween, ScanType::OpBetween},     {hsql::kOpLike, ScanType::OpLike},
  };

  auto it = operator_to_filter_type.find(condition.opType);
  if (it == operator_to_filter_type.end()) {
    Fail("Filter expression clause operator is not yet supported.");
  }

  auto scan_type = it->second;

  // Determine join mode.
  // TODO(tim): refactor to other location
  JoinMode join_mode;
  switch (join.type) {
    case hsql::kJoinInner:
      join_mode = JoinMode::Inner;
      break;
    case hsql::kJoinOuter:
      join_mode = JoinMode::Outer;
      break;
    case hsql::kJoinLeft:
      join_mode = JoinMode::Left;
      break;
    case hsql::kJoinRight:
      join_mode = JoinMode::Right;
      break;
    case hsql::kJoinNatural:
      join_mode = JoinMode::Natural;
      break;
    case hsql::kJoinCross:
      join_mode = JoinMode::Cross;
      break;
    default:
      Fail("Unable to handle join type.");
  }

  std::string prefix_left = std::string(join.left->getName()) + ".";
  std::string prefix_right = std::string(join.right->getName()) + ".";

  auto join_node = std::make_shared<JoinNode>(column_names, scan_type, join_mode, prefix_left, prefix_right);
  join_node->set_left_child(left_node);
  join_node->set_right_child(right_node);
  return join_node;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_table_ref(const hsql::TableRef& table) {
  switch (table.type) {
    case hsql::kTableName: {
      return std::make_shared<StoredTableNode>(table.name);
    }
    case hsql::kTableSelect: {
      return _translate_select(*table.select);
    }
    case hsql::kTableJoin: {
      return _translate_join(*table.join);
    }
    case hsql::kTableCrossProduct: {
      // TODO(tim)
      //      return _translate_cross_product(*table.join);
      throw std::runtime_error("Unable to translate table cross product.");
    }
  }
  throw std::runtime_error("Unable to translate source table.");
}

std::string SQLQueryNodeTranslator::_get_column_name(const hsql::Expr& expr) const {
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

AllTypeVariant SQLQueryNodeTranslator::_translate_literal(const hsql::Expr& expr) {
  switch (expr.type) {
    case hsql::kExprLiteralInt:
      return expr.ival;
    case hsql::kExprLiteralFloat:
      return expr.fval;
    case hsql::kExprLiteralString:
      return expr.name;
    default:
      std::cout << "Unexpected Expr type" << std::endl;
      return 0;
      //      throw std::runtime_error("Could not translate literal: expression type not supported.");
  }
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_filter_expr(
    const hsql::Expr& expr, const std::shared_ptr<AbstractASTNode>& input_node) {
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

  const auto scan_type = it->second;

  std::shared_ptr<ExpressionNode> expressionNode = SQLExpressionTranslator::translate_expression(expr);

  Expr* column_expr = (expr.expr->isType(hsql::kExprColumnRef)) ? expr.expr : expr.expr2;
  if (!column_expr->isType(hsql::kExprColumnRef)) {
    throw std::runtime_error("Unsupported filter: we must have a column reference on either side of the expression.");
  }

  const auto column_name = _get_column_name(*column_expr);

  AllParameterVariant value;
  optional<AllTypeVariant> value2;
  if (scan_type == ScanType::OpBetween) {
    const Expr* left_expr = expr.exprList->at(0);
    value = AllParameterVariant(_translate_literal(*left_expr));

    const Expr* right_expr = expr.exprList->at(1);
    value2 = _translate_literal(*right_expr);
  } else {
    const Expr* other_expr = (column_expr == expr.expr) ? expr.expr2 : expr.expr;
    value = AllParameterVariant(_translate_literal(*other_expr));
  }

  const std::string column_name2{column_name};
  const std::shared_ptr<ExpressionNode> expressionNode2{expressionNode};
  ScanType scan_type2{scan_type};
  const AllParameterVariant value3{value};
  const optional<AllTypeVariant> value4{value2};

  auto predicate_node = std::make_shared<PredicateNode>(column_name2, expressionNode2, scan_type2, value3, value4);
  predicate_node->set_left_child(input_node);

  return predicate_node;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_aggregate(
        const hsql::GroupByDescription* group_by, const std::vector<hsql::Expr*>& select_list,
        const std::shared_ptr<AbstractASTNode>& input_node) {
  /**
   * Build Aggregates
   */
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  aggregate_column_definitions.reserve(select_list.size());

  for (auto* expr : select_list) {
    if (expr->isType(hsql::kExprFunctionRef)) {
      auto opossum_expr = SQLExpressionTranslator().translate_expression(*expr);
      if (expr->alias) {
        aggregate_column_definitions.emplace_back(opossum_expr, expr->alias);
      } else {
        aggregate_column_definitions.emplace_back(opossum_expr);
      }
    } else if (expr->isType(hsql::kExprColumnRef)) {
      // If the item is a column, it has to be in the GROUP BY clause.
      // TODO(tim): do check
    }
  }

  /**
   * Build GROUP BY
   */
  std::vector<std::string> groupby_columns;
  if (group_by != nullptr) {
    groupby_columns.reserve(group_by->columns->size());
    for (const auto* groupby_expr : *group_by->columns) {
      Assert(groupby_expr->isType(hsql::kExprColumnRef), "Only column ref GROUP BYs supported atm");
      Assert(groupby_expr->name != nullptr, "Expr::name needs to be set");

      groupby_columns.emplace_back(groupby_expr->name);
    }
  }

  auto aggregate_node = std::make_shared<AggregateNode>(aggregate_column_definitions, groupby_columns);
  aggregate_node->set_left_child(input_node);

  // TODO(tim&moritz): Transform HAVING.
  Assert(group_by == nullptr || group_by->having == nullptr, "HAVING not supported, yet");

  return aggregate_node;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_projection(
    const std::vector<hsql::Expr*>& select_list, const std::shared_ptr<AbstractASTNode>& input_node) {
  std::vector<std::string> columns;
  for (const Expr* expr : select_list) {
    // TODO(tim): expressions
    if (expr->isType(hsql::kExprColumnRef)) {
      columns.push_back(_get_column_name(*expr));
    } else if (expr->isType(hsql::kExprStar)) {
      // Resolve '*' by getting the output columns of the input node.
      auto input_columns = input_node->output_column_names();
      columns.insert(columns.end(), input_columns.begin(), input_columns.end());
    } else {
      throw std::runtime_error("Projection only supports columns to be selected.");
    }
  }

  auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(input_node);

  return projection_node;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_order_by(
    const std::vector<hsql::OrderDescription*> order_list, const std::shared_ptr<AbstractASTNode>& input_node) {
  if (order_list.empty()) {
    return input_node;
  }

  std::shared_ptr<AbstractASTNode> current_result_node = input_node;

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
    sort_node->set_left_child(current_result_node);
    current_result_node = sort_node;
  }

  return current_result_node;
}

}  // namespace opossum
