#include "sql_query_node_translator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "sql/sql_expression_translator.hpp"
#include "storage/storage_manager.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

using hsql::Expr;
using hsql::ExprType;
using hsql::JoinDefinition;
using hsql::SelectStatement;
using hsql::SQLStatement;

namespace opossum {

ScanType translate_operator_type_to_scan_type(const hsql::OperatorType operator_type) {
  const std::unordered_map<const hsql::OperatorType, const ScanType> operator_to_scan_type = {
      {hsql::kOpEquals, ScanType::OpEquals},       {hsql::kOpNotEquals, ScanType::OpNotEquals},
      {hsql::kOpGreater, ScanType::OpGreaterThan}, {hsql::kOpGreaterEq, ScanType::OpGreaterThanEquals},
      {hsql::kOpLess, ScanType::OpLessThan},       {hsql::kOpLessEq, ScanType::OpLessThanEquals},
      {hsql::kOpBetween, ScanType::OpBetween},     {hsql::kOpLike, ScanType::OpLike},
  };

  auto it = operator_to_scan_type.find(operator_type);
  if (it == operator_to_scan_type.end()) {
    Fail("Filter expression clause operator is not yet supported.");
  }

  return it->second;
}

JoinMode translate_join_type_to_join_mode(const hsql::JoinType join_type) {
  const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinOuter, JoinMode::Outer},     {hsql::kJoinLeft, JoinMode::Left},
      {hsql::kJoinRight, JoinMode::Right}, {hsql::kJoinNatural, JoinMode::Natural}, {hsql::kJoinCross, JoinMode::Cross},
  };

  auto it = join_type_to_mode.find(join_type);
  if (it == join_type_to_mode.end()) {
    Fail("Unable to handle join type.");
  }

  return it->second;
}

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
      const auto& select = (const SelectStatement&)statement;
      return _translate_select(select);
    }
    default:
      Fail("Translating statement failed.");
      return {};
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

  if (select.whereClause != nullptr) {
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
    current_result_node = _translate_aggregate(select, current_result_node);
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
  std::pair<std::string, std::string> column_names(get_column_name(*condition.expr, false),
                                                   get_column_name(*condition.expr2, false));

  // Joins currently only support one simple condition (i.e., not multiple conditions).
  auto scan_type = translate_operator_type_to_scan_type(condition.opType);
  auto join_mode = translate_join_type_to_join_mode(join.type);

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
      // TODO(mp)
      Fail("Unable to translate table cross product.");
      return {};
    }
  }
  Fail("Unable to translate source table.");
  return {};
}

std::string SQLQueryNodeTranslator::get_column_name(const hsql::Expr& expr, bool include_table_name) {
  std::string name;

  // Translate an aggregate function to a string that the Aggregate operator generates.
  if (expr.isType(hsql::kExprFunctionRef)) {
    name += expr.name;
    name += "(";
    name += get_column_name(*expr.exprList->at(0), include_table_name);
    name += ")";
    return name;
  }

  DebugAssert(expr.isType(hsql::kExprColumnRef), "Expected column reference.");

  if (include_table_name && expr.hasTable()) {
    name += std::string(expr.table) + ".";
  }

  name += expr.name;

  Assert(!name.empty(), "Column name is empty.");

  return name;
}

AllParameterVariant SQLQueryNodeTranslator::translate_literal(const hsql::Expr& expr) {
  switch (expr.type) {
    case hsql::kExprLiteralInt:
      return AllTypeVariant(expr.ival);
    case hsql::kExprLiteralFloat:
      return AllTypeVariant(expr.fval);
    case hsql::kExprLiteralString:
      return AllTypeVariant(expr.name);
    case hsql::kExprParameter:
      return ValuePlaceholder(expr.ival);
    case hsql::kExprColumnRef:
      return ColumnName(get_column_name(expr, true));
    default:
      Fail("Could not translate literal: expression type not supported.");
      return {};
  }
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_filter_expr(
    const hsql::Expr& expr, const std::shared_ptr<AbstractASTNode>& input_node) {
  if (!expr.isType(hsql::kExprOperator)) {
    Fail("Filter expression clause has to be of type operator!");
  }

  // If the expression is a nested expression, recursively resolve.
  // TODO(anybody): implement OR.
  if (expr.opType == hsql::kOpAnd) {
    auto filter_node = _translate_filter_expr(*expr.expr, input_node);
    return _translate_filter_expr(*expr.expr2, filter_node);
  }

  // TODO(anybody): handle IN with join
  const auto scan_type = translate_operator_type_to_scan_type(expr.opType);

  // We accept functions here because we assume they have been translated by Aggregate.
  // They will be treated as a regular column of the same name.
  // TODO(mp): this has to change once we have extended HAVING support.
  Expr* column_operand_expr = nullptr;
  Expr* value_operand_expr = nullptr;
  if (expr.expr->isType(hsql::kExprColumnRef) || expr.expr->isType(hsql::kExprFunctionRef)) {
    column_operand_expr = expr.expr;
    value_operand_expr = expr.expr2;
  } else {
    value_operand_expr = expr.expr;
    column_operand_expr = expr.expr2;
    Assert(column_operand_expr->isType(hsql::kExprColumnRef) || column_operand_expr->isType(hsql::kExprFunctionRef),
           "Unsupported filter: we must have a function or column reference on at least one side of the expression.");
  }

  const auto column_name = get_column_name(*column_operand_expr, true);

  AllParameterVariant value;
  optional<AllTypeVariant> value2;

  if (scan_type == ScanType::OpBetween) {
    const Expr* left_expr = (*expr.exprList)[0];
    const Expr* right_expr = (*expr.exprList)[1];

    value = translate_literal(*left_expr);

    // TODO(torpedro / mp): TableScan does not support AllParameterVariant as second value.
    // This would be required to prepare BETWEEN.
    value2 = boost::get<AllTypeVariant>(translate_literal(*right_expr));
  } else {
    value = translate_literal(*value_operand_expr);
  }

  std::shared_ptr<ExpressionNode> expression_node = SQLExpressionTranslator::translate_expression(expr);
  // TODO(moritz&tim): BLOCKING - remove, just for compilation testing
  const std::string column_name2{column_name};
  const std::shared_ptr<ExpressionNode> expression_node2{expression_node};
  ScanType scan_type2{scan_type};
  const AllParameterVariant value3{value};
  const optional<AllTypeVariant> value4{value2};

  auto predicate_node = std::make_shared<PredicateNode>(column_name2, expression_node2, scan_type2, value3, value4);
  predicate_node->set_left_child(input_node);

  return predicate_node;
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_aggregate(
    const hsql::SelectStatement& select, const std::shared_ptr<AbstractASTNode>& input_node) {
  const auto& select_list = *select.selectList;
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
    } else {
      Fail("Unsupported item in projection list for AggregateOperator.");
    }
  }

  /**
   * Build GROUP BY
   */
  const auto* group_by = select.groupBy;
  std::vector<std::string> groupby_columns;
  if (group_by != nullptr) {
    groupby_columns.reserve(group_by->columns->size());
    for (const auto* groupby_expr : *group_by->columns) {
      Assert(groupby_expr->isType(hsql::kExprColumnRef), "Only column ref GROUP BYs supported atm");
      Assert(groupby_expr->name != nullptr, "Expr::name needs to be set");

      groupby_columns.emplace_back(get_column_name(*groupby_expr, true));
    }
  }

  auto aggregate_node = std::make_shared<AggregateNode>(aggregate_column_definitions, groupby_columns);
  aggregate_node->set_left_child(input_node);

  if (group_by == nullptr || group_by->having == nullptr) {
    return aggregate_node;
  }

  /**
   * Build HAVING
   */
  // TODO(mp): Support HAVING clauses with aggregates different to the ones in the select list.
  // The HAVING clause may contain aggregates that are not part of the select list.
  // In that case, a succeeding table scan will not be able to filter because the column will not be part of the table.
  return _translate_filter_expr(*group_by->having, aggregate_node);
}

std::shared_ptr<AbstractASTNode> SQLQueryNodeTranslator::_translate_projection(
    const std::vector<hsql::Expr*>& select_list, const std::shared_ptr<AbstractASTNode>& input_node) {
  std::vector<std::string> columns;
  for (const Expr* expr : select_list) {
    // TODO(mp): expressions
    if (expr->isType(hsql::kExprColumnRef)) {
      columns.push_back(get_column_name(*expr, true));
    } else if (expr->isType(hsql::kExprStar)) {
      // Resolve '*' by getting the output columns of the input node.
      auto input_columns = input_node->output_column_names();
      columns.insert(columns.end(), input_columns.begin(), input_columns.end());
    } else {
      Fail("Projection only supports columns to be selected.");
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

    // TODO(mp): handle non-column refs
    Assert(order_expr.isType(hsql::kExprColumnRef), "Can only order by columns for now.");

    const auto column_name = get_column_name(order_expr, true);
    const auto asc = order_description->type == hsql::kOrderAsc;

    auto sort_node = std::make_shared<SortNode>(column_name, asc);
    sort_node->set_left_child(current_result_node);
    current_result_node = sort_node;
  }

  return current_result_node;
}

}  // namespace opossum
