#include "sql_query_translator.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../operators/abstract_join_operator.hpp"
#include "../operators/abstract_operator.hpp"
#include "../operators/aggregate.hpp"
#include "../operators/difference.hpp"
#include "../operators/export_binary.hpp"
#include "../operators/export_csv.hpp"
#include "../operators/get_table.hpp"
#include "../operators/import_csv.hpp"
#include "../operators/index_column_scan.hpp"
#include "../operators/join_nested_loop_a.hpp"
#include "../operators/print.hpp"
#include "../operators/product.hpp"
#include "../operators/projection.hpp"
#include "../operators/sort.hpp"
#include "../operators/table_scan.hpp"
#include "../operators/union_all.hpp"
#include "../utils/assert.hpp"

#include "constant_mappings.hpp"
#include "types.hpp"

#include "SQLParser.h"

using hsql::Expr;
using hsql::SQLParser;
using hsql::SQLParserResult;
using hsql::SQLStatement;
using hsql::SelectStatement;
using hsql::TableRef;
using hsql::JoinDefinition;

namespace opossum {

SQLQueryTranslator::SQLQueryTranslator() {}

SQLQueryTranslator::~SQLQueryTranslator() {}

const SQLQueryPlan& SQLQueryTranslator::get_query_plan() { return _plan; }

const std::string& SQLQueryTranslator::get_error_msg() { return _error_msg; }

void SQLQueryTranslator::reset() {
  _plan.clear();
  _error_msg = "";
}

bool SQLQueryTranslator::translate_parse_result(const hsql::SQLParserResult& result) {
  const std::vector<SQLStatement*>& statements = result.getStatements();

  for (const SQLStatement* stmt : statements) {
    if (!translate_statement(*stmt)) {
      return false;
    }
  }

  return true;
}

bool SQLQueryTranslator::translate_statement(const SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect: {
      const SelectStatement& select = (const SelectStatement&)statement;
      if (!_translate_select(select)) {
        return false;
      }

      _plan.add_tree_by_root(_current_root);
      return true;
    }
    default:
      _error_msg = "Can only translate SELECT queries at the moment!";
      return false;
  }
}

bool SQLQueryTranslator::_translate_select(const SelectStatement& select) {
  // SQL Order of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause
  // 2. WHERE clause
  // 3. GROUP BY clause
  // 4. HAVING clause
  // 5. SELECT clause
  // 6. ORDER BY clause

  // Translate FROM.
  if (!_translate_table_ref(*select.fromTable)) {
    return false;
  }

  // Translate WHERE.
  // Add table scan if applicable.
  if (select.whereClause != nullptr) {
    Expr& where = *select.whereClause;
    auto input_op = _current_root;
    if (!_translate_filter_expr(where, input_op)) {
      return false;
    }
  }

  // Translate GROUP BY & HAVING
  if (select.groupBy != nullptr) {
    if (!_translate_group_by(*select.groupBy, *select.selectList, _current_root)) {
      return false;
    }
  }

  // Translate SELECT list.
  // Add projection for select list.
  // TODO(torpedro): Handle DISTINCT.
  if (!_translate_projection(*select.selectList, _current_root)) {
    return false;
  }

  // Translate ORDER BY.
  if (select.order != nullptr) {
    if (!_translate_order_by(*select.order, _current_root)) {
      return false;
    }
  }

  // TODO(torpedro): Translate LIMIT/TOP.

  return true;
}

bool SQLQueryTranslator::_translate_filter_expr(const hsql::Expr& expr,
                                                const std::shared_ptr<AbstractOperator>& input_op) {
  if (!expr.isType(hsql::kExprOperator)) {
    _error_msg = "Filter expression clause has to be of type operator!";
    return false;
  }

  // Handle operation types and get the filter op string..
  ScanType scan_type;
  switch (expr.opType) {
    case hsql::kOpAnd:
      // Recursively translate the child expressions.
      // This will chain TableScans.
      if (!_translate_filter_expr(*expr.expr, input_op)) {
        return false;
      }
      if (!_translate_filter_expr(*expr.expr2, _current_root)) {
        return false;
      }
      return true;

    default:
      // Get the operation string, if possible.
      if (!_translate_filter_op(expr, &scan_type)) {
        _error_msg = "Filter expression clause operator is not supported yet!";
        return false;
      }
  }

  // TODO(torpedro): Handle BETWEEN.

  // Get the column_name.
  Expr* column_expr =
      (expr.expr->isType(hsql::kExprColumnRef) || expr.expr->isType(hsql::kExprFunctionRef)) ? expr.expr : expr.expr2;

  if (!column_expr->isType(hsql::kExprColumnRef) && !column_expr->isType(hsql::kExprFunctionRef)) {
    _error_msg = "Unsupported filter expression!";
    return false;
  }
  std::string column_name = _get_column_name(*column_expr);

  // Get the value.
  // At this moment the value is expected to be a literal.
  Expr* other_expr = (column_expr == expr.expr) ? expr.expr2 : expr.expr;

  const AllParameterVariant value = translate_literal(*other_expr);

  if (column_name.length() == 0) {
    _error_msg = "Unsupported filter expression!";
    return false;
  }

  auto table_scan = std::make_shared<TableScan>(input_op, ColumnName(column_name), scan_type, value);
  _current_root = table_scan;
  return true;
}

bool SQLQueryTranslator::_translate_projection(const std::vector<hsql::Expr*>& expr_list,
                                               const std::shared_ptr<AbstractOperator>& input_op) {
  // Commented out because I won't make this compatible with the new Projections
//  std::vector<std::string> columns;
//  for (const Expr* expr : expr_list) {
//    // At this moment we only support selecting columns in the projection.
//    if (!expr->isType(hsql::kExprColumnRef) && !expr->isType(hsql::kExprStar) &&
//        !expr->isType(hsql::kExprFunctionRef)) {
//      _error_msg = "Projection only supports columns to be selected.";
//      return false;
//    }
//
//    if (expr->isType(hsql::kExprStar)) {
//      columns.push_back("*");
//      continue;
//    }
//
//    columns.push_back(_get_column_name(*expr));
//  }
//
//  // If only * is selected, no projection operator is needed.
//  if (columns.size() == 1 && columns[0].compare("*") == 0) {
//    return true;
//  }
//
//  auto projection = std::make_shared<Projection>(input_op, columns);
//  _current_root = projection;
  return true;
}

bool SQLQueryTranslator::_translate_group_by(const hsql::GroupByDescription& group_by,
                                             const std::vector<hsql::Expr*>& select_list,
                                             const std::shared_ptr<AbstractOperator>& input_op) {
  std::vector<AggregateDefinition> aggregates;
  std::vector<std::string> groupby_columns;

  // Process group by columns.
  for (const auto expr : *group_by.columns) {
    DebugAssert(expr->isType(hsql::kExprColumnRef), "Expect group by columns to be column references.");
    groupby_columns.push_back(_get_column_name(*expr));
  }

  // Process select list to build aggregate functions.
  for (const auto expr : select_list) {
    if (expr->isType(hsql::kExprFunctionRef)) {
      std::string fun_name(expr->name);

      DebugAssert(expr->exprList->size() == 1, "Expect SQL functions to only have single argument.");
      std::string argument = _get_column_name(*expr->exprList->at(0));

      if (aggregate_function_to_string.right.find(fun_name) != aggregate_function_to_string.right.end()) {
        aggregates.emplace_back(argument, aggregate_function_to_string.right.at(fun_name));
        continue;
      }

      _error_msg = "Unsupported aggregation function. (" + fun_name + ")";
      return false;
    }

    // TODO(torpedro): Check that all other columns are in the group by columns.
  }

  auto aggregate = std::make_shared<Aggregate>(input_op, aggregates, groupby_columns);
  _current_root = aggregate;

  // Handle HAVING clause.
  if (group_by.having != nullptr) {
    if (!_translate_filter_expr(*group_by.having, _current_root)) {
      return false;
    }
  }

  return true;
}

bool SQLQueryTranslator::_translate_order_by(const std::vector<hsql::OrderDescription*> order_list,
                                             const std::shared_ptr<AbstractOperator>& input_op) {
  // Make mutable copy.
  std::shared_ptr<AbstractOperator> prev_op = input_op;

  // Go through all the order descriptions and create sort task for each.
  for (const hsql::OrderDescription* order_desc : order_list) {
    const Expr& expr = *order_desc->expr;

    // TODO(torpedro): Check that Expr is actual column ref.
    const std::string name = _get_column_name(expr);
    const bool asc = (order_desc->type == hsql::kOrderAsc);
    auto sort = std::make_shared<Sort>(prev_op, name, asc);
    _current_root = sort;
    prev_op = sort;
  }

  return true;
}

bool SQLQueryTranslator::_translate_table_ref(const hsql::TableRef& table) {
  switch (table.type) {
    case hsql::kTableName: {
      auto get_table = std::make_shared<GetTable>(table.name);
      _current_root = get_table;
      return true;
    }
    case hsql::kTableSelect: {
      return _translate_select(*table.select);
    }
    case hsql::kTableJoin: {
      // TODO(torpedro): Split into method.
      const JoinDefinition& join_def = *table.join;

      // Get left and right sub tables.
      if (!_translate_table_ref(*join_def.left)) {
        return false;
      }
      auto left_op = _current_root;

      if (!_translate_table_ref(*join_def.right)) {
        return false;
      }
      auto right_op = _current_root;

      // Determine join condition.
      const Expr& condition = *join_def.condition;
      std::pair<std::string, std::string> columns(_get_column_name(*condition.expr),
                                                  _get_column_name(*condition.expr2));
      ScanType scan_type;
      if (!_translate_filter_op(condition, &scan_type)) {
        _error_msg = "Can not handle JOIN condition.";
        return false;
      }

      // Determine join mode.
      JoinMode mode;
      switch (join_def.type) {
        case hsql::kJoinInner:
          mode = JoinMode::Inner;
          break;
        case hsql::kJoinOuter:
          mode = JoinMode::Outer;
          break;
        case hsql::kJoinLeft:
          mode = JoinMode::Left;
          break;
        case hsql::kJoinRight:
          mode = JoinMode::Right;
          break;
        case hsql::kJoinNatural:
          mode = JoinMode::Natural;
          break;
        case hsql::kJoinCross:
          mode = JoinMode::Cross;
          break;
        default:
          _error_msg = "Unable to handle join type.";
          return false;
      }

      // In Opossum, the join requires a prefix.
      std::string prefix_left = std::string(join_def.left->getName()) + ".";
      std::string prefix_right = std::string(join_def.right->getName()) + ".";

      // TODO(torpedro): Optimize join type selection.
      auto join =
          std::make_shared<JoinNestedLoopA>(left_op, right_op, columns, scan_type, mode, prefix_left, prefix_right);
      _current_root = join;
      return true;
    }
    case hsql::kTableCrossProduct: {
      _error_msg = "Unable to translate table cross product.";
      return false;
    }
  }
  _error_msg = "Unable to translate source table.";
  return false;
}

// static
const AllParameterVariant SQLQueryTranslator::translate_literal(const hsql::Expr& expr) {
  switch (expr.type) {
    case hsql::kExprLiteralInt:
      return AllTypeVariant(expr.ival);
    case hsql::kExprLiteralFloat:
      return AllTypeVariant(expr.fval);
    case hsql::kExprLiteralString:
      return AllTypeVariant(expr.name);
    case hsql::kExprParameter:
      return ValuePlaceholder(expr.ival);
    default:
      throw std::runtime_error("Error while SQL planning. Expected literal.");
  }
}

// static
bool SQLQueryTranslator::_translate_filter_op(const hsql::Expr& expr, ScanType* output) {
  switch (expr.opType) {
    case hsql::kOpEquals:
      *output = ScanType::OpEquals;
      return true;
    case hsql::kOpLess:
      *output = ScanType::OpLessThan;
      return true;
    case hsql::kOpGreater:
      *output = ScanType::OpGreaterThan;
      return true;
    case hsql::kOpGreaterEq:
      *output = ScanType::OpGreaterThanEquals;
      return true;
    case hsql::kOpLessEq:
      *output = ScanType::OpLessThanEquals;
      return true;
    case hsql::kOpNotEquals:
      *output = ScanType::OpNotEquals;
      return true;
    case hsql::kOpBetween:
      *output = ScanType::OpBetween;
      return true;
    default:
      return false;
  }
  return false;
}

// static
std::string SQLQueryTranslator::_get_column_name(const hsql::Expr& expr) {
  std::string name = "";

  if (expr.isType(hsql::kExprFunctionRef)) {
    name += expr.name;
    name += "(";
    name += _get_column_name(*expr.exprList->at(0));
    name += ")";
    return name;
  }

  DebugAssert(expr.isType(hsql::kExprColumnRef), "Expected column reference.");
  if (expr.hasTable()) name += std::string(expr.table) + ".";
  name += expr.name;
  return name;
}

}  // namespace opossum
