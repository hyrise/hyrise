#include "sql_query_translator.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../operators/abstract_join_operator.hpp"
#include "../operators/abstract_operator.hpp"
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

const std::vector<std::shared_ptr<OperatorTask>>& SQLQueryTranslator::get_tasks() { return _tasks; }

const std::string& SQLQueryTranslator::get_error_msg() { return _error_msg; }

void SQLQueryTranslator::reset() {
  _tasks.clear();
  _error_msg = "";
}

bool SQLQueryTranslator::translate_query(const std::string& query) {
  hsql::SQLParserResult result;

  // Parse the query.
  if (!parse_query(query, &result)) {
    return false;
  }

  // Translate into execution plan.
  if (!translate_parse_result(result)) {
    return false;
  }

  return true;
}

bool SQLQueryTranslator::parse_query(const std::string& query, hsql::SQLParserResult* result) {
  SQLParser::parseSQLString(query, result);

  if (!result->isValid()) {
    std::stringstream ss;
    ss << "SQL Parsing failed: " << result->errorMsg();
    ss << " (L" << result->errorLine() << ":" << result->errorColumn() << ")";
    _error_msg = ss.str();
    return false;
  }

  return true;
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
      return _translate_select(select);
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
    auto input_task = _tasks.back();

    if (!_translate_filter_expr(where, input_task)) {
      return false;
    }
  }

  // TODO(torpedro): Transform GROUP BY.
  // TODO(torpedro): Transform HAVING.

  // Translate SELECT list.
  // Add projection for select list.
  // TODO(torpedro): Handle DISTINCT.
  if (!_translate_projection(*select.selectList, _tasks.back())) {
    return false;
  }

  // Translate ORDER BY.
  if (select.order != nullptr) {
    if (!_translate_order_by(*select.order, _tasks.back())) {
      return false;
    }
  }

  // TODO(torpedro): Translate LIMIT/TOP.

  return true;
}

bool SQLQueryTranslator::_translate_filter_expr(const hsql::Expr& expr,
                                                const std::shared_ptr<OperatorTask>& input_task) {
  if (!expr.isType(hsql::kExprOperator)) {
    _error_msg = "Filter expression clause has to be of type operator!";
    return false;
  }

  // Handle operation types and get the filter op string..
  std::string filter_op = "";
  switch (expr.opType) {
    case hsql::kOpAnd:
      // Recursively translate the child expressions.
      // This will chain TableScans.
      if (!_translate_filter_expr(*expr.expr, input_task)) {
        return false;
      }
      if (!_translate_filter_expr(*expr.expr2, _tasks.back())) {
        return false;
      }
      return true;

    default:
      // Get the operation string, if possible.
      if (!_translate_filter_op(expr, &filter_op)) {
        _error_msg = "Filter expression clause operator is not supported yet!";
        return false;
      }
  }

  // TODO(torpedro): Handle BETWEEN.

  // Get the column_name.
  Expr* column_expr = (expr.expr->isType(hsql::kExprColumnRef)) ? expr.expr : expr.expr2;

  if (!column_expr->isType(hsql::kExprColumnRef)) {
    _error_msg = "Unsupported filter expression!";
    return false;
  }
  std::string column_name = _get_column_name(*column_expr);

  // Get the value.
  // At this moment the value is expected to be a literal.
  Expr* other_expr = (column_expr == expr.expr) ? expr.expr2 : expr.expr;
  AllTypeVariant value;
  if (!_translate_literal(*other_expr, &value)) {
    _error_msg = "Expected literal in WHERE condition.";
    return false;
  }

  if (filter_op.length() == 0 || column_name.length() == 0) {
    _error_msg = "Unsupported filter expression!";
    return false;
  }

  auto table_scan = std::make_shared<TableScan>(input_task->get_operator(), ColumnName(column_name), filter_op, value);
  auto scan_task = std::make_shared<OperatorTask>(table_scan);
  input_task->set_as_predecessor_of(scan_task);
  _tasks.push_back(scan_task);
  return true;
}

bool SQLQueryTranslator::_translate_projection(const std::vector<hsql::Expr*>& expr_list,
                                               const std::shared_ptr<OperatorTask>& input_task) {
  std::vector<std::string> columns;
  for (const Expr* expr : expr_list) {
    // At this moment we only support selecting columns in the projection.
    if (!expr->isType(hsql::kExprColumnRef) && !expr->isType(hsql::kExprStar)) {
      _error_msg = "Projection only supports columns to be selected.";
      return false;
    }

    if (expr->isType(hsql::kExprStar)) {
      columns.push_back("*");
      continue;
    }

    columns.push_back(_get_column_name(*expr));
  }

  // If only * is selected, no projection operator is needed.
  if (columns.size() == 1 && columns[0].compare("*") == 0) {
    return true;
  }

  auto projection = std::make_shared<Projection>(input_task->get_operator(), columns);
  auto projection_task = std::make_shared<OperatorTask>(projection);
  input_task->set_as_predecessor_of(projection_task);
  _tasks.push_back(projection_task);
  return true;
}

bool SQLQueryTranslator::_translate_order_by(const std::vector<hsql::OrderDescription*> order_list,
                                             const std::shared_ptr<OperatorTask>& input_task) {
  // Make mutable copy.
  std::shared_ptr<OperatorTask> prev_task = input_task;

  // Go through all the order descriptions and create sort task for each.
  for (const hsql::OrderDescription* order_desc : order_list) {
    const Expr& expr = *order_desc->expr;

    // TODO(torpedro): Check that Expr is actual column ref.
    const std::string name = _get_column_name(expr);
    const bool asc = (order_desc->type == hsql::kOrderAsc);
    auto sort = std::make_shared<Sort>(prev_task->get_operator(), name, asc);
    auto sort_task = std::make_shared<OperatorTask>(sort);
    prev_task->set_as_predecessor_of(sort_task);
    _tasks.push_back(sort_task);

    prev_task = sort_task;
  }

  return true;
}

bool SQLQueryTranslator::_translate_table_ref(const hsql::TableRef& table) {
  switch (table.type) {
    case hsql::kTableName: {
      auto get_table = std::make_shared<GetTable>(table.name);
      auto task = std::make_shared<OperatorTask>(get_table);
      _tasks.push_back(task);
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
      auto left_task = _tasks.back();

      if (!_translate_table_ref(*join_def.right)) {
        return false;
      }
      auto right_task = _tasks.back();

      // Determine join condition.
      const Expr& condition = *join_def.condition;
      std::pair<std::string, std::string> columns(condition.expr->name, condition.expr2->name);
      std::string op;
      if (!_translate_filter_op(condition, &op)) {
        _error_msg = "Can not handle JOIN condition.";
        return false;
      }

      // Determine join mode.
      JoinMode mode;
      switch (join_def.type) {
        case hsql::kJoinInner:
          mode = Inner;
          break;
        case hsql::kJoinOuter:
          mode = Outer;
          break;
        case hsql::kJoinLeft:
          mode = Left;
          break;
        case hsql::kJoinRight:
          mode = Right;
          break;
        case hsql::kJoinNatural:
          mode = Natural;
          break;
        case hsql::kJoinCross:
          mode = Cross;
          break;
        default:
          _error_msg = "Unable to handle join type.";
          return false;
      }

      // In Opossum, the join requires a prefix.
      std::string prefix_left = std::string(join_def.left->getName()) + ".";
      std::string prefix_right = std::string(join_def.right->getName()) + ".";

      // TODO(torpedro): Optimize join type selection.
      auto join = std::make_shared<JoinNestedLoopA>(left_task->get_operator(), right_task->get_operator(), columns, op,
                                                    mode, prefix_left, prefix_right);
      auto task = std::make_shared<OperatorTask>(join);
      left_task->set_as_predecessor_of(task);
      right_task->set_as_predecessor_of(task);
      _tasks.push_back(task);
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
bool SQLQueryTranslator::_translate_literal(const hsql::Expr& expr, AllTypeVariant* output) {
  switch (expr.type) {
    case hsql::kExprLiteralInt:
      *output = expr.ival;
      return true;
    case hsql::kExprLiteralFloat:
      *output = expr.fval;
      return true;
    case hsql::kExprLiteralString:
      *output = expr.name;
      return true;
    default:
      return false;
  }
}

// static
bool SQLQueryTranslator::_translate_filter_op(const hsql::Expr& expr, std::string* output) {
  switch (expr.opType) {
    case hsql::kOpSimple:
      if (expr.isSimpleOp('=')) *output = "=";
      if (expr.isSimpleOp('<')) *output = "<";
      if (expr.isSimpleOp('>')) *output = ">";
      return true;
    case hsql::kOpGreaterEq:
      *output = ">=";
      return true;
    case hsql::kOpLessEq:
      *output = "<=";
      return true;
    case hsql::kOpNotEquals:
      *output = "!=";
      return true;
    case hsql::kOpBetween:
      *output = "BETWEEN";
      return true;
    default:
      return false;
  }
  return false;
}

// static
std::string SQLQueryTranslator::_get_column_name(const hsql::Expr& expr) {
  std::string name = "";
  if (expr.hasTable()) name += std::string(expr.table) + ".";
  name += expr.name;
  return name;
}

}  // namespace opossum
