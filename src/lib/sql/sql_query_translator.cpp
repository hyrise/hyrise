#include "sql_query_translator.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate.hpp"
#include "operators/difference.hpp"
#include "operators/export_binary.hpp"
#include "operators/export_csv.hpp"
#include "operators/get_table.hpp"
#include "operators/import_csv.hpp"
#include "operators/index_column_scan.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

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
      return _translate_select(select);
    }
    case hsql::kStmtPrepare: {
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
    auto input_task = _plan.back();

    if (!_translate_filter_expr(where, input_task)) {
      return false;
    }
  }

  // Create TableScans in the optimal order as estimated by their intermediate result sizes.
  if (_filters_by_table.size() > 0) {
    auto input_task = _plan.back();

    for (auto& filter : _filters_by_table) {
      auto table_filters = filter.second;
      auto predicate_estimates = std::vector<std::pair<size_t, double>>();

      if (table_filters.size() == 1) {
        // Do not estimate statistics if there is only one filter on the table.
        // Instead simply add the filter with a dummy value to predicate estimates to avoid duplicate code.
        predicate_estimates.emplace_back(0, 0.0);
      } else {
        // Estimate the result size of the filters individually.
        auto table_name = filter.first;
        auto table = StorageManager::get().get_table(table_name);

        for (auto filter_index = 0u; filter_index < table_filters.size(); filter_index++) {
          auto tuple = table_filters[filter_index];
          auto estimated_result_size = table->get_table_statistics()
                                           ->predicate_statistics(get<0>(tuple), get<1>(tuple), get<2>(tuple))
                                           ->row_count();
          predicate_estimates.emplace_back(filter_index, estimated_result_size);
        }

        // Sort predicates by their estimated result size, smallest sizes first.
        std::sort(predicate_estimates.begin(), predicate_estimates.end(),
                  [](auto& left, auto& right) { return left.second < right.second; });
      }

      // Create TableScans.
      for (auto& predicate_estimate : predicate_estimates) {
        auto current_filter = table_filters[predicate_estimate.first];
        auto column_name = get<0>(current_filter);
        auto filter_op = get<1>(current_filter);
        auto value = get<2>(current_filter);

        auto table_scan =
            std::make_shared<TableScan>(input_task->get_operator(), ColumnName(column_name), filter_op, value);
        auto scan_task = std::make_shared<OperatorTask>(table_scan);
        input_task->set_as_predecessor_of(scan_task);

        // Add task to the list, and use this scan task as input for the next scan.
        _plan.add_task(scan_task);
        input_task = scan_task;
      }
    }

    // Clear the processed filters.
    // This is important for sub selects, which would otherwise use them again.
    _filters_by_table.clear();
  }

  // Translate GROUP BY & HAVING
  if (select.groupBy != nullptr) {
    if (!_translate_group_by(*select.groupBy, *select.selectList, _plan.back())) {
      return false;
    }
  }

  // Translate SELECT list.
  // Add projection for select list.
  // TODO(torpedro): Handle DISTINCT.
  if (!_translate_projection(*select.selectList, _plan.back())) {
    return false;
  }

  // Translate ORDER BY.
  if (select.order != nullptr) {
    if (!_translate_order_by(*select.order, _plan.back())) {
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

  // Check if the filter can be on a physical table.
  // In that case, we can already do predicate ordering.
  auto is_base_table = false;
  auto get_table_operator = std::dynamic_pointer_cast<const GetTable>(input_task->get_operator());
  if (get_table_operator) {
    is_base_table = true;
  }

  // Handle operation types and get the filter op string..
  ScanType scan_type;
  switch (expr.opType) {
    case hsql::kOpAnd:
      // Recursively translate the child expressions.
      // This will chain TableScans.
      if (!_translate_filter_expr(*expr.expr, input_task)) {
        return false;
      }

      // If we can filter on a physical table, we don't want to create the TableScans immediately,
      // but check which order is expected to be the best.
      // Thus we provide the physical table and not the TableScan of the first expression as input task.
      if (is_base_table) {
        return _translate_filter_expr(*expr.expr2, input_task);
      } else {
        return _translate_filter_expr(*expr.expr2, _plan.back());
      }

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

  // If we filter on a base table, temporarily store the parameters for the table scan and create the task later.
  // Otherwise append the TableScan to `_plan`.
  if (is_base_table) {
    auto table_name = get_table_operator->table_name();
    _filters_by_table[table_name].emplace_back(ColumnName(column_name), scan_type, value);
  } else {
    auto table_scan =
        std::make_shared<TableScan>(input_task->get_operator(), ColumnName(column_name), scan_type, value);
    auto scan_task = std::make_shared<OperatorTask>(table_scan);
    input_task->set_as_predecessor_of(scan_task);
    _plan.add_task(scan_task);
  }

  return true;
}

bool SQLQueryTranslator::_translate_projection(const std::vector<hsql::Expr*>& expr_list,
                                               const std::shared_ptr<OperatorTask>& input_task) {
  std::vector<std::string> columns;
  for (const Expr* expr : expr_list) {
    // At this moment we only support selecting columns in the projection.
    if (!expr->isType(hsql::kExprColumnRef) && !expr->isType(hsql::kExprStar) &&
        !expr->isType(hsql::kExprFunctionRef)) {
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
  _plan.add_task(projection_task);
  return true;
}

bool SQLQueryTranslator::_translate_group_by(const hsql::GroupByDescription& group_by,
                                             const std::vector<hsql::Expr*>& select_list,
                                             const std::shared_ptr<OperatorTask>& input_task) {
  std::vector<AggregateDefinition> aggregates;
  std::vector<std::string> groupby_columns;

  // Process group by columns.
  for (const auto expr : *group_by.columns) {
    DebugAssert(expr->isType(hsql::kExprColumnRef), "Expect group by columns to be column references.");
    groupby_columns.push_back(_get_column_name(*expr));
  }

  // Process select list to build aggregate functions.
  std::map<std::string, AggregateFunction> agg_map = {
      std::pair<std::string, AggregateFunction>("SUM", Sum),     std::pair<std::string, AggregateFunction>("AVG", Avg),
      std::pair<std::string, AggregateFunction>("MIN", Min),     std::pair<std::string, AggregateFunction>("MAX", Max),
      std::pair<std::string, AggregateFunction>("COUNT", Count),
  };
  for (const auto expr : select_list) {
    if (expr->isType(hsql::kExprFunctionRef)) {
      std::string fun_name(expr->name);

      DebugAssert(expr->exprList->size() == 1, "Expect SQL functions to only have single argument.");
      std::string argument = _get_column_name(*expr->exprList->at(0));

      if (agg_map.find(fun_name) != agg_map.end()) {
        aggregates.emplace_back(argument, agg_map[fun_name]);
        continue;
      }

      _error_msg = "Unsupported aggregation function. (" + fun_name + ")";
      return false;
    }

    // TODO(torpedro): Check that all other columns are in the group by columns.
  }

  auto aggregate = std::make_shared<Aggregate>(input_task->get_operator(), aggregates, groupby_columns);
  auto aggregate_task = std::make_shared<OperatorTask>(aggregate);
  input_task->set_as_predecessor_of(aggregate_task);
  _plan.add_task(aggregate_task);

  // Handle HAVING clause.
  if (group_by.having != nullptr) {
    if (!_translate_filter_expr(*group_by.having, _plan.back())) {
      return false;
    }
  }

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
    _plan.add_task(sort_task);

    prev_task = sort_task;
  }

  return true;
}

bool SQLQueryTranslator::_translate_table_ref(const hsql::TableRef& table) {
  switch (table.type) {
    case hsql::kTableName: {
      auto get_table = std::make_shared<GetTable>(table.name);
      auto task = std::make_shared<OperatorTask>(get_table);
      _plan.add_task(task);
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
      auto left_task = _plan.back();

      if (!_translate_table_ref(*join_def.right)) {
        return false;
      }
      auto right_task = _plan.back();

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
      auto join = std::make_shared<JoinNestedLoopA>(left_task->get_operator(), right_task->get_operator(), columns,
                                                    scan_type, mode, prefix_left, prefix_right);
      auto task = std::make_shared<OperatorTask>(join);
      left_task->set_as_predecessor_of(task);
      right_task->set_as_predecessor_of(task);
      _plan.add_task(task);
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
