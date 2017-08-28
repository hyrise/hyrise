#include "sql_to_ast_translator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "operators/projection.hpp"
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

namespace opossum {

ScanType translate_operator_type_to_scan_type(const hsql::OperatorType operator_type) {
  static const std::unordered_map<const hsql::OperatorType, const ScanType> operator_to_scan_type = {
      {hsql::kOpEquals, ScanType::OpEquals},       {hsql::kOpNotEquals, ScanType::OpNotEquals},
      {hsql::kOpGreater, ScanType::OpGreaterThan}, {hsql::kOpGreaterEq, ScanType::OpGreaterThanEquals},
      {hsql::kOpLess, ScanType::OpLessThan},       {hsql::kOpLessEq, ScanType::OpLessThanEquals},
      {hsql::kOpBetween, ScanType::OpBetween},     {hsql::kOpLike, ScanType::OpLike},
  };

  auto it = operator_to_scan_type.find(operator_type);
  DebugAssert(it != operator_to_scan_type.end(), "Filter expression clause operator is not yet supported.");
  return it->second;
}

ScanType get_scan_type_for_reverse_order(const ScanType scan_type) {
  /**
     * If we switch the sides for the expressions, we might have to change the operator that is used for the predicate.
     * This function returns the respective ScanType.
     *
     * Example:
     *     SELECT * FROM t WHERE 1 > a
     *  -> SELECT * FROM t WHERE a < 1
     *
     *    but:
     *     SELECT * FROM t WHERE 1 = a
     *  -> SELECT * FROM t WHERE a = 1
     */
  static const std::unordered_map<const ScanType, const ScanType> scan_type_for_reverse_order = {
      {ScanType::OpGreaterThan, ScanType::OpLessThan},
      {ScanType::OpLessThan, ScanType::OpGreaterThan},
      {ScanType::OpGreaterThanEquals, ScanType::OpLessThanEquals},
      {ScanType::OpLessThanEquals, ScanType::OpGreaterThanEquals}};

  auto it = scan_type_for_reverse_order.find(scan_type);
  if (it != scan_type_for_reverse_order.end()) {
    return it->second;
  }

  return scan_type;
}

JoinMode translate_join_type_to_join_mode(const hsql::JoinType join_type) {
  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinOuter, JoinMode::Outer},     {hsql::kJoinLeft, JoinMode::Left},
      {hsql::kJoinRight, JoinMode::Right}, {hsql::kJoinNatural, JoinMode::Natural}, {hsql::kJoinCross, JoinMode::Cross},
  };

  auto it = join_type_to_mode.find(join_type);
  DebugAssert(it != join_type_to_mode.end(), "Unable to handle join type.");
  return it->second;
}

SQLToASTTranslator& SQLToASTTranslator::get() {
  static SQLToASTTranslator instance;
  return instance;
}

std::vector<std::shared_ptr<AbstractASTNode>> SQLToASTTranslator::translate_parse_result(
    const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractASTNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::translate_statement(const hsql::SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return _translate_select((const hsql::SelectStatement&)statement);
    default:
      Fail("Only SELECT statements are supported as of now.");
      return {};
  }
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_select(const hsql::SelectStatement& select) {
  // SQL Order of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause (incl. JOINs and subselects that are part of this)
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
  DebugAssert(select.selectList != nullptr, "SELECT list needs to exist");
  DebugAssert(!select.selectList->empty(), "SELECT list needs to have entries");

  // If the query has a GROUP BY clause or if it has aggregates, we do not need a top-level projection
  // because all elements must either be aggregate functions or columns of the GROUP BY clause,
  // so the Aggregate operator will handle them.
  auto is_aggregate = select.groupBy != nullptr;
  if (!is_aggregate) {
    for (auto* expr : *select.selectList) {
      // TODO(anybody): Only consider aggregate functions here (i.e., SUM, COUNT, etc. - but not CONCAT, ...).
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

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_join(const hsql::JoinDefinition& join) {
  auto left_node = _translate_table_ref(*join.left);
  auto right_node = _translate_table_ref(*join.right);

  const hsql::Expr& condition = *join.condition;
  std::pair<std::string, std::string> column_names(generate_column_name(*condition.expr, false),
                                                   generate_column_name(*condition.expr2, false));

  // Joins currently only support one simple condition (i.e., not multiple conditions).
  auto scan_type = translate_operator_type_to_scan_type(condition.opType);
  auto join_mode = translate_join_type_to_join_mode(join.type);

  std::string prefix_left = std::string(join.left->getName()) + ".";
  std::string prefix_right = std::string(join.right->getName()) + ".";

  auto join_node = std::make_shared<JoinNode>(join_mode, prefix_left, prefix_right, column_names, scan_type);
  join_node->set_left_child(left_node);
  join_node->set_right_child(right_node);

  return join_node;
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_cross_product(
    const std::vector<hsql::TableRef*>& tables) {
  DebugAssert(!tables.empty(), "Cannot translate cross product without tables");
  auto product = _translate_table_ref(*tables.front());

  for (size_t i = 1; i < tables.size(); i++) {
    auto next_node = _translate_table_ref(*tables[i]);

    auto new_product = std::make_shared<JoinNode>(JoinMode::Cross, "", "");
    new_product->set_left_child(product);
    new_product->set_right_child(next_node);

    product = new_product;
  }

  return product;
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_table_ref(const hsql::TableRef& table) {
  switch (table.type) {
    case hsql::kTableName:
      return std::make_shared<StoredTableNode>(table.name);
    case hsql::kTableSelect:
      return _translate_select(*table.select);
    case hsql::kTableJoin:
      return _translate_join(*table.join);
    case hsql::kTableCrossProduct:
      return _translate_cross_product(*table.list);
    default:
      Fail("Unable to translate source table.");
      return {};
  }
}

std::string SQLToASTTranslator::generate_column_name(const hsql::Expr& expr, bool include_table_name) {
  std::string name;

  // Translate an aggregate function to a string that the Aggregate operator generates.
  if (expr.isType(hsql::kExprFunctionRef)) {
    name += expr.name;
    name += "(";
    name += generate_column_name(*expr.exprList->at(0), include_table_name);
    name += ")";
    return name;
  }

  DebugAssert(expr.isType(hsql::kExprColumnRef), "Expected column reference.");

  if (include_table_name && expr.hasTable()) {
    name += std::string(expr.table) + ".";
  }

  name += expr.name;

  DebugAssert(!name.empty(), "Column name is empty.");

  return name;
}

AllParameterVariant SQLToASTTranslator::translate_argument(const hsql::Expr& expr) {
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
      return ColumnName(generate_column_name(expr, true));
    default:
      Fail("Could not translate literal: expression type not supported.");
      return {};
  }
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_filter_expr(
    const hsql::Expr& expr, const std::shared_ptr<AbstractASTNode>& input_node) {
  DebugAssert(expr.isType(hsql::kExprOperator), "Filter expression clause has to be of type operator!");

  // If the expression is a nested expression, recursively resolve.
  // TODO(anybody): implement OR.
  DebugAssert(expr.opType != hsql::kOpOr, "OR is currently not supported by SQLToASTTranslator");

  if (expr.opType == hsql::kOpAnd) {
    auto filter_node = _translate_filter_expr(*expr.expr, input_node);
    return _translate_filter_expr(*expr.expr2, filter_node);
  }

  // TODO(anybody): handle IN with join
  auto scan_type = translate_operator_type_to_scan_type(expr.opType);

  /**
   * We have to determine which side of the expression is a column or function reference.
   * That's because the WHERE clause could be `a = 5`, but it could also be `5 = a`.
   * However, it is currently required in the TableScan operator that one side is a column.
   *
   * We accept functions here because we assume they have been translated by Aggregate.
   * They will be treated as a regular column of the same name.
   * If this assumption is incorrect, the translation will fail because the column won't be found.
   *
   * Supported:
   *   SELECT a, SUM(B) FROM t GROUP BY a HAVING SUM(B) > 0
   * This query is fine because the expression used in the HAVING clause is part of the SELECT list.
   * We first translate the SELECT list, which will result in an Aggregate operator that creates a column for the sum.
   * We can subsequently access that column when we translate the HAVING expression here.
   *
   * Unsupported:
   *   SELECT a, SUM(B) FROM t GROUP BY a HAVING AVG(B) > 0
   * This query cannot be translated at the moment because the Aggregate does not produce an output column for the AVG.
   * Therefore, the filter expression cannot be translated, because the TableScan operator is not able to compute
   * aggregates on its own.
   *
   * TODO(anybody): extend support for those HAVING clauses.
   * One option is to add them to the Aggregate and then use a Projection to remove them from the result.
   */
  hsql::Expr* column_operand_expr = nullptr;
  hsql::Expr* argument_expr = nullptr;
  if (expr.expr->isType(hsql::kExprColumnRef) || expr.expr->isType(hsql::kExprFunctionRef)) {
    column_operand_expr = expr.expr;
    argument_expr = expr.expr2;
  } else {
    /**
     * TODO(anybody): think about how this can be supported as well.
     *
     * * Example:
     *     SELECT * FROM t WHERE 1 BETWEEN a AND 3
     *  -> SELECT * FROM t WHERE a <= 1
     *
     *     SELECT * FROM t WHERE 3 BETWEEN 1 AND a
     *  -> SELECT * FROM t WHERE a >= 3
     *
     *  The biggest question is how to introduce this in the code nicely.
     */
    DebugAssert(scan_type != ScanType::OpBetween,
                "Currently the term left of the BETWEEN expression needs to be a column reference.");

    argument_expr = expr.expr;
    column_operand_expr = expr.expr2;

    DebugAssert(
        column_operand_expr->isType(hsql::kExprColumnRef) || column_operand_expr->isType(hsql::kExprFunctionRef),
        "Unsupported filter: we must have a function or column reference on at least one side of the expression.");

    // We might have to change the ScanType when we reverse the sides of the expression.
    scan_type = get_scan_type_for_reverse_order(scan_type);
  }

  const auto column_name = generate_column_name(*column_operand_expr, true);

  AllParameterVariant argument;
  optional<AllTypeVariant> argument2;

  if (scan_type == ScanType::OpBetween) {
    Assert(expr.exprList->size() == 2, "Need two arguments for BETWEEEN");

    const auto* left_expr = (*expr.exprList)[0];
    const auto* right_expr = (*expr.exprList)[1];

    argument = translate_argument(*left_expr);

    // TODO(anybody): TableScan does not support AllParameterVariant as second value.
    // This would be required to prepare BETWEEN, or to do a BETWEEN scan for three columns (a BETWEEN b and c).
    argument2 = boost::get<AllTypeVariant>(translate_argument(*right_expr));
  } else {
    argument = translate_argument(*argument_expr);
  }

  std::shared_ptr<ExpressionNode> expression_node = SQLExpressionTranslator::translate_expression(expr);
  auto predicate_node = std::make_shared<PredicateNode>(column_name, expression_node, scan_type, argument, argument2);

  predicate_node->set_left_child(input_node);

  return predicate_node;
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_aggregate(
    const hsql::SelectStatement& select, const std::shared_ptr<AbstractASTNode>& input_node) {
  const auto& select_list = *select.selectList;
  const auto* group_by = select.groupBy;

  /**
   * Build Aggregates
   */
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  aggregate_column_definitions.reserve(select_list.size());

  for (const auto* column_expr : select_list) {
    if (column_expr->isType(hsql::kExprFunctionRef)) {
      auto opossum_expr = SQLExpressionTranslator().translate_expression(*column_expr);

      optional<std::string> alias;
      if (column_expr->alias) {
        alias = std::string(column_expr->alias);
      }

      aggregate_column_definitions.emplace_back(opossum_expr, alias);
    } else if (column_expr->isType(hsql::kExprColumnRef)) {
      /**
       * This if block is a SQL conformity check, whether column references in SELECT list of
       * aggregates appears in GROUP BY clause
       */

      // If the item is a column, it has to be in the GROUP BY clause.
      Assert(group_by != nullptr,
             "SELECT list of aggregate contains a column, but the query does not have a GROUP BY clause.");

      auto expr_name = column_expr->getName();

      auto is_in_group_by_clause = false;
      for (const auto* groupby_expr : *group_by->columns) {
        if (strcmp(expr_name, groupby_expr->getName()) == 0) {
          is_in_group_by_clause = true;
          break;
        }
      }

      Assert(is_in_group_by_clause,
             std::string("Column '") + expr_name + "' is specified in SELECT list, but not in GROUP BY clause.");
    } else {
      Fail("Unsupported item in projection list for AggregateOperator.");
    }
  }

  /**
   * Build GROUP BY
   */
  std::vector<std::string> groupby_columns;
  if (group_by != nullptr) {
    groupby_columns.reserve(group_by->columns->size());
    for (const auto* groupby_expr : *group_by->columns) {
      DebugAssert(groupby_expr->isType(hsql::kExprColumnRef), "Only column ref GROUP BYs supported at the moment");
      DebugAssert(groupby_expr->name != nullptr, "Column name for GROUP BY expression is not set.");

      groupby_columns.emplace_back(generate_column_name(*groupby_expr, true));
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

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_projection(
    const std::vector<hsql::Expr*>& select_list, const std::shared_ptr<AbstractASTNode>& input_node) {
  Projection::ColumnExpressions column_expressions;
  for (const auto* hsql_expr : select_list) {
    // TODO(mp): expressions
    const auto expression = SQLExpressionTranslator::translate_expression(*hsql_expr);

    DebugAssert(expression->type() == ExpressionType::Star || expression->type() == ExpressionType::ColumnIdentifier ||
                    expression->is_arithmetic_operator(),
                "Only column references, star-selects, and arithmetic expressions supported for now.");

    if (expression->type() == ExpressionType::Star) {
      // Resolve `SELECT *` to columns.
      const auto& column_references = ExpressionNode::create_column_identifiers(input_node->output_column_names());
      column_expressions.insert(column_expressions.end(), column_references.cbegin(), column_references.cend());
    } else {
      column_expressions.emplace_back(expression);
    }
  }

  auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
  projection_node->set_left_child(input_node);

  return projection_node;
}

std::shared_ptr<AbstractASTNode> SQLToASTTranslator::_translate_order_by(
    const std::vector<hsql::OrderDescription*>& order_list, const std::shared_ptr<AbstractASTNode>& input_node) {
  if (order_list.empty()) {
    return input_node;
  }

  auto current_result_node = input_node;

  // Go through all the order descriptions and create a sort node for each of them.
  // Iterate in reverse because the sort operator does not support multiple columns,
  // and instead relies on stable sort. We therefore sort by the n+1-th column before sorting by the n-th column.
  for (auto it = order_list.rbegin(); it != order_list.rend(); ++it) {
    auto order_description = *it;
    const auto& order_expr = *order_description->expr;

    // TODO(anybody): handle non-column refs
    DebugAssert(order_expr.isType(hsql::kExprColumnRef), "Can only order by columns for now.");

    const auto column_name = generate_column_name(order_expr, true);
    const auto asc = order_description->type == hsql::kOrderAsc;

    auto sort_node = std::make_shared<SortNode>(column_name, asc);
    sort_node->set_left_child(current_result_node);
    current_result_node = sort_node;
  }

  return current_result_node;
}

}  // namespace opossum
