#include "sql_translator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/not_expression.hpp"
#include "expression/value_expression.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "sql/hsql_expr_translator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "util/sqlhelper.h"
#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

JoinMode translate_join_mode(const hsql::JoinType join_type) {
  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinFull, JoinMode::Outer},      {hsql::kJoinLeft, JoinMode::Left},
      {hsql::kJoinRight, JoinMode::Right}, {hsql::kJoinNatural, JoinMode::Natural}, {hsql::kJoinCross, JoinMode::Cross},
  };

  auto it = join_type_to_mode.find(join_type);
  DebugAssert(it != join_type_to_mode.end(), "Unable to handle join type.");
  return it->second;
}

std::vector<std::shared_ptr<AbstractLQPNode>> SQLTranslator::translate_parse_result(
    const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractLQPNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::translate_statement(const hsql::SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return translate_select(static_cast<const hsql::SelectStatement &>(statement));
    case hsql::kStmtInsert:
      return _translate_insert(static_cast<const hsql::InsertStatement&>(statement));
    case hsql::kStmtDelete:
      return _translate_delete(static_cast<const hsql::DeleteStatement&>(statement));
    case hsql::kStmtUpdate:
      return _translate_update(static_cast<const hsql::UpdateStatement&>(statement));
    case hsql::kStmtShow:
      return _translate_show(static_cast<const hsql::ShowStatement&>(statement));
    case hsql::kStmtCreate:
      return _translate_create(static_cast<const hsql::CreateStatement&>(statement));
    case hsql::kStmtDrop:
      return _translate_drop(static_cast<const hsql::DropStatement&>(statement));
    default:
      Fail("SQL statement type not supported");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_insert(const hsql::InsertStatement& insert) {
  const std::string table_name{insert.tableName};
  auto target_table = StorageManager::get().get_table(table_name);

  Assert(target_table != nullptr, "Insert: Invalid table name");

  std::shared_ptr<AbstractLQPNode> current_result_node;

  // Check for SELECT ... INTO .. query
  if (insert.type == hsql::kInsertSelect) {
    DebugAssert(insert.select != nullptr, "Insert: no select statement given");
    current_result_node = translate_select(*insert.select);
  } else {
    current_result_node = DummyTableNode::make();
  }

  // Lambda to compare a DataType to the type of an hqsl::Expr
  auto literal_matches_data_type = [](const hsql::Expr& expr, const DataType& column_type) {
    switch (column_type) {
      case DataType::Int:
        return expr.isType(hsql::kExprLiteralInt);
      case DataType::Long:
        return expr.isType(hsql::kExprLiteralInt);
      case DataType::Float:
        return expr.isType(hsql::kExprLiteralFloat);
      case DataType::Double:
        return expr.isType(hsql::kExprLiteralFloat);
      case DataType::String:
        return expr.isType(hsql::kExprLiteralString);
      case DataType::Null:
        return expr.isType(hsql::kExprLiteralNull);
      default:
        return false;
    }
  };

  // Lambda to compare a vector of DataType to the types of a vector of hqsl::Expr
  auto data_types_match_expr_types = [&](const std::vector<DataType>& data_types,
                                         const std::vector<hsql::Expr*>& expressions) {
    auto data_types_it = data_types.begin();
    auto expressions_it = expressions.begin();

    while (data_types_it != data_types.end() && expressions_it != expressions.end()) {
      if (!literal_matches_data_type(*(*expressions_it), *data_types_it) &&
          !(*expressions_it)->isType(hsql::kExprParameter)) {
        // if this is a PreparedStatement, we don't have a mismatch
        return false;
      }
      data_types_it++;
      expressions_it++;
    }

    return true;
  };

  if (!insert.columns) {
    // No column order given. Assuming all columns in regular order.
    // For SELECT ... INTO we are basically done because can use the above node as input.

    if (insert.type == hsql::kInsertValues) {
      DebugAssert(insert.values != nullptr, "Insert: no values given");

      Assert(data_types_match_expr_types(target_table->column_data_types(), *insert.values),
             "Insert: Column type mismatch");

      // In the case of INSERT ... VALUES (...), simply create a
      current_result_node = _translate_projection(*insert.values, current_result_node);
    }

    Assert(current_result_node->output_column_count() == target_table->column_count(), "Insert: Column count mismatch");
  } else {
    // Certain columns have been specified. In this case we create a new expression list
    // for the Projection, so that it contains as many columns as the target table.

    // pre-fill new projection list with NULLs
    std::vector<std::shared_ptr<LQPExpression>> projections(target_table->column_count(),
                                                            LQPExpression::create_literal(NULL_VALUE));

    ColumnID insert_column_index{0};
    for (const auto& column_name : *insert.columns) {
      // retrieve correct ColumnID from the target table
      auto column_id = target_table->column_id_by_name(column_name);

      if (insert.type == hsql::kInsertValues) {
        // when inserting values, simply translate the literal expression
        const auto& hsql_expr = *(*insert.values)[insert_column_index];

        Assert(literal_matches_data_type(hsql_expr, target_table->column_data_types()[column_id]),
               "Insert: Column type mismatch");

        projections[column_id] = HSQLExprTranslator::to_lqp_expression(hsql_expr, nullptr);
      } else {
        DebugAssert(insert.type == hsql::kInsertSelect, "Unexpected Insert type");
        DebugAssert(insert_column_index < current_result_node->output_column_count(), "ColumnID out of range");
        // when projecting from another table, create a column reference expression
        projections[column_id] =
            LQPExpression::create_column(current_result_node->output_column_references()[insert_column_index]);
      }

      ++insert_column_index;
    }

    // create projection and add to the node chain
    auto projection_node = ProjectionNode::make(projections);
    projection_node->set_left_input(current_result_node);

    current_result_node = projection_node;
  }

  auto insert_node = InsertNode::make(table_name);
  insert_node->set_left_input(current_result_node);

  return insert_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_delete(const hsql::DeleteStatement& del) {
  std::shared_ptr<AbstractLQPNode> current_result_node = StoredTableNode::make(del.tableName);
  current_result_node = _validate_if_active(current_result_node);
  if (del.expr) {
    current_result_node = _translate_where(*del.expr, current_result_node);
  }

  auto delete_node = DeleteNode::make(del.tableName);
  delete_node->set_left_input(current_result_node);

  return delete_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_update(const hsql::UpdateStatement& update) {
  std::shared_ptr<AbstractLQPNode> current_values_node = _translate_table_ref(*update.table);
  if (update.where) {
    current_values_node = _translate_where(*update.where, current_values_node);
  }

  // The update operator wants ReferenceColumns on its left side
  // TODO(anyone): fix this
  Assert(!std::dynamic_pointer_cast<StoredTableNode>(current_values_node),
         "Unconditional updates are currently not supported");

  std::vector<std::shared_ptr<LQPExpression>> update_expressions;
  update_expressions.reserve(current_values_node->output_column_count());

  // pre-fill with regular column references
  for (ColumnID column_idx{0}; column_idx < current_values_node->output_column_count(); ++column_idx) {
    update_expressions.emplace_back(
        LQPExpression::create_column(current_values_node->output_column_references()[column_idx]));
  }

  // now update with new values
  for (auto& sql_expr : *update.updates) {
    const auto named_column_ref = QualifiedColumnName{sql_expr->column, std::nullopt};
    const auto column_reference = current_values_node->get_column(named_column_ref);
    const auto column_id = current_values_node->get_output_column_id(column_reference);

    auto expr = HSQLExprTranslator::to_lqp_expression(*sql_expr->value, current_values_node);
    update_expressions[column_id] = expr;
  }

  std::shared_ptr<AbstractLQPNode> update_node = UpdateNode::make((update.table)->name, update_expressions);
  update_node->set_left_input(current_values_node);

  return update_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::translate_select(const hsql::SelectStatement &select) {
  // SQL Orders of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause (incl. JOINs and subselects that are part of this)
  // 2. WHERE clause
  // 3. GROUP BY clause
  // 4. HAVING clause
  // 5. SELECT clause
  // 6. UNION clause
  // 7. ORDER BY clause
  // 8. LIMIT clause

  Assert(select.selectList != nullptr, "SELECT list needs to exist");
  Assert(!select.selectList->empty(), "SELECT list needs to have entries");
  Assert(select.unionSelect == nullptr, "Set operations (UNION/INTERSECT/...) are not supported yet");
  Assert(!select.selectDistinct, "DISTINCT is not yet supported");

  auto current_result_node = _translate_table_ref(*select.fromTable);

  if (select.whereClause != nullptr) {
    current_result_node = _translate_predicates(*select.whereClause, current_result_node);
  }

  current_result_node = _translate_expressions(select, current_result_node);

  if (select.order != nullptr) {
    current_result_node = _translate_order_by(*select.order, current_result_node);
  }

  if (select.limit != nullptr) {
    current_result_node = _translate_limit(*select.limit, current_result_node);
  }

  return current_result_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_join(const hsql::JoinDefinition& join) {
  const auto join_mode = translate_join_mode(join.type);

  if (join_mode == JoinMode::Natural) {
    return _translate_natural_join(join);
  }

  auto left_node = _translate_table_ref(*join.left);
  auto right_node = _translate_table_ref(*join.right);

  const hsql::Expr& condition = *join.condition;

  Assert(condition.type == hsql::kExprOperator, "Join condition must be operator.");
  // The Join operators only support simple comparisons for now.
  switch (condition.opType) {
    case hsql::kOpEquals:
    case hsql::kOpNotEquals:
    case hsql::kOpLess:
    case hsql::kOpLessEq:
    case hsql::kOpGreater:
    case hsql::kOpGreaterEq:
      break;
    default:
      Fail("Join condition must be a simple comparison operator.");
  }
  Assert(condition.expr && condition.expr->type == hsql::kExprColumnRef,
         "Left arg of join condition must be column ref");
  Assert(condition.expr2 && condition.expr2->type == hsql::kExprColumnRef,
         "Right arg of join condition must be column ref");

  const auto left_qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*condition.expr);
  const auto right_qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*condition.expr2);

  /**
   * `x_in_y_node` indicates whether the column identifier on the `x` side in the join expression is in the input node
   * on
   * the `y` side of the join. So in the query
   * `SELECT * FROM T1 JOIN T2 on person_id == customer_id`
   * We have to check whether `person_id` belongs to T1 (left_in_left_node == true) or to T2
   * (left_in_right_node == true). Later we make sure that one and only one of them is true, otherwise we either have
   * ambiguity or the column is simply not existing.
   */
  const auto left_in_left_node = left_node->find_column(left_qualified_column_name);
  const auto left_in_right_node = right_node->find_column(left_qualified_column_name);
  const auto right_in_left_node = left_node->find_column(right_qualified_column_name);
  const auto right_in_right_node = right_node->find_column(right_qualified_column_name);

  Assert(static_cast<bool>(left_in_left_node) ^ static_cast<bool>(left_in_right_node),
         std::string("Left operand ") + left_qualified_column_name.as_string() +
             " must be in exactly one of the input nodes");
  Assert(static_cast<bool>(right_in_left_node) ^ static_cast<bool>(right_in_right_node),
         std::string("Right operand ") + right_qualified_column_name.as_string() +
             " must be in exactly one of the input nodes");

  const auto column_references = left_in_left_node ? std::make_pair(*left_in_left_node, *right_in_right_node)
                                                   : std::make_pair(*left_in_right_node, *right_in_left_node);

  // Joins currently only support one simple condition (i.e., not multiple conditions).
  auto predicate_condition = translate_operator_type_to_predicate_condition(condition.opType);

  auto join_node = JoinNode::make(join_mode, column_references, predicate_condition);
  join_node->set_left_input(left_node);
  join_node->set_right_input(right_node);

  return join_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_natural_join(const hsql::JoinDefinition& join) {
  DebugAssert(translate_join_mode(join.type) == JoinMode::Natural, "join must be a natural join");

  const auto& left_node = _translate_table_ref(*join.left);
  const auto& right_node = _translate_table_ref(*join.right);

  // we need copies that we can sort on.
  auto left_column_names = left_node->output_column_names();
  auto right_column_names = right_node->output_column_names();

  std::sort(left_column_names.begin(), left_column_names.end());
  std::sort(right_column_names.begin(), right_column_names.end());

  std::vector<std::string> join_column_names;
  std::set_intersection(left_column_names.begin(), left_column_names.end(), right_column_names.begin(),
                        right_column_names.end(), std::back_inserter(join_column_names));

  Assert(!join_column_names.empty(), "No matching columns for natural join found");

  std::shared_ptr<AbstractLQPNode> return_node = JoinNode::make(JoinMode::Cross);
  return_node->set_left_input(left_node);
  return_node->set_right_input(right_node);

  for (const auto& join_column_name : join_column_names) {
    auto left_column_reference = left_node->get_column({join_column_name});
    auto right_column_reference = right_node->get_column({join_column_name});
    auto predicate = PredicateNode::make(left_column_reference, PredicateCondition::Equals, right_column_reference);
    predicate->set_left_input(return_node);
    return_node = predicate;
  }

  // We need to collect the column origins so that we can remove the duplicate columns used in the join condition
  std::vector<LQPColumnReference> column_references;
  for (auto column_id = ColumnID{0u}; column_id < return_node->output_column_count(); ++column_id) {
    const auto& column_name = return_node->output_column_names()[column_id];

    if (static_cast<size_t>(column_id) >= left_node->output_column_count() &&
        std::find(join_column_names.begin(), join_column_names.end(), column_name) != join_column_names.end()) {
      continue;
    }

    const auto& column_reference = return_node->output_column_references()[column_id];
    column_references.emplace_back(column_reference);
  }

  const auto column_expressions = LQPExpression::create_columns(column_references);

  auto projection = ProjectionNode::make(column_expressions);
  projection->set_left_input(return_node);

  return projection;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_cross_product(const std::vector<hsql::TableRef*>& tables) {
  DebugAssert(!tables.empty(), "Cannot translate cross product without tables");
  auto product = _translate_table_ref(*tables.front());

  for (size_t i = 1; i < tables.size(); i++) {
    auto next_node = _translate_table_ref(*tables[i]);

    auto new_product = JoinNode::make(JoinMode::Cross);
    new_product->set_left_input(product);
    new_product->set_right_input(next_node);

    product = new_product;
  }

  return product;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_table_ref_alias(const std::shared_ptr<AbstractLQPNode>& node,
                                                                           const hsql::TableRef& table) {
  // Add a new projection node for table alias with column alias declarations
  // e.g. select * from foo as bar(a, b)
  if (!table.alias || !table.alias->columns) {
    return node;
  }

  DebugAssert(table.type == hsql::kTableName || table.type == hsql::kTableSelect,
              "Aliases are only applicable to table names and subselects");

  // To stick to the sql standard there must be an alias for every column of the renamed table
  // https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt 6.3
  Assert(table.alias->columns->size() == node->output_column_count(),
         "The number of column aliases must match the number of columns");

  auto& column_references = node->output_column_references();
  std::vector<std::shared_ptr<LQPExpression>> projections;
  projections.reserve(table.alias->columns->size());
  size_t column_id = 0;
  for (const char* column : *(table.alias->columns)) {
    projections.push_back(LQPExpression::create_column(column_references.at(column_id), std::string(column)));
    ++column_id;
  }
  auto projection_node = ProjectionNode::make(projections);
  projection_node->set_left_input(node);
  return projection_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_table_ref(const hsql::TableRef& table) {
  auto alias = table.alias ? std::optional<std::string>(table.alias->name) : std::nullopt;
  std::shared_ptr<AbstractLQPNode> node;
  switch (table.type) {
    case hsql::kTableName:
      if (StorageManager::get().has_table(table.name)) {
        /**
         * Make sure the ALIAS is applied to the StoredTableNode and not the ValidateNode
         */
        auto stored_table_node = StoredTableNode::make(table.name);
        stored_table_node->set_alias(alias);
        return _translate_table_ref_alias(_validate_if_active(stored_table_node), table);
      } else if (StorageManager::get().has_view(table.name)) {
        node = StorageManager::get().get_view(table.name);
        Assert(!_validate || node->subplan_is_validated(), "Trying to add non-validated view to validated query");
      } else {
        Fail(std::string("Did not find a table or view with name ") + table.name);
      }
      break;
    case hsql::kTableSelect:
      node = translate_select(*table.select);
      Assert(alias, "Every derived table must have its own alias");
      node = _translate_table_ref_alias(node, table);
      break;
    case hsql::kTableJoin:
      node = _translate_join(*table.join);
      break;
    case hsql::kTableCrossProduct:
      node = _translate_cross_product(*table.list);
      break;
    default:
      Fail("Unable to translate source table.");
  }

  node->set_alias(alias);
  return node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_where(const hsql::Expr& expr,
                                                                 const std::shared_ptr<AbstractLQPNode>& input_node) {
  DebugAssert(expr.isType(hsql::kExprOperator), "Filter expression clause has to be of type operator!");

  /**
   * If the expression is a nested expression, recursively resolve
   */
  if (expr.opType == hsql::kOpOr) {
    auto union_unique_node = UnionNode::make(UnionMode::Positions);
    union_unique_node->set_left_input(_translate_where(*expr.expr, input_node));
    union_unique_node->set_right_input(_translate_where(*expr.expr2, input_node));
    return union_unique_node;
  }

  if (expr.opType == hsql::kOpAnd) {
    auto filter_node = _translate_where(*expr.expr, input_node);
    return _translate_where(*expr.expr2, filter_node);
  }

  return _translate_predicate(
      expr, false,
      [&](const hsql::Expr& hsql_expr) { return HSQLExprTranslator::to_column_reference(hsql_expr, input_node); },
      input_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_expressions(
const hsql::SelectStatement &select, const std::shared_ptr<AbstractLQPNode> &input_node) {
  auto current_node = input_node;

  // Gather all expression required for GROUP BY
  auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>();
  if (select.groupBy && select.groupBy->columns) {
    group_by_expressions.reserve(select.groupBy->columns->size());
    for (const auto* group_by_hsql_expr : *select.groupBy->columns) {
      group_by_expressions.emplace_back(HSQLExprTranslator::to_lqp_expression(*group_by_hsql_expr, input_node));
    }
  }

  auto input_and_group_by_expressions = current_node->output_column_expressions();

  current_node = ProjectionNode::make(pre_group_by_expressions, current_node);

  auto pre_groupby_projection = current_node;

  // Identify all expressions needed for SELECT and HAVING
  auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto select_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  for (const auto& hsql_select_expr : *select.selectList) {
    if (hsql_select_expr->type == hsql::kExprStar) continue;

    const auto expression = HSQLExprTranslator::to_lqp_expression(*hsql_select_expr, current_node);
    expressions.emplace_back(expression);
    select_expressions.emplace_back(expression);
  }

  // Identify pre aggregate expressions
  auto pre_aggregate_expressions = current_node->output_column_expressions();
  for (const auto& expression : expressions) {
    visit_expression(*expression, [&](auto& sub_expression) {
      if (sub_expression.type != ExpressionType::Aggregate) return true;
      const auto& aggregate_expression = std::static_pointer_cast<AggregateExpression>(expression);

      for (const auto& argument : aggregate_expression->arguments) {
        if (argument->requires_calculation()) {
          pre_aggregate_expressions.emplace_back(argument);
        }
      }
      return false;
    });
  }

  // Build pre_aggregate_projection
  current_node = ProjectionNode::make(pre_aggregate_expressions, current_node);

  // Build aggregate expressions accessing the pre aggregate expressions
  std::vector<std::shared_ptr<const LQPExpression>> aggregate_expressions;
  for (auto& expression : expressions) {
    visit_expression(*expression, [&](auto& expression) {
      if (expression.type() != ExpressionType::Aggregate) return true;
      const auto& aggregate_expression = std::static_pointer_cast<AggregateExpression>(expression);

      for (const auto& argument : aggregate_expression->arguments) {
        if (argument->requires_calculation()) {
          const auto column = current_node->get_column(*argument);
          argument = std::make_shared<LQPColumnExpression>(column);
        }
      }

      aggregate_expressions.emplace_back(aggregate_expression);

      return false;
    });
  }

  // Build groupby column references accessing the pre aggregate expressions
  std::vector<LQPColumnReference> group_by_columns;
  group_by_columns.reserve(group_by_expressions.size());
  for (auto& group_by_expression : group_by_expressions) {
    group_by_columns.emplace_back(current_node->get_column(*group_by_expression));
  }

  // Build Aggregate
  if (!aggregate_expressions.empty() || !group_by_columns.empty()) {
    current_node = AggregateNode::make(group_by_columns, aggregate_expressions, current_node);
  }

  // Build having
  if (select.groupBy && select.groupBy->having) {
    current_node = _translate_predicate(*select.groupBy->having, current_node);
  }


  // Make SELECT expressions use Aggregate results
  auto post_aggregate_expressions = select_expressions;
  for (auto& select_expression : post_aggregate_expressions) {
    visit_expression(select_expression, [&](auto& expression) {
      if (expression.type() != ExpressionType::Aggregate) return true;
      expression = std::make_shared<LQPColumnExpression>(current_node->get_column(expression));

      return false;
    });
  }

  post_aggregate_expressions.insert(post_aggregate_expressions.end(),
                                    group_by_expressions.begin(),
                                    group_by_expressions.end());
  post_aggregate_expressions.insert(post_aggregate_expressions.end(),
                                    aggregate_expressions.begin(),
                                    aggregate_expressions.end());

  current_node = ProjectionNode::make(post_aggregate_expressions, current_node);

  //
  std::vector<std::shared_ptr<LQPExpression>> output_expressions;
  for (const auto& hsql_expr : *select.selectList) {
    if (hsql_expr->type == hsql::kExprStar) {
      if (hsql_expr->table) {
        /**
         * Otherwise only take columns that belong to that qualifier.
         *
         * Consider `SELECT t1.* FROM (SELECT a,b FROM t) AS t1`
         *
         * First, we retrieve the node (`origin_node`) that "creates" "t1". Then, in the for loop, for every Column that
         * `origin_node` outputs, we check whether it "reaches" the input_node
         * (it may get discarded by a Projection/Aggregate along the way). If it is still contained in the input_node
         * it gets added to the list of Columns that the Projection outputs.
         */
        auto origin_node = input_node->find_table_name_origin(hsql_expr->table);
        Assert(origin_node, "Couldn't resolve '" + std::string(hsql_expr->table) + "'.*");

        for (const auto& origin_column : origin_node->output_column_references()) {
          const auto input_node_column_id = input_node->find_output_column_id(origin_column);
          if (input_node_column_id) {
            output_expressions.emplace_back(std::make_shared<LQPColumnExpression>(origin_column));
          }
        }
      } else {
        // If there is no table qualifier take all columns from the input.
        for (const auto& column : input_node->output_column_references()) {
          output_expressions.emplace_back(std::make_shared<LQPColumnExpression>(column));
        }
      }
    }
  }
  current_node = ProjectionNode::make(output_expressions, current_node);

  return current_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_order_by(
    const std::vector<hsql::OrderDescription*>& order_list, const std::shared_ptr<AbstractLQPNode>& input_node) {
  if (order_list.empty()) {
    return input_node;
  }

  std::vector<OrderByDefinition> order_by_definitions;
  order_by_definitions.reserve(order_list.size());

  for (const auto& order_description : order_list) {
    const auto& order_expr = *order_description->expr;

    // TODO(anybody): handle non-column refs
    DebugAssert(order_expr.isType(hsql::kExprColumnRef), "Can only order by columns for now.");

    const auto column_reference = HSQLExprTranslator::to_column_reference(order_expr, input_node);
    const auto order_by_mode = order_type_to_order_by_mode.at(order_description->type);

    order_by_definitions.emplace_back(column_reference, order_by_mode);
  }

  auto sort_node = SortNode::make(order_by_definitions);
  sort_node->set_left_input(input_node);

  return sort_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_limit(const hsql::LimitDescription& limit,
                                                                 const std::shared_ptr<AbstractLQPNode>& input_node) {
  auto limit_node = LimitNode::make(limit.limit);
  limit_node->set_left_input(input_node);
  return limit_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_predicate(
    const hsql::Expr& hsql_expr, bool allow_function_columns,
    const std::function<LQPColumnReference(const hsql::Expr&)>& resolve_column,
    const std::shared_ptr<AbstractLQPNode>& input_node) {
  DebugAssert(hsql_expr.expr != nullptr, "hsql malformed");

  const auto refers_to_column = [allow_function_columns](const hsql::Expr& hsql_expr) {
    return hsql_expr.isType(hsql::kExprColumnRef) ||
           (allow_function_columns && hsql_expr.isType(hsql::kExprFunctionRef));
  };

  auto predicate_negated = (hsql_expr.opType == hsql::kOpNot);

  const auto* column_ref_hsql_expr = hsql_expr.expr;
  PredicateCondition predicate_condition;

  if (predicate_negated) {
    Assert(hsql_expr.expr != nullptr, "NOT operator without further expressions");
    predicate_condition = translate_operator_type_to_predicate_condition(hsql_expr.expr->opType);

    /**
     * It should be possible for any predicate to be negated with "NOT",
     * e.g., WHERE NOT a > 5. However, this is currently not supported.
     * Right now we only use `kOpNot` to detect and set the `OpIsNotNull` predicate condition.
     */
    Assert(predicate_condition == PredicateCondition::IsNull, "Only IS NULL can be negated");

    if (predicate_condition == PredicateCondition::IsNull) {
      predicate_condition = PredicateCondition::IsNotNull;
    }

    // change column reference to the correct expression
    column_ref_hsql_expr = hsql_expr.expr->expr;
  } else {
    predicate_condition = translate_operator_type_to_predicate_condition(hsql_expr.opType);
  }

  // Indicates whether to use expr.expr or expr.expr2 as the main column to reference
  auto operands_switched = false;

  /**
   * value_ref_hsql_expr = the expr referring to the value of the scan, e.g. the 5 in `WHERE 5 > p_income`, but also
   * the secondary column p_b in a scan like `WHERE p_a > p_b`
   */
  const hsql::Expr* value_ref_hsql_expr = nullptr;

  std::optional<AllTypeVariant> value2;  // Left uninitialized for predicates that are not BETWEEN

  if (predicate_condition == PredicateCondition::Between) {
    /**
     * Translate expressions of the form `column_or_aggregate BETWEEN value AND value2`.
     * Both value and value2 can be any kind of literal, while value might also be a column or a placeholder.
     * As per the TODO below, value2 cannot be neither of those, YET
     */

    Assert(hsql_expr.exprList->size() == 2, "Need two arguments for BETWEEEN");

    const auto* expr0 = (*hsql_expr.exprList)[0];
    const auto* expr1 = (*hsql_expr.exprList)[1];
    DebugAssert(expr0 != nullptr && expr1 != nullptr, "hsql malformed");

    value_ref_hsql_expr = expr0;

    // TODO(anybody): TableScan does not support AllParameterVariant as second value.
    // This would be required to use BETWEEN in a prepared statement,
    // or to do a BETWEEN scan for three columns (a BETWEEN b and c).
    const auto value2_all_parameter_variant = HSQLExprTranslator::to_all_parameter_variant(*expr1);
    Assert(is_variant(value2_all_parameter_variant), "Value2 of a Predicate has to be AllTypeVariant");
    value2 = boost::get<AllTypeVariant>(value2_all_parameter_variant);

    Assert(refers_to_column(*column_ref_hsql_expr), "For BETWEENS, hsql_expr.expr has to refer to a column");
  } else if (predicate_condition == PredicateCondition::In) {
    // Handle IN by using a semi join
    Assert(hsql_expr.select, "The IN operand only supports subqueries so far");
    // TODO(anybody): Also support lists of literals
    auto subselect_node = translate_select(*hsql_expr.select);

    const auto left_column = resolve_column(*column_ref_hsql_expr);
    Assert(subselect_node->output_column_references().size() == 1, "You can only check IN on one column");
    auto right_column = subselect_node->output_column_references()[0];
    const auto column_references = std::make_pair(left_column, right_column);

    auto join_node = std::make_shared<JoinNode>(JoinMode::Semi, column_references, PredicateCondition::Equals);
    join_node->set_left_input(input_node);
    join_node->set_right_input(subselect_node);

    return join_node;
  } else if (predicate_condition != PredicateCondition::IsNull &&
             predicate_condition != PredicateCondition::IsNotNull) {
    /**
     * For logical operators (>, >=, <, ...), thanks to the strict interface of PredicateNode/TableScan, we have to
     * determine whether the left (expr.expr) or the right (expr.expr2) expr refers to the Column/AggregateFunction
     * or the other one.
     */
    DebugAssert(hsql_expr.expr2 != nullptr, "hsql malformed");

    if (!refers_to_column(*hsql_expr.expr)) {
      Assert(refers_to_column(*hsql_expr.expr2), "One side of the expression has to refer to a column.");
      operands_switched = true;
      predicate_condition = get_predicate_condition_for_reverse_order(predicate_condition);
    }

    value_ref_hsql_expr = operands_switched ? hsql_expr.expr : hsql_expr.expr2;
    column_ref_hsql_expr = operands_switched ? hsql_expr.expr2 : hsql_expr.expr;
  }

  /**
   * the argument passed to resolve_column() here:
   * the expr referring to the main column to be scanned, e.g. "p_income" in `WHERE 5 > p_income`
   * or "p_a" in `WHERE p_a > p_b`
   */
  const auto column_id = resolve_column(*column_ref_hsql_expr);

  auto current_node = input_node;
  auto has_nested_expression = false;

  AllParameterVariant value;
  if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    value = NULL_VALUE;
  } else if (refers_to_column(*value_ref_hsql_expr)) {
    value = resolve_column(*value_ref_hsql_expr);
  } else if (value_ref_hsql_expr->select) {
    // If a subselect is present, add the result of the subselect to the table using a projection
    const auto column_references = input_node->output_column_references();
    const auto original_column_expressions = LQPExpression::create_columns(column_references);

    auto subselect_node = translate_select(*value_ref_hsql_expr->select);
    auto subselect_expression = LQPExpression::create_subselect(subselect_node);

    auto column_expressions = original_column_expressions;
    column_expressions.push_back(subselect_expression);

    auto expand_projection_node = std::make_shared<ProjectionNode>(column_expressions);
    expand_projection_node->set_left_input(input_node);

    // Compare against the column containing the subselect result
    auto subselect_column_id = ColumnID(column_expressions.size() - 1);
    auto predicate_node =
        std::make_shared<PredicateNode>(column_id, predicate_condition, subselect_column_id, std::nullopt);
    predicate_node->set_left_input(expand_projection_node);

    // Remove the column containing the subselect result
    auto reduce_projection_node = std::make_shared<ProjectionNode>(original_column_expressions);
    reduce_projection_node->set_left_input(predicate_node);

    return reduce_projection_node;
  } else if (value_ref_hsql_expr->type == hsql::kExprOperator) {
    /**
     * If there is a nested expression (e.g. 1233 + 1) instead of a column reference or literal,
     * we need to add a Projection node that handles this before adding the PredicateNode.
     */
    auto column_expressions = LQPExpression::create_columns(current_node->output_column_references());
    column_expressions.push_back(HSQLExprTranslator::to_lqp_expression(*value_ref_hsql_expr, current_node));

    auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
    projection_node->set_left_input(current_node);
    current_node = projection_node;
    has_nested_expression = true;

    DebugAssert(column_expressions.size() <= std::numeric_limits<uint16_t>::max(),
                "Number of column expressions cannot exceed maximum value of ColumnID.");
    value = LQPColumnReference(current_node, ColumnID{static_cast<uint16_t>(column_expressions.size() - 1)});
  } else {
    value = HSQLExprTranslator::to_all_parameter_variant(*value_ref_hsql_expr);
  }

  auto predicate_node = PredicateNode::make(column_id, predicate_condition, value, value2);
  predicate_node->set_left_input(current_node);

  current_node = predicate_node;

  /**
   * The ProjectionNode we added previously (if we have a nested expression)
   * added a column expression for that expression, which we need to remove here.
   */
  if (has_nested_expression) {
    auto column_expressions = LQPExpression::create_columns(current_node->output_column_references());
    column_expressions.pop_back();

    auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
    projection_node->set_left_input(current_node);
    current_node = projection_node;
  }

  return current_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_show(const hsql::ShowStatement& show_statement) {
  switch (show_statement.type) {
    case hsql::ShowType::kShowTables:
      return ShowTablesNode::make();
    case hsql::ShowType::kShowColumns:
      return ShowColumnsNode::make(std::string(show_statement.name));
    default:
      Fail("hsql::ShowType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create(const hsql::CreateStatement& create_statement) {
  switch (create_statement.type) {
    case hsql::CreateType::kCreateView: {
      auto view = translate_select((const hsql::SelectStatement &) *create_statement.select);

      if (create_statement.viewColumns) {
        // The CREATE VIEW statement has renamed the columns: CREATE VIEW myview (foo, bar) AS SELECT ...
        Assert(create_statement.viewColumns->size() == view->output_column_count(),
               "Number of Columns in CREATE VIEW does not match SELECT statement");

        // Create a list of renamed column expressions
        std::vector<std::shared_ptr<LQPExpression>> projections;
        ColumnID column_id{0};
        for (const auto& alias : *create_statement.viewColumns) {
          const auto column_reference = view->output_column_references()[column_id];
          // rename columns so they match the view definition
          projections.push_back(LQPExpression::create_column(column_reference, alias));
          ++column_id;
        }

        // Create a projection node for this renaming
        auto projection_node = ProjectionNode::make(projections);
        projection_node->set_left_input(view);
        view = projection_node;
      }

      return std::make_shared<CreateViewNode>(create_statement.tableName, view);
    }
    default:
      Fail("hsql::CreateType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_drop(const hsql::DropStatement& drop_statement) {
  switch (drop_statement.type) {
    case hsql::DropType::kDropView: {
      return DropViewNode::make(drop_statement.name);
    }
    default:
      Fail("hsql::DropType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_validate_if_active(
    const std::shared_ptr<AbstractLQPNode>& input_node) {
  if (!_validate) return input_node;

  auto validate_node = ValidateNode::make();
  validate_node->set_left_input(input_node);
  return validate_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_predicate_expression(
const std::shared_ptr<AbstractExpression> &expression,
std::shared_ptr<AbstractLQPNode> current_node) const {
  if (expression->type == ExpressionType::Predicate) {
    for (const auto& argument : expression->arguments) {
      current_node = _translate_predicate_expression(argument, current_node);
    }

    const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(expression);

    return PredicateNode::make(predicate_expression, current_node);
  }

  if (expression->type == ExpressionType::Logical) {
    const auto logical_expression = std::static_pointer_cast<LogicalExpression>(expression);

    switch (logical_expression->logical_operator) {
      case LogicalOperator::And: {
        current_node = _translate_predicate_expression(logical_expression->left_operand(), current_node);
        return _translate_predicate_expression(logical_expression->right_operand(), current_node);
      }
      case LogicalOperator::Or: {
        const auto input_expressions = current_node->output_column_expressions();

        const auto left_input = _translate_predicate_expression(logical_expression->left_operand(), current_node);
        const auto right_input = _translate_predicate_expression(logical_expression->left_operand(), current_node);

        // For Union to work we need to eliminate all potential temporary columns added in the branches of the Union
        // E.g. "a+b" in "a+b > 5 OR a < 3"
        return UnionNode::make(UnionMode::Positions,
          _prune_expressions(left_input, input_expressions),
                               _prune_expressions(right_input, input_expressions));
      }
    }
  }

  if (expression->type == ExpressionType::Not) {
    // Fallback implementation for NOT, for now
    current_node = _add_expression(current_node, expression);
    return PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
    PredicateCondition::NotEquals, expression, std::make_shared<ValueExpression>(0)));
  }

  // The required expression is already available or doesn't need to be computed (e.g. when it is a literal)
  if (!expression->requires_calculation() || current_node->find_column(expression)) return current_node;

  // The required expression needs to be computed
  return _add_expression(current_node, expression);
}


std::shared_ptr<AbstractLQPNode> SQLTranslator::_prune_expressions(const std::shared_ptr<AbstractLQPNode>& node,
                                                            const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  if (deep_equals_expressions(node->output_column_expressions(), expressions)) return node;
  return ProjectionNode::make(expressions, node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_add_expression(const std::shared_ptr<AbstractLQPNode>& node,
                                                 const std::shared_ptr<AbstractExpression>& expression) const {
  Assert(expression->type != ExpressionType::Aggregate, "Aggregate used in illegal location");

  auto expressions = node->output_column_expressions();
  expressions.emplace_back(expression);
  return ProjectionNode::make(expressions, node);
}

}  // namespace opossum
