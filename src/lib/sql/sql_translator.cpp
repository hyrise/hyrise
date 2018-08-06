#include "sql_translator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "create_sql_parser_error_message.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
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
#include "storage/lqp_view.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "SQLParser.h"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

const std::unordered_map<hsql::OperatorType, ArithmeticOperator> hsql_arithmetic_operators = {
    {hsql::kOpPlus, ArithmeticOperator::Addition},           {hsql::kOpMinus, ArithmeticOperator::Subtraction},
    {hsql::kOpAsterisk, ArithmeticOperator::Multiplication}, {hsql::kOpSlash, ArithmeticOperator::Division},
    {hsql::kOpPercentage, ArithmeticOperator::Modulo},
};

const std::unordered_map<hsql::OperatorType, LogicalOperator> hsql_logical_operators = {
    {hsql::kOpAnd, LogicalOperator::And}, {hsql::kOpOr, LogicalOperator::Or}};

const std::unordered_map<hsql::OperatorType, PredicateCondition> hsql_predicate_condition = {
    {hsql::kOpBetween, PredicateCondition::Between},
    {hsql::kOpEquals, PredicateCondition::Equals},
    {hsql::kOpNotEquals, PredicateCondition::NotEquals},
    {hsql::kOpLess, PredicateCondition::LessThan},
    {hsql::kOpLessEq, PredicateCondition::LessThanEquals},
    {hsql::kOpGreater, PredicateCondition::GreaterThan},
    {hsql::kOpGreaterEq, PredicateCondition::GreaterThanEquals},
    {hsql::kOpLike, PredicateCondition::Like},
    {hsql::kOpNotLike, PredicateCondition::NotLike},
    {hsql::kOpIsNull, PredicateCondition::IsNull}};

const std::unordered_map<hsql::DatetimeField, DatetimeComponent> hsql_datetime_field = {
    {hsql::kDatetimeYear, DatetimeComponent::Year},     {hsql::kDatetimeMonth, DatetimeComponent::Month},
    {hsql::kDatetimeDay, DatetimeComponent::Day},       {hsql::kDatetimeHour, DatetimeComponent::Hour},
    {hsql::kDatetimeMinute, DatetimeComponent::Minute}, {hsql::kDatetimeSecond, DatetimeComponent::Second},
};

JoinMode translate_join_mode(const hsql::JoinType join_type) {
  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinFull, JoinMode::Outer},  {hsql::kJoinLeft, JoinMode::Left},
      {hsql::kJoinRight, JoinMode::Right}, {hsql::kJoinCross, JoinMode::Cross},
  };

  auto it = join_type_to_mode.find(join_type);
  Assert(it != join_type_to_mode.end(), "Unknown join type.");
  return it->second;
}

/**
 * Is the expression a predicate that our Join Operators can process directly?
 * That is, is it of the form <column> <predicate_condition> <column>?
 */
bool is_trivial_join_predicate(const AbstractExpression& expression, const AbstractLQPNode& left_input,
                               const AbstractLQPNode& right_input) {
  if (expression.type != ExpressionType::Predicate) return false;

  const auto* binary_predicate_expression = dynamic_cast<const BinaryPredicateExpression*>(&expression);
  if (!binary_predicate_expression) return false;

  const auto left_in_left = left_input.find_column_id(*binary_predicate_expression->left_operand());
  const auto right_in_right = right_input.find_column_id(*binary_predicate_expression->right_operand());
  const auto right_in_left = left_input.find_column_id(*binary_predicate_expression->right_operand());
  const auto left_in_right = right_input.find_column_id(*binary_predicate_expression->left_operand());

  return (left_in_left && right_in_right) || (right_in_left && left_in_right);
}
}  // namespace

namespace opossum {

SQLTranslator::SQLTranslator(const UseMvcc use_mvcc,
                             const std::shared_ptr<SQLIdentifierResolverProxy>& external_sql_identifier_resolver_proxy,
                             const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator)
    : _use_mvcc(use_mvcc),
      _external_sql_identifier_resolver_proxy(external_sql_identifier_resolver_proxy),
      _parameter_id_allocator(parameter_id_allocator) {}

const std::unordered_map<ValuePlaceholderID, ParameterID>& SQLTranslator::value_placeholders() const {
  return _parameter_id_allocator->value_placeholders();
}

std::vector<std::shared_ptr<AbstractLQPNode>> SQLTranslator::translate_parser_result(
    const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractLQPNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = _translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_statement(const hsql::SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return _translate_select_statement(static_cast<const hsql::SelectStatement&>(statement));
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
      FailInput("SQL statement type not supported");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_select_statement(const hsql::SelectStatement& select) {
  // SQL Orders of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause (incl. JOINs and subselects that are part of this)
  // 2. WHERE clause
  // 3. GROUP BY clause
  // 4. HAVING clause
  // 5. SELECT clause
  // 6. UNION clause
  // 7. ORDER BY clause
  // 8. LIMIT clause

  AssertInput(select.selectList != nullptr, "SELECT list needs to exist");
  AssertInput(!select.selectList->empty(), "SELECT list needs to have entries");
  AssertInput(select.unionSelect == nullptr, "Set operations (UNION/INTERSECT/...) are not supported yet");
  AssertInput(!select.selectDistinct, "DISTINCT is not yet supported");

  // Translate FROM
  if (select.fromTable) {
    _from_clause_result = _translate_table_ref(*select.fromTable);
    _current_lqp = _from_clause_result->lqp;
    _sql_identifier_resolver = _from_clause_result->sql_identifier_resolver;
  } else {
    _current_lqp = std::make_shared<DummyTableNode>();
    _sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();
  }

  // Translate WHERE
  if (select.whereClause != nullptr) {
    const auto where_expression = _translate_hsql_expr(*select.whereClause, _sql_identifier_resolver);
    _current_lqp = _translate_predicate_expression(where_expression, _current_lqp);
  }

  // Translate SELECT, HAVING, GROUP BY in one go, as they are interdependent
  _translate_select_list_groupby_having(select);

  // Translate ORDER BY and LIMIT
  if (select.order) _translate_order_by(*select.order);
  if (select.limit) _translate_limit(*select.limit);

  /**
   * Name, select and arrange the Columns as specified in the SELECT clause
   */
  // Only add a ProjectionNode if necessary
  if (!expressions_equal(_current_lqp->column_expressions(), _inflated_select_list_expressions)) {
    _current_lqp = ProjectionNode::make(_inflated_select_list_expressions, _current_lqp);
  }

  // Check whether we need to create an AliasNode - this is the case whenever an Expression was assigned a column_name
  // that is not its generated name.
  const auto need_alias_node = std::any_of(
      _inflated_select_list_expressions.begin(), _inflated_select_list_expressions.end(), [&](const auto& expression) {
        const auto identifier = _sql_identifier_resolver->get_expression_identifier(expression);
        return identifier && identifier->column_name != expression->as_column_name();
      });

  if (need_alias_node) {
    std::vector<std::string> aliases;
    for (const auto& output_column_expression : _inflated_select_list_expressions) {
      const auto identifier = _sql_identifier_resolver->get_expression_identifier(output_column_expression);
      if (identifier) {
        aliases.emplace_back(identifier->column_name);
      } else {
        aliases.emplace_back(output_column_expression->as_column_name());
      }
    }

    _current_lqp = AliasNode::make(_inflated_select_list_expressions, aliases, _current_lqp);
  }

  return _current_lqp;
}

std::shared_ptr<AbstractExpression> SQLTranslator::translate_hsql_expr(const hsql::Expr& hsql_expr) {
  // Create an empty SQLIdentifier context - thus the expression cannot refer to any external columns
  return SQLTranslator{}._translate_hsql_expr(hsql_expr, std::make_shared<SQLIdentifierResolver>());
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_insert(const hsql::InsertStatement& insert) {
  const auto table_name = std::string{insert.tableName};
  const auto target_table = StorageManager::get().get_table(table_name);
  auto insert_data_node = std::shared_ptr<AbstractLQPNode>{};
  auto column_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto insert_data_projection_required = false;

  /**
   * 1. Create the expressions/LQP producing the data to insert.
   *        - For `INSERT INTO <table> SELECT ...` this means evaluating the select statement
   *        - For `INSERT INTO <table> VALUES ...` this means creating a one row table with the VALUES
   */
  if (insert.type == hsql::kInsertSelect) {
    // `INSERT INTO newtable SELECT ... FROM oldtable WHERE condition`
    AssertInput(insert.select, "INSERT INTO ... SELECT ...: No SELECT statement given");
    insert_data_node = _translate_select_statement(*insert.select);
    column_expressions = insert_data_node->column_expressions();

  } else {
    // `INSERT INTO table_name [(column1, column2, column3, ...)] VALUES (value1, value2, value3, ...);`
    AssertInput(insert.values, "INSERT INTO ... VALUES: No values given");

    column_expressions.reserve(insert.values->size());
    for (const auto* value : *insert.values) {
      column_expressions.emplace_back(_translate_hsql_expr(*value, _sql_identifier_resolver));
    }

    insert_data_node = DummyTableNode::make();
    insert_data_projection_required = true;
  }

  /**
   * 2. Rearrange the columns of the data to insert to match the target table
   *    E.g., `SELECT INTO table (c, a) VALUES (1, 2)` becomes `SELECT INTO table (a, b, c) VALUES (2, NULL, 1)
   */
  if (insert.columns) {
    // `INSERT INTO table_name (column1, column2, column3, ...) ...;`
    // Create a Projection that matches the specified columns with the columns of `table_name`

    AssertInput(insert.columns->size() == column_expressions.size(),
                "INSERT: Target column count and number of input columns mismatch");

    auto expressions = std::vector<std::shared_ptr<AbstractExpression>>(target_table->column_count(), null_());
    auto source_column_id = ColumnID{0};
    for (const auto& column_name : *insert.columns) {
      // retrieve correct ColumnID from the target table
      const auto target_column_id = target_table->column_id_by_name(column_name);
      expressions[target_column_id] = column_expressions[source_column_id];
      ++source_column_id;
    }
    column_expressions = expressions;

    insert_data_projection_required = true;
  }

  /**
   * 3. When inserting NULL literals (or not inserting into all columns), wrap NULLs in
   *    `CAST(NULL AS <column_data_type>), since a temporary table with the data to insert will be created and NULL is
   *    an invalid column data type in Hyrise.
   */
  for (auto column_id = ColumnID{0}; column_id < target_table->column_count(); ++column_id) {
    // Turn `expression` into `CAST(expression AS <column_data_type>)`, if expression is a NULL literal
    auto expression = column_expressions[column_id];
    if (const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression); value_expression) {
      if (variant_is_null(value_expression->value)) {
        column_expressions[column_id] = cast_(null_(), target_table->column_data_type(column_id));
        insert_data_projection_required = true;
      }
    }
  }

  /**
   * 4. Perform type conversions if necessary so the types of the inserted data exactly matches the table column types
   */
  for (auto column_id = ColumnID{0}; column_id < target_table->column_count(); ++column_id) {
    // Always cast if the expression contains a placeholder, since we can't know the actual data type of the expression
    // until it is replaced.
    if (expression_contains_placeholders(column_expressions[column_id]) ||
        target_table->column_data_type(column_id) != column_expressions[column_id]->data_type()) {
      column_expressions[column_id] = cast_(column_expressions[column_id], target_table->column_data_type(column_id));
    }
  }

  /**
   * 5. Project the data to insert ONLY if required, i.e. when column order needed to be arranged or NULLs were wrapped
   *    in `CAST(NULL as <data_type>)`
   */
  if (insert_data_projection_required) {
    insert_data_node = ProjectionNode::make(column_expressions, insert_data_node);
  }

  AssertInput(insert_data_node->column_expressions().size() == target_table->column_count(),
              "INSERT: Column count mismatch");

  /**
   * NOTE: DataType checking has to be done at runtime, as Query could still contain Placeholder with unspecified type
   */

  return InsertNode::make(table_name, insert_data_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_delete(const hsql::DeleteStatement& delete_statement) {
  const auto sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();
  auto data_to_delete_node = _translate_stored_table(delete_statement.tableName, sql_identifier_resolver);

  if (delete_statement.expr) {
    const auto delete_where_expression = _translate_hsql_expr(*delete_statement.expr, sql_identifier_resolver);
    data_to_delete_node = _translate_predicate_expression(delete_where_expression, data_to_delete_node);
  }

  return DeleteNode::make(delete_statement.tableName, data_to_delete_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_update(const hsql::UpdateStatement& update) {
  auto translation_state = _translate_table_ref(*update.table);

  // The LQP that selects the fields to update
  auto selection_lqp = translation_state.lqp;

  // Take a copy intentionally, we're going to replace some of these later
  auto update_expressions = selection_lqp->column_expressions();

  if (update.where) {
    const auto where_expression = _translate_hsql_expr(*update.where, translation_state.sql_identifier_resolver);
    selection_lqp = _translate_predicate_expression(where_expression, selection_lqp);
  }

  // The update operator wants ReferenceColumns on its left side
  // TODO(anyone): fix this
  AssertInput(!std::dynamic_pointer_cast<StoredTableNode>(selection_lqp),
              "Unconditional updates are currently not supported");

  for (const auto* update_clause : *update.updates) {
    const auto column_name = std::string{update_clause->column};
    const auto column_expression = translation_state.sql_identifier_resolver->resolve_identifier_relaxed(column_name);
    const auto column_id = selection_lqp->get_column_id(*column_expression);

    update_expressions[column_id] =
        _translate_hsql_expr(*update_clause->value, translation_state.sql_identifier_resolver);
  }

  return UpdateNode::make((update.table)->name, update_expressions, selection_lqp);
}

SQLTranslator::TableSourceState SQLTranslator::_translate_table_ref(const hsql::TableRef& hsql_table_ref) {
  switch (hsql_table_ref.type) {
    case hsql::kTableName:
    case hsql::kTableSelect:
      return _translate_table_origin(hsql_table_ref);

    case hsql::kTableJoin:
      if (hsql_table_ref.join->type == hsql::kJoinNatural) {
        return _translate_natural_join(*hsql_table_ref.join);
      } else {
        return _translate_predicated_join(*hsql_table_ref.join);
      }

    case hsql::kTableCrossProduct:
      return _translate_cross_product(*hsql_table_ref.list);

    default:
      Fail("Unexpected SQLParser TableRef in FROM");
  }
}

SQLTranslator::TableSourceState SQLTranslator::_translate_table_origin(const hsql::TableRef& hsql_table_ref) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // Each element in the FROM list needs to have a unique table name (i.e. Subselects are required to have an ALIAS)
  auto table_name = std::string{};
  auto sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();

  switch (hsql_table_ref.type) {
    case hsql::kTableName: {
      if (StorageManager::get().has_table(hsql_table_ref.name)) {
        lqp = _translate_stored_table(hsql_table_ref.name, sql_identifier_resolver);

      } else if (StorageManager::get().has_view(hsql_table_ref.name)) {
        const auto view = StorageManager::get().get_view(hsql_table_ref.name);
        lqp = view->lqp;

        /**
         * Add all named columns from the view to the IdentifierContext
         */
        for (auto column_id = ColumnID{0}; column_id < view->lqp->column_expressions().size(); ++column_id) {
          const auto column_expression = view->lqp->column_expressions()[column_id];

          const auto column_name_iter = view->column_names.find(column_id);
          if (column_name_iter != view->column_names.end()) {
            sql_identifier_resolver->set_column_name(column_expression, column_name_iter->second);
          }
          sql_identifier_resolver->set_table_name(column_expression, hsql_table_ref.name);
        }

        Assert(_use_mvcc == UseMvcc::No || lqp_is_validated(view->lqp),
               "Can't use unvalidated View in validated Query");
      } else {
        FailInput(std::string("Did not find a table or view with name ") + hsql_table_ref.name);
      }
      table_name = hsql_table_ref.alias ? hsql_table_ref.alias->name : hsql_table_ref.name;
    } break;

    case hsql::kTableSelect: {
      AssertInput(hsql_table_ref.alias && hsql_table_ref.alias->name, "Every SubSelect must have its own alias");
      table_name = hsql_table_ref.alias->name;

      SQLTranslator sub_select_translator{_use_mvcc};
      lqp = sub_select_translator._translate_select_statement(*hsql_table_ref.select);

      for (const auto& sub_select_expression : lqp->column_expressions()) {
        const auto identifier =
            sub_select_translator._sql_identifier_resolver->get_expression_identifier(sub_select_expression);

        // Make sure each column from the SubSelect has a name
        if (identifier) {
          sql_identifier_resolver->set_column_name(sub_select_expression, identifier->column_name);
        } else {
          sql_identifier_resolver->set_column_name(sub_select_expression, sub_select_expression->as_column_name());
        }
      }

      table_name = hsql_table_ref.alias->name;
    } break;

    default:
      Fail("_translate_table_origin() is only for Tables, Views and Sub Selects.");
  }

  // Rename columns as in "SELECT * FROM t AS x (y,z)"
  if (hsql_table_ref.alias && hsql_table_ref.alias->columns) {
    const auto& column_expressions = lqp->column_expressions();

    AssertInput(hsql_table_ref.alias->columns->size() == column_expressions.size(),
                "Must specify a name for exactly each column");

    for (auto column_id = ColumnID{0}; column_id < hsql_table_ref.alias->columns->size(); ++column_id) {
      sql_identifier_resolver->set_column_name(column_expressions[column_id],
                                               (*hsql_table_ref.alias->columns)[column_id]);
    }
  }

  for (const auto& expression : lqp->column_expressions()) {
    sql_identifier_resolver->set_table_name(expression, table_name);
  }

  return {lqp,
          {{
              {table_name, lqp->column_expressions()},
          }},
          {lqp->column_expressions()},
          sql_identifier_resolver};
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_stored_table(
    const std::string& name, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) {
  const auto stored_table_node = StoredTableNode::make(name);
  const auto validated_stored_table_node = _validate_if_active(stored_table_node);

  const auto table = StorageManager::get().get_table(name);

  // Publish the columns of the table in the SQLIdentifierResolver
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    const auto& column_definition = table->column_definitions()[column_id];
    const auto column_reference = LQPColumnReference{stored_table_node, column_id};
    const auto column_expression = std::make_shared<LQPColumnExpression>(column_reference);
    sql_identifier_resolver->set_column_name(column_expression, column_definition.name);
    sql_identifier_resolver->set_table_name(column_expression, name);
  }

  return validated_stored_table_node;
}

SQLTranslator::TableSourceState SQLTranslator::_translate_predicated_join(const hsql::JoinDefinition& join) {
  const auto join_mode = translate_join_mode(join.type);

  auto left_state = _translate_table_ref(*join.left);
  auto right_state = _translate_table_ref(*join.right);

  auto left_input_lqp = left_state.lqp;
  auto right_input_lqp = right_state.lqp;

  // left_state becomes the result state
  auto result_state = std::move(left_state);
  result_state.append(std::move(right_state));

  /**
   * Hyrise doesn't have support for complex join predicates in OUTER JOINs
   * The current implementation expects a single join condition in a set of conjunctive
   * clauses. The remaining clauses are expected to be relevant for only one of
   * the join partners and are therefore converted into predicates inserted in between the
   * source relations and the actual join node.
   * See TPC-H 13 for an example query.
   */
  const auto raw_join_predicate = _translate_hsql_expr(*join.condition, result_state.sql_identifier_resolver);
  const auto raw_join_predicate_cnf = expression_flatten_conjunction(raw_join_predicate);

  auto left_local_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto right_local_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto join_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& predicate : raw_join_predicate_cnf) {
    if (expression_evaluable_on_lqp(predicate, *left_input_lqp)) {
      left_local_predicates.emplace_back(predicate);
    } else if (expression_evaluable_on_lqp(predicate, *right_input_lqp)) {
      right_local_predicates.emplace_back(predicate);
    } else {
      // Accept any kind of predicate here and let the LQPTranslator fail on those that it doesn't support
      join_predicates.emplace_back(predicate);
    }
  }

  AssertInput(join_mode == JoinMode::Inner || !join_predicates.empty(),
              "Multiple Predicates not supported in Outer Join");

  /**
   * Add local predicates - ignore local predicates on the preserving side of OUTER JOINs
   */
  if (join_mode != JoinMode::Left && join_mode != JoinMode::Outer) {
    for (const auto& left_local_predicate : left_local_predicates) {
      left_input_lqp = _translate_predicate_expression(left_local_predicate, left_input_lqp);
    }
  }
  if (join_mode != JoinMode::Right && join_mode != JoinMode::Outer) {
    for (const auto& right_local_predicate : right_local_predicates) {
      right_input_lqp = _translate_predicate_expression(right_local_predicate, right_input_lqp);
    }
  }

  /**
   * Add the join predicates
   */
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  const auto join_predicate_iter =
      std::find_if(join_predicates.begin(), join_predicates.end(), [&](const auto& join_predicate) {
        return is_trivial_join_predicate(*join_predicate, *left_input_lqp, *right_input_lqp);
      });

  if (join_predicate_iter == join_predicates.end()) {
    lqp = JoinNode::make(JoinMode::Cross, left_input_lqp, right_input_lqp);
  } else {
    lqp = JoinNode::make(join_mode, *join_predicate_iter, left_input_lqp, right_input_lqp);
    join_predicates.erase(join_predicate_iter);
  }

  // Add secondary join predicates as normal PredicateNodes
  for (const auto& join_predicate : join_predicates) {
    PerformanceWarning("Secondary Join Predicates added as normal Predicates");
    lqp = _translate_predicate_expression(join_predicate, lqp);
  }

  result_state.lqp = lqp;
  return result_state;
}

SQLTranslator::TableSourceState SQLTranslator::_translate_natural_join(const hsql::JoinDefinition& join) {
  Assert(join.type == hsql::kJoinNatural, "join must be a natural join");

  auto left_state = _translate_table_ref(*join.left);
  auto right_state = _translate_table_ref(*join.right);

  const auto left_sql_identifier_resolver = left_state.sql_identifier_resolver;
  const auto right_sql_identifier_resolver = right_state.sql_identifier_resolver;
  const auto left_input_lqp = left_state.lqp;
  const auto right_input_lqp = right_state.lqp;

  auto join_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto result_state = std::move(left_state);

  // a) Find matching columns and create JoinPredicates from them
  // b) Add columns from right input to the output when they have no match in the left input
  for (const auto& right_expression : right_state.elements_in_order) {
    const auto right_identifier = right_sql_identifier_resolver->get_expression_identifier(right_expression);

    if (right_identifier) {
      const auto left_expression =
          left_sql_identifier_resolver->resolve_identifier_relaxed({right_identifier->column_name});

      if (left_expression) {
        // Two columns match, let's join on them.
        join_predicates.emplace_back(
            std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, left_expression, right_expression));
        continue;
      }
    }

    // No matching column in the left input found, add the column from the right input to the output
    result_state.elements_in_order.emplace_back(right_expression);
    if (right_identifier) {
      result_state.sql_identifier_resolver->set_column_name(right_expression, right_identifier->column_name);
      if (right_identifier->table_name) {
        result_state.elements_by_table_name[*right_identifier->table_name].emplace_back(right_expression);
        result_state.sql_identifier_resolver->set_table_name(right_expression, *right_identifier->table_name);
      }
    }
  }

  auto lqp = std::shared_ptr<AbstractLQPNode>();

  if (join_predicates.empty()) {
    // No matching columns? Then the NATURAL JOIN becomes a Cross Join
    lqp = JoinNode::make(JoinMode::Cross, left_input_lqp, right_input_lqp);
  } else {
    // Turn one of the Join Predicates into an actual join
    lqp = JoinNode::make(JoinMode::Inner, join_predicates.front(), left_input_lqp, right_input_lqp);
  }

  // Add remaining join predicates as normal predicates
  for (auto join_predicate_idx = size_t{1}; join_predicate_idx < join_predicates.size(); ++join_predicate_idx) {
    lqp = PredicateNode::make(join_predicates[join_predicate_idx], lqp);
  }

  if (!join_predicates.empty()) {
    // Projection Node to remove duplicate columns
    lqp = ProjectionNode::make(result_state.elements_in_order, lqp);
  }

  // Create output TableSourceState
  result_state.lqp = lqp;

  return result_state;
}

SQLTranslator::TableSourceState SQLTranslator::_translate_cross_product(const std::vector<hsql::TableRef*>& tables) {
  Assert(!tables.empty(), "Cannot translate cross product without tables");

  auto result_table_source_state = _translate_table_ref(*tables.front());

  for (auto table_idx = size_t{1}; table_idx < tables.size(); ++table_idx) {
    auto table_source_state = _translate_table_ref(*tables[table_idx]);
    result_table_source_state.lqp =
        JoinNode::make(JoinMode::Cross, result_table_source_state.lqp, table_source_state.lqp);
    result_table_source_state.append(std::move(table_source_state));
  }

  return result_table_source_state;
}

void SQLTranslator::_translate_select_list_groupby_having(const hsql::SelectStatement& select) {
  const auto& input_expressions = _current_lqp->column_expressions();

  auto pre_aggregate_expression_set = ExpressionUnorderedSet{input_expressions.begin(), input_expressions.end()};
  auto pre_aggregate_expressions =
      std::vector<std::shared_ptr<AbstractExpression>>{input_expressions.begin(), input_expressions.end()};
  auto aggregate_expression_set = ExpressionUnorderedSet{};
  auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  // Visitor that identifies aggregates and their arguments
  const auto find_aggregates_and_arguments = [&](auto& sub_expression) {
    if (sub_expression->type != ExpressionType::Aggregate) return ExpressionVisitation::VisitArguments;

    /**
     * If the AggregateExpression has already been computed in a previous node (consider "x" in
     * "SELECT x FROM (SELECT MIN(a) as x FROM t) AS y)", it doesn't count as a new Aggregate and is therefore not
     * considered a "Aggregate" in the current SELECT list. Handling this as a special case seems hacky to me as well,
     * but it's the best solution I can come up with right now.
     */
    if (_current_lqp->find_column_id(*sub_expression)) return ExpressionVisitation::DoNotVisitArguments;

    auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(sub_expression);
    if (aggregate_expression_set.emplace(aggregate_expression).second) {
      aggregate_expressions.emplace_back(aggregate_expression);
      for (const auto& argument : aggregate_expression->arguments) {
        if (argument->requires_computation()) {
          if (pre_aggregate_expression_set.emplace(argument).second) {
            pre_aggregate_expressions.emplace_back(argument);
          }
        }
      }
    }

    return ExpressionVisitation::DoNotVisitArguments;
  };

  // Identify all Aggregates and their arguments needed for SELECT and build the select_list_elements
  // Each select_list_element is either an Expression or nullptr if the element is a Wildcard
  std::vector<std::shared_ptr<AbstractExpression>> select_list_elements;
  auto post_select_sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>(*_sql_identifier_resolver);
  for (const auto& hsql_select_expr : *select.selectList) {
    if (hsql_select_expr->type == hsql::kExprStar) {
      select_list_elements.emplace_back(nullptr);
    } else {
      auto expression = _translate_hsql_expr(*hsql_select_expr, _sql_identifier_resolver);
      visit_expression(expression, find_aggregates_and_arguments);
      select_list_elements.emplace_back(expression);

      if (hsql_select_expr->alias) {
        post_select_sql_identifier_resolver->set_column_name(expression, hsql_select_expr->alias);
      }
    }
  }
  _sql_identifier_resolver = post_select_sql_identifier_resolver;

  // Identify all GROUP BY expressions
  auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  if (select.groupBy && select.groupBy->columns) {
    group_by_expressions.reserve(select.groupBy->columns->size());
    for (const auto* group_by_hsql_expr : *select.groupBy->columns) {
      const auto group_by_expression = _translate_hsql_expr(*group_by_hsql_expr, _sql_identifier_resolver);
      group_by_expressions.emplace_back(group_by_expression);
      if (pre_aggregate_expression_set.emplace(group_by_expression).second) {
        pre_aggregate_expressions.emplace_back(group_by_expression);
      }
    }
  }

  // Gather all aggregates and arguments from HAVING
  auto having_expression = std::shared_ptr<AbstractExpression>{};
  if (select.groupBy && select.groupBy->having) {
    having_expression = _translate_hsql_expr(*select.groupBy->having, _sql_identifier_resolver);
    visit_expression(having_expression, find_aggregates_and_arguments);
  }

  // Build pre_aggregate_projection, i.e. evaluate all Expression required for GROUP BY/Aggregates
  if (pre_aggregate_expressions.size() != _current_lqp->column_expressions().size()) {
    _current_lqp = ProjectionNode::make(pre_aggregate_expressions, _current_lqp);
  }

  // Build Aggregate
  const auto is_aggregate = !aggregate_expressions.empty() || !group_by_expressions.empty();
  if (is_aggregate) {
    _current_lqp = AggregateNode::make(group_by_expressions, aggregate_expressions, _current_lqp);
  }

  // Build Having
  if (having_expression) {
    _current_lqp = _translate_predicate_expression(having_expression, _current_lqp);
  }

  // Create output_expressions from SELECT list, including column wildcards
  std::unordered_map<std::shared_ptr<AbstractExpression>, std::string> column_aliases;

  for (auto select_list_idx = size_t{0}; select_list_idx < select.selectList->size(); ++select_list_idx) {
    const auto* hsql_expr = (*select.selectList)[select_list_idx];

    if (hsql_expr->type == hsql::kExprStar) {
      AssertInput(_from_clause_result, "Can't SELECT with wildcards since there are no FROM tables specified");

      if (hsql_expr->table) {
        if (is_aggregate) {
          // Select all GROUP BY columns with the specified table name
          for (const auto& group_by_expression : group_by_expressions) {
            const auto identifier = _sql_identifier_resolver->get_expression_identifier(group_by_expression);
            if (identifier && identifier->table_name == hsql_expr->table) {
              _inflated_select_list_expressions.emplace_back(group_by_expression);
            }
          }
        } else {
          // Select all columns from the FROM element with the specified name
          const auto from_element_iter = _from_clause_result->elements_by_table_name.find(hsql_expr->table);
          AssertInput(from_element_iter != _from_clause_result->elements_by_table_name.end(),
                      std::string("No such element in FROM with table name '") + hsql_expr->table + "'");

          _inflated_select_list_expressions.insert(_inflated_select_list_expressions.end(),
                                                   from_element_iter->second.begin(), from_element_iter->second.end());
        }
      } else {
        if (is_aggregate) {
          // Select all GROUP BY columns
          _inflated_select_list_expressions.insert(_inflated_select_list_expressions.end(),
                                                   group_by_expressions.begin(), group_by_expressions.end());
        } else {
          // Select all columns from the FROM elements
          _inflated_select_list_expressions.insert(_inflated_select_list_expressions.end(),
                                                   _from_clause_result->elements_in_order.begin(),
                                                   _from_clause_result->elements_in_order.end());
        }
      }
    } else {
      auto output_expression = select_list_elements[select_list_idx];
      _inflated_select_list_expressions.emplace_back(output_expression);

      if (hsql_expr->alias) {
        _sql_identifier_resolver->set_column_name(output_expression, hsql_expr->alias);
      }
    }
  }
}

void SQLTranslator::_translate_order_by(const std::vector<hsql::OrderDescription*>& order_list) {
  if (order_list.empty()) return;

  // So we can later reset the available Expressions to the Expressions of this LQP
  const auto input_lqp = _current_lqp;

  std::vector<std::shared_ptr<AbstractExpression>> expressions(order_list.size());
  std::vector<OrderByMode> order_by_modes(order_list.size());
  for (auto expression_idx = size_t{0}; expression_idx < order_list.size(); ++expression_idx) {
    const auto& order_description = order_list[expression_idx];
    expressions[expression_idx] = _translate_hsql_expr(*order_description->expr, _sql_identifier_resolver);
    order_by_modes[expression_idx] = order_type_to_order_by_mode.at(order_description->type);
  }

  _current_lqp = _add_expressions_if_unavailable(_current_lqp, expressions);

  _current_lqp = SortNode::make(expressions, order_by_modes, _current_lqp);

  // If any Expressions were added to perform the sorting, remove them again
  if (input_lqp->column_expressions().size() != _current_lqp->column_expressions().size()) {
    _current_lqp = ProjectionNode::make(input_lqp->column_expressions(), _current_lqp);
  }
}

void SQLTranslator::_translate_limit(const hsql::LimitDescription& limit) {
  // TODO(anybody) SQLParser doesn't support Expressions in LIMIT clause yet
  const auto num_rows_expression = std::make_shared<ValueExpression>(limit.limit);
  _current_lqp = LimitNode::make(num_rows_expression, _current_lqp);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_show(const hsql::ShowStatement& show_statement) {
  switch (show_statement.type) {
    case hsql::ShowType::kShowTables:
      return ShowTablesNode::make();
    case hsql::ShowType::kShowColumns:
      return ShowColumnsNode::make(std::string(show_statement.name));
    default:
      FailInput("hsql::ShowType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create(const hsql::CreateStatement& create_statement) {
  switch (create_statement.type) {
    case hsql::CreateType::kCreateView: {
      auto lqp = _translate_select_statement(static_cast<const hsql::SelectStatement&>(*create_statement.select));

      std::unordered_map<ColumnID, std::string> column_names;

      if (create_statement.viewColumns) {
        // The CREATE VIEW statement has renamed the columns: CREATE VIEW myview (foo, bar) AS SELECT ...
        AssertInput(create_statement.viewColumns->size() == lqp->column_expressions().size(),
                    "Number of Columns in CREATE VIEW does not match SELECT statement");

        for (auto column_id = ColumnID{0}; column_id < create_statement.viewColumns->size(); ++column_id) {
          column_names.emplace(column_id, (*create_statement.viewColumns)[column_id]);
        }
      } else {
        for (auto column_id = ColumnID{0}; column_id < lqp->column_expressions().size(); ++column_id) {
          const auto identifier =
              _sql_identifier_resolver->get_expression_identifier(lqp->column_expressions()[column_id]);
          if (identifier) {
            column_names.emplace(column_id, identifier->column_name);
          }
        }
      }

      return CreateViewNode::make(create_statement.tableName, std::make_shared<LQPView>(lqp, column_names));
    }
    default:
      FailInput("hsql::CreateType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_drop(const hsql::DropStatement& drop_statement) {
  switch (drop_statement.type) {
    case hsql::DropType::kDropView:
      return DropViewNode::make(drop_statement.name);
    default:
      FailInput("hsql::DropType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_validate_if_active(
    const std::shared_ptr<AbstractLQPNode>& input_node) {
  if (_use_mvcc == UseMvcc::No) return input_node;

  return ValidateNode::make(input_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_predicate_expression(
    const std::shared_ptr<AbstractExpression>& expression, std::shared_ptr<AbstractLQPNode> current_node) const {
  /**
   * Translate AbstractPredicateExpression
   */
  switch (expression->type) {
    case ExpressionType::Predicate: {
      const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(expression);

      if (predicate_expression->predicate_condition == PredicateCondition::In) {
        current_node = _add_expressions_if_unavailable(current_node, {expression});
        return PredicateNode::make(not_equals_(expression, 0), current_node);
      } else {
        current_node = _add_expressions_if_unavailable(current_node, expression->arguments);
        return PredicateNode::make(expression, current_node);
      }
    }

    case ExpressionType::Logical: {
      const auto logical_expression = std::static_pointer_cast<LogicalExpression>(expression);

      switch (logical_expression->logical_operator) {
        case LogicalOperator::And: {
          current_node = _translate_predicate_expression(logical_expression->right_operand(), current_node);
          return _translate_predicate_expression(logical_expression->left_operand(), current_node);
        }
        case LogicalOperator::Or: {
          current_node = _add_expressions_if_unavailable(current_node, {expression});
          return PredicateNode::make(not_equals_(expression, 0), current_node);
        }
      }
    } break;

    case ExpressionType::Exists: {
      current_node = _add_expressions_if_unavailable(current_node, {expression});
      return PredicateNode::make(not_equals_(expression, 0), current_node);
    }

    default:
      FailInput("Cannot use this ExpressionType as predicate");
  }

  Fail("GCC thinks this is reachable");
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_prune_expressions(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  if (expressions_equal(node->column_expressions(), expressions)) return node;
  return ProjectionNode::make(expressions, node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_add_expressions_if_unavailable(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  std::vector<std::shared_ptr<AbstractExpression>> projection_expressions;

  for (const auto& expression : expressions) {
    // The required expression is already available or doesn't need to be computed (e.g. when it is a literal)
    if (!expression->requires_computation() || node->find_column_id(*expression)) continue;
    projection_expressions.emplace_back(expression);
  }

  // If all requested expressions are available, no need to create a projection
  if (projection_expressions.empty()) return node;

  projection_expressions.insert(projection_expressions.end(), node->column_expressions().begin(),
                                node->column_expressions().end());

  return ProjectionNode::make(projection_expressions, node);
}

std::shared_ptr<AbstractExpression> SQLTranslator::_translate_hsql_expr(
    const hsql::Expr& expr, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) const {
  auto name = expr.name != nullptr ? std::string(expr.name) : "";

  const auto left = expr.expr ? _translate_hsql_expr(*expr.expr, sql_identifier_resolver) : nullptr;
  const auto right = expr.expr2 ? _translate_hsql_expr(*expr.expr2, sql_identifier_resolver) : nullptr;

  switch (expr.type) {
    case hsql::kExprColumnRef: {
      const auto table_name = expr.table ? std::optional<std::string>(std::string(expr.table)) : std::nullopt;
      const auto identifier = SQLIdentifier{name, table_name};

      auto expression = sql_identifier_resolver->resolve_identifier_relaxed(identifier);
      if (!expression && _external_sql_identifier_resolver_proxy) {
        // Try to resolve the identifier in the outer queries
        expression = _external_sql_identifier_resolver_proxy->resolve_identifier_relaxed(identifier);
      }
      AssertInput(expression, "Couldn't resolve identifier '" + identifier.as_string() + "' or it is ambiguous");

      return expression;
    }

    case hsql::kExprLiteralFloat:
      return std::make_shared<ValueExpression>(expr.fval);

    case hsql::kExprLiteralString:
      AssertInput(expr.name, "No value given for string literal");
      return std::make_shared<ValueExpression>(name);

    case hsql::kExprLiteralInt:
      if (static_cast<int32_t>(expr.ival) == expr.ival) {
        return std::make_shared<ValueExpression>(static_cast<int32_t>(expr.ival));
      } else {
        return std::make_shared<ValueExpression>(expr.ival);
      }

    case hsql::kExprLiteralNull:
      return std::make_shared<ValueExpression>(NullValue{});

    case hsql::kExprParameter: {
      Assert(expr.ival >= 0 && expr.ival <= std::numeric_limits<ValuePlaceholderID::base_type>::max(),
             "ValuePlaceholderID out of range");
      auto value_placeholder_id = ValuePlaceholderID{static_cast<uint16_t>(expr.ival)};
      return std::make_shared<ParameterExpression>(
          _parameter_id_allocator->allocate_for_value_placeholder(value_placeholder_id));
    }

    case hsql::kExprFunctionRef: {
      // convert to upper-case to find mapping
      std::transform(name.begin(), name.end(), name.begin(), [](const auto c) { return std::toupper(c); });

      if (name == "EXTRACT"s) {
        Assert(expr.datetimeField != hsql::kDatetimeNone, "No DatetimeField specified in EXTRACT. Bug in sqlparser?");

        auto datetime_component = hsql_datetime_field.at(expr.datetimeField);
        return std::make_shared<ExtractExpression>(datetime_component, left);
      }

      Assert(expr.exprList, "FunctionRef has no exprList. Bug in sqlparser?");

      /**
       * Aggregate function
       */
      const auto aggregate_iter = aggregate_function_to_string.right.find(name);
      if (aggregate_iter != aggregate_function_to_string.right.end()) {
        auto aggregate_function = aggregate_iter->second;

        if (aggregate_function == AggregateFunction::Count && expr.distinct) {
          aggregate_function = AggregateFunction::CountDistinct;
        }

        AssertInput(expr.exprList && expr.exprList->size() == 1,
                    "Expected exactly one argument for this AggregateFunction");

        switch (aggregate_function) {
          case AggregateFunction::Min:
          case AggregateFunction::Max:
          case AggregateFunction::Sum:
          case AggregateFunction::Avg:
            return std::make_shared<AggregateExpression>(
                aggregate_function, _translate_hsql_expr(*expr.exprList->front(), sql_identifier_resolver));

          case AggregateFunction::Count:
          case AggregateFunction::CountDistinct:
            if (expr.exprList->front()->type == hsql::kExprStar) {
              AssertInput(!expr.exprList->front()->name, "Illegal <t>.* in COUNT()");
              return std::make_shared<AggregateExpression>(aggregate_function);
            } else {
              return std::make_shared<AggregateExpression>(
                  aggregate_function, _translate_hsql_expr(*expr.exprList->front(), sql_identifier_resolver));
            }
        }
      }

      /**
       * "Normal" function
       */
      const auto function_iter = function_type_to_string.right.find(name);

      if (function_iter != function_type_to_string.right.end()) {
        auto arguments = std::vector<std::shared_ptr<AbstractExpression>>{};
        arguments.reserve(expr.exprList->size());

        for (const auto* hsql_argument : *expr.exprList) {
          arguments.emplace_back(_translate_hsql_expr(*hsql_argument, sql_identifier_resolver));
        }

        return std::make_shared<FunctionExpression>(function_iter->second, arguments);
      } else {
        FailInput("Couldn't resolve function '"s + name + "'");
      }
    }

    case hsql::kExprOperator: {
      // Translate ArithmeticExpression
      const auto arithmetic_operators_iter = hsql_arithmetic_operators.find(expr.opType);
      if (arithmetic_operators_iter != hsql_arithmetic_operators.end()) {
        Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary expression.");
        return std::make_shared<ArithmeticExpression>(arithmetic_operators_iter->second, left, right);
      }

      // Translate PredicateExpression
      const auto predicate_condition_iter = hsql_predicate_condition.find(expr.opType);
      if (predicate_condition_iter != hsql_predicate_condition.end()) {
        const auto predicate_condition = predicate_condition_iter->second;

        if (is_binary_predicate_condition(predicate_condition)) {
          Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary_expression");
          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
        } else if (predicate_condition == PredicateCondition::Between) {
          Assert(expr.exprList && expr.exprList->size() == 2, "Expected two arguments for BETWEEN");
          return std::make_shared<BetweenExpression>(
              left, _translate_hsql_expr(*(*expr.exprList)[0], sql_identifier_resolver),
              _translate_hsql_expr(*(*expr.exprList)[1], sql_identifier_resolver));
        }
      }

      // Translate all other expression types
      switch (expr.opType) {
        case hsql::kOpUnaryMinus:
          return std::make_shared<UnaryMinusExpression>(left);
        case hsql::kOpCase:
          return _translate_hsql_case(expr, sql_identifier_resolver);
        case hsql::kOpOr:
          return std::make_shared<LogicalExpression>(LogicalOperator::Or, left, right);
        case hsql::kOpAnd:
          return std::make_shared<LogicalExpression>(LogicalOperator::And, left, right);
        case hsql::kOpIn: {
          if (expr.select) {
            // `a IN (SELECT ...)`
            const auto sub_select = _translate_hsql_sub_select(*expr.select, sql_identifier_resolver);
            return std::make_shared<InExpression>(left, sub_select);

          } else {
            // `a IN (x, y, z)`
            std::vector<std::shared_ptr<AbstractExpression>> arguments;
            if (expr.exprList) {
              arguments.reserve(expr.exprList->size());
              for (const auto* hsql_argument : *expr.exprList) {
                arguments.emplace_back(_translate_hsql_expr(*hsql_argument, sql_identifier_resolver));
              }
            }

            const auto array = std::make_shared<ListExpression>(arguments);
            return std::make_shared<InExpression>(left, array);
          }
        }

        case hsql::kOpIsNull:
          return is_null_(left);

        case hsql::kOpNot: {
          // If the argument is a predicate, just inverse it (e.g. NOT (a > b) becomes b <= a)
          if (left->type == ExpressionType::Predicate) {
            if (const auto binary_predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(left);
                binary_predicate_expression) {
              return std::make_shared<BinaryPredicateExpression>(
                  inverse_predicate_condition(binary_predicate_expression->predicate_condition),
                  binary_predicate_expression->left_operand(), binary_predicate_expression->right_operand());
            } else if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(left);
                       is_null_expression) {
              return std::make_shared<IsNullExpression>(
                  inverse_predicate_condition(is_null_expression->predicate_condition), is_null_expression->operand());
            }
          }

          /**
           * "NOT <some_expression>" becomes "<some_expression> == 0"
           */
          return equals_(left, 0);
        }

        case hsql::kOpExists:
          AssertInput(expr.select, "Expected SELECT argument for EXISTS");
          return std::make_shared<ExistsExpression>(_translate_hsql_sub_select(*expr.select, sql_identifier_resolver));

        default:
          FailInput("Not handling this OperatorType yet");
      }
    }

    case hsql::kExprSelect:
      return _translate_hsql_sub_select(*expr.select, sql_identifier_resolver);

    case hsql::kExprArray:
      FailInput("Can't translate a standalone array, arrays only valid in IN expressions");

    case hsql::kExprHint:
    case hsql::kExprStar:
    case hsql::kExprArrayIndex:
      FailInput("Can't translate this hsql expression into a Hyrise expression");

    default:
      FailInput("Unknown expression type, can't translate expression");
  }
}

std::shared_ptr<LQPSelectExpression> SQLTranslator::_translate_hsql_sub_select(
    const hsql::SelectStatement& select, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) const {
  const auto sql_identifier_proxy = std::make_shared<SQLIdentifierResolverProxy>(
      sql_identifier_resolver, _parameter_id_allocator, _external_sql_identifier_resolver_proxy);

  auto sub_select_translator = SQLTranslator{_use_mvcc, sql_identifier_proxy, _parameter_id_allocator};
  const auto sub_select_lqp = sub_select_translator._translate_select_statement(select);
  const auto parameter_count = sql_identifier_proxy->accessed_expressions().size();

  auto parameter_ids = std::vector<ParameterID>{};
  parameter_ids.reserve(parameter_count);

  auto parameter_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  parameter_expressions.reserve(parameter_count);

  for (const auto& expression_and_parameter_id : sql_identifier_proxy->accessed_expressions()) {
    parameter_ids.emplace_back(expression_and_parameter_id.second);
    parameter_expressions.emplace_back(expression_and_parameter_id.first);
  }

  return std::make_shared<LQPSelectExpression>(sub_select_lqp, parameter_ids, parameter_expressions);
}

std::shared_ptr<AbstractExpression> SQLTranslator::_translate_hsql_case(
    const hsql::Expr& expr, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) const {
  /**
   * There is a "simple" and a "searched" CASE syntax, see http://www.oratable.com/simple-case-searched-case/
   * Hyrise supports both.
   */

  Assert(expr.exprList, "Unexpected SQLParserResult. Case needs exprList");
  Assert(!expr.exprList->empty(), "Unexpected SQLParserResult. Case needs non-empty exprList");

  // "a + b" in "CASE a + b WHEN ... THEN ... END", or nullptr when using the "searched" CASE syntax
  auto simple_case_left_operand = std::shared_ptr<AbstractExpression>{};
  if (expr.expr) simple_case_left_operand = _translate_hsql_expr(*expr.expr, sql_identifier_resolver);

  // Initialize CASE with the ELSE expression and then put the remaining WHEN...THEN... clauses on top of that
  // in reverse order
  auto current_case_expression = std::shared_ptr<AbstractExpression>{};
  if (expr.expr2) {
    current_case_expression = _translate_hsql_expr(*expr.expr2, sql_identifier_resolver);
  } else {
    // No ELSE specified, use NULL
    current_case_expression = std::make_shared<ValueExpression>(NullValue{});
  }

  for (auto case_reverse_idx = size_t{0}; case_reverse_idx < expr.exprList->size(); ++case_reverse_idx) {
    const auto case_idx = expr.exprList->size() - case_reverse_idx - 1;
    const auto case_clause = (*expr.exprList)[case_idx];

    auto when = _translate_hsql_expr(*case_clause->expr, sql_identifier_resolver);
    if (simple_case_left_operand) {
      when = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, simple_case_left_operand, when);
    }

    const auto then = _translate_hsql_expr(*case_clause->expr2, sql_identifier_resolver);
    current_case_expression = std::make_shared<CaseExpression>(when, then, current_case_expression);
  }

  return current_case_expression;
}

SQLTranslator::TableSourceState::TableSourceState(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::unordered_map<std::string, std::vector<std::shared_ptr<AbstractExpression>>>& elements_by_table_name,
    const std::vector<std::shared_ptr<AbstractExpression>>& elements_in_order,
    const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver)
    : lqp(lqp),
      elements_by_table_name(elements_by_table_name),
      elements_in_order(elements_in_order),
      sql_identifier_resolver(sql_identifier_resolver) {}

void SQLTranslator::TableSourceState::append(TableSourceState&& rhs) {
  for (auto& table_name_and_elements : rhs.elements_by_table_name) {
    const auto unique = elements_by_table_name.count(table_name_and_elements.first) == 0;
    AssertInput(unique, "Table Name '"s + table_name_and_elements.first + "' in FROM clause is not unique");
  }

  // This should be ::merge, but that is not yet supported by clang.
  // elements_by_table_name.merge(std::move(rhs.elements_by_table_name));
  for (auto& kv : rhs.elements_by_table_name) {
    elements_by_table_name.try_emplace(kv.first, std::move(kv.second));
  }

  elements_in_order.insert(elements_in_order.end(), rhs.elements_in_order.begin(), rhs.elements_in_order.end());
  sql_identifier_resolver->append(std::move(*rhs.sql_identifier_resolver));
}

}  // namespace opossum
