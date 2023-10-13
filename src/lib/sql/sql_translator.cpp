#include "sql_translator.hpp"

#include "create_sql_parser_error_message.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/window_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/change_meta_table_node.hpp"
#include "logical_query_plan/create_prepared_plan_node.hpp"
#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_table_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/except_node.hpp"
#include "logical_query_plan/export_node.hpp"
#include "logical_query_plan/import_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/intersect_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "logical_query_plan/window_node.hpp"
#include "storage/lqp_view.hpp"
#include "storage/table.hpp"
#include "utils/date_time_utils.hpp"
#include "utils/meta_table_manager.hpp"

#include "SQLParser.h"

namespace {

using namespace std::string_literals;           // NOLINT(build/namespaces)
using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

const auto hsql_arithmetic_operators = std::unordered_map<hsql::OperatorType, ArithmeticOperator>{
    {hsql::kOpPlus, ArithmeticOperator::Addition},           {hsql::kOpMinus, ArithmeticOperator::Subtraction},
    {hsql::kOpAsterisk, ArithmeticOperator::Multiplication}, {hsql::kOpSlash, ArithmeticOperator::Division},
    {hsql::kOpPercentage, ArithmeticOperator::Modulo},
};

const auto hsql_logical_operators = std::unordered_map<hsql::OperatorType, LogicalOperator>{
    {hsql::kOpAnd, LogicalOperator::And}, {hsql::kOpOr, LogicalOperator::Or}};

const auto hsql_predicate_condition = std::unordered_map<hsql::OperatorType, PredicateCondition>{
    {hsql::kOpBetween, PredicateCondition::BetweenInclusive},
    {hsql::kOpEquals, PredicateCondition::Equals},
    {hsql::kOpNotEquals, PredicateCondition::NotEquals},
    {hsql::kOpLess, PredicateCondition::LessThan},
    {hsql::kOpLessEq, PredicateCondition::LessThanEquals},
    {hsql::kOpGreater, PredicateCondition::GreaterThan},
    {hsql::kOpGreaterEq, PredicateCondition::GreaterThanEquals},
    {hsql::kOpLike, PredicateCondition::Like},
    {hsql::kOpNotLike, PredicateCondition::NotLike},
    {hsql::kOpIsNull, PredicateCondition::IsNull}};

const auto hsql_datetime_field = std::unordered_map<hsql::DatetimeField, DatetimeComponent>{
    {hsql::kDatetimeYear, DatetimeComponent::Year},     {hsql::kDatetimeMonth, DatetimeComponent::Month},
    {hsql::kDatetimeDay, DatetimeComponent::Day},       {hsql::kDatetimeHour, DatetimeComponent::Hour},
    {hsql::kDatetimeMinute, DatetimeComponent::Minute}, {hsql::kDatetimeSecond, DatetimeComponent::Second},
};

const auto order_type_to_sort_mode = std::unordered_map<hsql::OrderType, SortMode>{
    {hsql::kOrderAsc, SortMode::Ascending},
    {hsql::kOrderDesc, SortMode::Descending},
};

const auto supported_hsql_data_types = std::unordered_map<hsql::DataType, DataType>{
    {hsql::DataType::INT, DataType::Int},     {hsql::DataType::LONG, DataType::Long},
    {hsql::DataType::FLOAT, DataType::Float}, {hsql::DataType::DOUBLE, DataType::Double},
    {hsql::DataType::TEXT, DataType::String}, {hsql::DataType::BIGINT, DataType::Long}};

JoinMode translate_join_mode(const hsql::JoinType join_type) {
  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinFull, JoinMode::FullOuter}, {hsql::kJoinLeft, JoinMode::Left},
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
  if (expression.type != ExpressionType::Predicate) {
    return false;
  }

  const auto* binary_predicate_expression = dynamic_cast<const BinaryPredicateExpression*>(&expression);
  if (!binary_predicate_expression) {
    return false;
  }

  const auto find_predicate_in_input = [&](const auto& input) {
    auto left_argument_found = false;
    auto right_argument_found = false;

    input.iterate_output_expressions([&](const auto /*column_id*/, const auto& expression) {
      if (*expression == *binary_predicate_expression->left_operand()) {
        left_argument_found = true;
      } else if (*expression == *binary_predicate_expression->right_operand()) {
        right_argument_found = true;
      }
      return AbstractLQPNode::ExpressionIteration::Continue;
    });
    return std::make_pair(left_argument_found, right_argument_found);
  };

  const auto [left_in_left, right_in_left] = find_predicate_in_input(left_input);
  const auto [left_in_right, right_in_right] = find_predicate_in_input(right_input);
  return (left_in_left && right_in_right) || (right_in_left && left_in_right);
}

std::shared_ptr<AbstractLQPNode> add_expressions_if_unavailable(
    const std::shared_ptr<AbstractLQPNode>& node, const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  auto projection_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& expression : expressions) {
    // The required expression is already available or does not need to be computed (e.g., when it is a literal).
    if (!expression->requires_computation() || node->find_column_id(*expression)) {
      continue;
    }
    projection_expressions.emplace_back(expression);
  }

  // If all requested expressions are available, no need to create a Projection.
  if (projection_expressions.empty()) {
    return node;
  }

  const auto output_expressions = node->output_expressions();
  projection_expressions.insert(projection_expressions.end(), output_expressions.cbegin(), output_expressions.cend());

  // If the current LQP already is a ProjectionNode, do not add another one.
  if (node->type == LQPNodeType::Projection) {
    node->node_expressions = projection_expressions;
    return node;
  }

  return ProjectionNode::make(projection_expressions, node);
}

std::vector<std::shared_ptr<AbstractExpression>> unwrap_elements(
    const std::vector<SQLTranslator::SelectListElement>& select_list_elements) {
  auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  expressions.reserve(select_list_elements.size());
  for (const auto& element : select_list_elements) {
    expressions.emplace_back(element.expression);
  }
  return expressions;
}

std::string trim_meta_table_name(const std::string& name) {
  DebugAssert(MetaTableManager::is_meta_table_name(name), name + " is not a meta table name.");
  return name.substr(MetaTableManager::META_PREFIX.size());
}

std::shared_ptr<AbstractExpression> inverse_predicate(const AbstractExpression& expression) {
  /**
   * Inverse a boolean expression
   */

  switch (expression.type) {
    case ExpressionType::Predicate: {
      if (const auto* binary_predicate_expression = dynamic_cast<const BinaryPredicateExpression*>(&expression);
          binary_predicate_expression) {
        // If the argument is a predicate, just inverse it (e.g. NOT (a > b) becomes b <= a)
        return std::make_shared<BinaryPredicateExpression>(
            inverse_predicate_condition(binary_predicate_expression->predicate_condition),
            binary_predicate_expression->left_operand(), binary_predicate_expression->right_operand());
      }

      if (const auto* const is_null_expression = dynamic_cast<const IsNullExpression*>(&expression);
          is_null_expression) {
        // NOT (IS NULL ...) -> IS NOT NULL ...
        return std::make_shared<IsNullExpression>(inverse_predicate_condition(is_null_expression->predicate_condition),
                                                  is_null_expression->operand());
      }

      if (const auto* const between_expression = dynamic_cast<const BetweenExpression*>(&expression);
          between_expression) {
        // a BETWEEN b AND c -> a < b OR a > c
        return or_(less_than_(between_expression->operand(), between_expression->lower_bound()),
                   greater_than_(between_expression->operand(), between_expression->upper_bound()));
      }

      const auto* in_expression = dynamic_cast<const InExpression*>(&expression);
      Assert(in_expression, "Expected InExpression");
      return std::make_shared<InExpression>(inverse_predicate_condition(in_expression->predicate_condition),
                                            in_expression->operand(), in_expression->set());
    } break;

    case ExpressionType::Logical: {
      const auto* logical_expression = static_cast<const LogicalExpression*>(&expression);

      switch (logical_expression->logical_operator) {
        case LogicalOperator::And:
          return or_(inverse_predicate(*logical_expression->left_operand()),
                     inverse_predicate(*logical_expression->right_operand()));
        case LogicalOperator::Or:
          return and_(inverse_predicate(*logical_expression->left_operand()),
                      inverse_predicate(*logical_expression->right_operand()));
      }
    } break;

    case ExpressionType::Exists: {
      const auto* exists_expression = static_cast<const ExistsExpression*>(&expression);

      switch (exists_expression->exists_expression_type) {
        case ExistsExpressionType::Exists:
          return not_exists_(exists_expression->subquery());
        case ExistsExpressionType::NotExists:
          return exists_(exists_expression->subquery());
      }
    } break;

    default:
      Fail("Cannot invert non-boolean expression.");
  }

  Fail("Invalid enum value.");
}

FrameBound translate_frame_bound(const hsql::FrameBound& hsql_frame_bound) {
  auto bound_type = FrameBoundType::CurrentRow;
  switch (hsql_frame_bound.type) {
    case hsql::FrameBoundType::kCurrentRow:
      break;
    case hsql::FrameBoundType::kPreceding:
      bound_type = FrameBoundType::Preceding;
      break;
    case hsql::FrameBoundType::kFollowing:
      bound_type = FrameBoundType::Following;
  }

  const auto offset = hsql_frame_bound.offset;
  Assert(offset >= 0, "Expected non-negative offset. Bug in sqlparser?");

  return FrameBound{static_cast<uint64_t>(offset), bound_type, hsql_frame_bound.unbounded};
}

}  // namespace

namespace hyrise {

SQLTranslator::SQLTranslator(const UseMvcc use_mvcc)
    : SQLTranslator(use_mvcc, nullptr, std::make_shared<ParameterIDAllocator>(),
                    std::unordered_map<std::string, std::shared_ptr<LQPView>>{},
                    std::make_shared<std::unordered_map<std::string, std::shared_ptr<Table>>>()) {}

SQLTranslationResult SQLTranslator::translate_parser_result(const hsql::SQLParserResult& result) {
  _cacheable = true;

  auto result_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  for (const hsql::SQLStatement* stmt : result.getStatements()) {
    auto result_node = _translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  const auto& parameter_ids_of_value_placeholders = _parameter_id_allocator->value_placeholders();
  auto parameter_ids = std::vector<ParameterID>{parameter_ids_of_value_placeholders.size()};

  for (const auto& [value_placeholder_id, parameter_id] : parameter_ids_of_value_placeholders) {
    parameter_ids[value_placeholder_id] = parameter_id;
  }

  return {result_nodes, {_cacheable, parameter_ids}};
}

SQLTranslator::SQLTranslator(
    const UseMvcc use_mvcc, const std::shared_ptr<SQLIdentifierResolverProxy>& external_sql_identifier_resolver_proxy,
    const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
    const std::unordered_map<std::string, std::shared_ptr<LQPView>>& with_descriptions,
    const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Table>>>& meta_tables)
    : _use_mvcc(use_mvcc),
      _external_sql_identifier_resolver_proxy(external_sql_identifier_resolver_proxy),
      _parameter_id_allocator(parameter_id_allocator),
      _with_descriptions(with_descriptions),
      _meta_tables(meta_tables) {}

SQLTranslator::SQLTranslator(
    const UseMvcc use_mvcc, const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Table>>>& meta_tables)
    : SQLTranslator(use_mvcc, nullptr, std::make_shared<ParameterIDAllocator>(),
                    std::unordered_map<std::string, std::shared_ptr<LQPView>>{}, meta_tables) {}

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
    case hsql::kStmtPrepare:
      return _translate_prepare(static_cast<const hsql::PrepareStatement&>(statement));
    case hsql::kStmtExecute:
      return _translate_execute(static_cast<const hsql::ExecuteStatement&>(statement));
    case hsql::kStmtImport:
      return _translate_import(static_cast<const hsql::ImportStatement&>(statement));
    case hsql::kStmtExport:
      return _translate_export(static_cast<const hsql::ExportStatement&>(statement));
    case hsql::kStmtTransaction:
      // The transaction statements are handled directly in the SQLPipelineStatement,
      //  but the translation is still called, so we return a dummy node here.
      return DummyTableNode::make();
    case hsql::kStmtAlter:
    case hsql::kStmtError:
    case hsql::kStmtRename:
      FailInput("Statement type not supported");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_select_statement(const hsql::SelectStatement& select) {
  // SQL Orders of Operations
  // 1. WITH clause
  // 2. FROM clause (incl. JOINs and sub-SELECTs that are part of this)
  // 3. SELECT list (to retrieve aliases)
  // 4. WHERE clause
  // 5. GROUP BY clause
  // 6. HAVING clause
  // 7. SELECT clause (incl. DISTINCT)
  // 8. ORDER BY clause
  // 9. LIMIT clause
  // 10. UNION/INTERSECT/EXCEPT clause
  // 11. UNION/INTERSECT/EXCEPT ORDER BY clause
  // 12. UNION/INTERSECT/EXCEPT LIMIT clause

  AssertInput(select.selectList, "SELECT list needs to exist");
  AssertInput(!select.selectList->empty(), "SELECT list needs to have entries");

  // Translate WITH clause
  if (select.withDescriptions) {
    for (const auto& with_description : *select.withDescriptions) {
      _translate_hsql_with_description(*with_description);
    }
  }

  // Translate FROM.
  if (select.fromTable) {
    _from_clause_result = _translate_table_ref(*select.fromTable);
    _current_lqp = _from_clause_result->lqp;
    _sql_identifier_resolver = _from_clause_result->sql_identifier_resolver;
  } else {
    _current_lqp = std::make_shared<DummyTableNode>();
    _sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();
  }

  // Translate SELECT list (to retrieve aliases).
  const auto select_list_elements = _translate_select_list(*select.selectList);

  // Translate WHERE.
  if (select.whereClause) {
    const auto where_expression = _translate_hsql_expr(*select.whereClause, _sql_identifier_resolver);
    _current_lqp = _translate_predicate_expression(where_expression, _current_lqp);
  }

  // Translate SELECT, HAVING, GROUP BY in one go, as they are interdependent.
  _translate_select_groupby_having(select, select_list_elements);

  // Translate ORDER BY and DISTINCT. ORDER BY and LIMIT must be executed after DISTINCT. Thus, we must ensure that all
  // ORDER BY expressions are part of the SELECT list if a DISTINCT result is required.
  const auto& inflated_select_list_expressions = unwrap_elements(_inflated_select_list_elements);
  _translate_distinct_order_by(select.order, inflated_select_list_expressions, select.selectDistinct);

  // Translate LIMIT.
  if (select.limit) {
    _translate_limit(*select.limit);
  }

  // Project, arrange, and name the columns as specified in the SELECT clause.
  //
  // 1. Add a ProjectionNode if necessary.
  if (!expressions_equal(_current_lqp->output_expressions(), inflated_select_list_expressions)) {
    _current_lqp = ProjectionNode::make(inflated_select_list_expressions, _current_lqp);
  }

  // 2. Check whether we need to create an AliasNode. This is the case whenever an expression was assigned a
  //    column_name that is not its generated name.
  const auto need_alias_node = std::any_of(
      _inflated_select_list_elements.begin(), _inflated_select_list_elements.end(), [](const auto& element) {
        return std::any_of(element.identifiers.begin(), element.identifiers.end(), [&](const auto& identifier) {
          return identifier.column_name != element.expression->as_column_name();
        });
      });

  if (need_alias_node) {
    auto aliases = std::vector<std::string>{};
    for (const auto& element : _inflated_select_list_elements) {
      if (!element.identifiers.empty()) {
        aliases.emplace_back(element.identifiers.back().column_name);
      } else {
        aliases.emplace_back(element.expression->as_column_name());
      }
    }

    _current_lqp = AliasNode::make(unwrap_elements(_inflated_select_list_elements), aliases, _current_lqp);
  }

  if (select.setOperations) {
    for (const auto* const set_operator : *select.setOperations) {
      // Currently, only the SQL translation of intersect and except is implemented.
      AssertInput(set_operator->setType != hsql::kSetUnion, "Union Operations are currently not supported");
      _translate_set_operation(*set_operator);

      // In addition to local ORDER BY and LIMIT clauses, the result of the set operation(s) may have final clauses,
      // too. Consider the following example query (returns the first ten dates when store_sales happened, except the
      // days one of the five web_sales with the highest price happened):
      //     SELECT DISTINCT sold_date
      //                FROM (SELECT sold_date FROM store_sales)
      //              EXCEPT (SELECT sold_date FROM web_sales
      //                    ORDER BY sales_price DESC
      //                       LIMIT 5)
      //            ORDER BY sold_date ASC
      //               LIMIT 10;
      // While ORDER BY sales_price DESC LIMIT 5 belongs to the subquery and has to be executed locally, ORDER BY
      // sold_date ASC LIMIT 10 refers to the intersection and must be executed on the result.
      _translate_distinct_order_by(set_operator->resultOrder, inflated_select_list_expressions, select.selectDistinct);
      if (set_operator->resultLimit) {
        _translate_limit(*set_operator->resultLimit);
      }
    }
  }

  return _current_lqp;
}

void SQLTranslator::_translate_hsql_with_description(hsql::WithDescription& desc) {
  auto with_translator = SQLTranslator{_use_mvcc, nullptr, _parameter_id_allocator, _with_descriptions, _meta_tables};
  const auto lqp = with_translator._translate_select_statement(*desc.select);

  // Save mappings: ColumnID -> ColumnName
  auto column_names = std::unordered_map<ColumnID, std::string>{};
  const auto output_expressions = lqp->output_expressions();
  const auto output_expression_count = output_expressions.size();
  for (auto column_id = ColumnID{0}; column_id < output_expression_count; ++column_id) {
    for (const auto& identifier : with_translator._inflated_select_list_elements[column_id].identifiers) {
      column_names.insert_or_assign(column_id, identifier.column_name);
    }
  }

  // Store resolved WithDescription / temporary view
  const auto lqp_view = std::make_shared<LQPView>(lqp, column_names);
  // A WITH description masks a preceding WITH description if their aliases are identical
  AssertInput(!_with_descriptions.contains(desc.alias), "Invalid redeclaration of WITH alias.");
  _with_descriptions.emplace(desc.alias, lqp_view);
}

std::shared_ptr<AbstractExpression> SQLTranslator::translate_hsql_expr(const hsql::Expr& hsql_expr,
                                                                       const UseMvcc use_mvcc) {
  // Create an empty SQLIdentifier context - thus the expression cannot refer to any external columns
  return SQLTranslator{use_mvcc, _meta_tables}._translate_hsql_expr(hsql_expr,
                                                                    std::make_shared<SQLIdentifierResolver>());
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_insert(const hsql::InsertStatement& insert) {
  const auto table_name = std::string{insert.tableName};

  const bool is_meta_table = MetaTableManager::is_meta_table_name(table_name);

  auto target_table = std::shared_ptr<Table>{};
  if (is_meta_table) {
    auto sql_identifier_resolver =
        _sql_identifier_resolver ? _sql_identifier_resolver : std::make_shared<SQLIdentifierResolver>();
    _translate_meta_table(table_name, sql_identifier_resolver);
    AssertInput(Hyrise::get().meta_table_manager.can_insert_into(table_name), "Cannot insert into " + table_name);
    target_table = _meta_tables->at(trim_meta_table_name(table_name));
  } else {
    AssertInput(Hyrise::get().storage_manager.has_table(table_name),
                std::string{"Did not find a table with name "} + table_name);
    target_table = Hyrise::get().storage_manager.get_table(table_name);
  }

  auto insert_data_node = std::shared_ptr<AbstractLQPNode>{};
  auto output_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
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
    output_expressions = insert_data_node->output_expressions();

  } else {
    // `INSERT INTO table_name [(column1, column2, column3, ...)] VALUES (value1, value2, value3, ...);`
    AssertInput(insert.values, "INSERT INTO ... VALUES: No values given");

    output_expressions.reserve(insert.values->size());
    for (const auto* value : *insert.values) {
      output_expressions.emplace_back(_translate_hsql_expr(*value, _sql_identifier_resolver));
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

    AssertInput(insert.columns->size() == output_expressions.size(),
                "INSERT: Target column count and number of input columns mismatch");

    auto expressions = std::vector<std::shared_ptr<AbstractExpression>>(target_table->column_count(), null_());
    auto source_column_id = ColumnID{0};
    for (const auto& column_name : *insert.columns) {
      // retrieve correct ColumnID from the target table
      const auto target_column_id = target_table->column_id_by_name(column_name);
      expressions[target_column_id] = output_expressions[source_column_id];
      ++source_column_id;
    }
    output_expressions = expressions;

    insert_data_projection_required = true;
  }

  /**
   * 3. When inserting NULL literals (or not inserting into all columns), wrap NULLs in
   *    `CAST(NULL AS <column_data_type>), since a temporary table with the data to insert will be created and NULL is
   *    an invalid column data type in Hyrise.
   */
  for (auto column_id = ColumnID{0}; column_id < target_table->column_count(); ++column_id) {
    // Turn `expression` into `CAST(expression AS <column_data_type>)` if expression is a NULL literal
    auto expression = output_expressions[column_id];
    if (const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression); value_expression) {
      if (variant_is_null(value_expression->value)) {
        output_expressions[column_id] = cast_(null_(), target_table->column_data_type(column_id));
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
    if (expression_contains_placeholder(output_expressions[column_id]) ||
        target_table->column_data_type(column_id) != output_expressions[column_id]->data_type()) {
      output_expressions[column_id] = cast_(output_expressions[column_id], target_table->column_data_type(column_id));
    }
  }

  /**
   * 5. Project the data to insert ONLY if required, i.e. when column order needed to be arranged or NULLs were wrapped
   *    in `CAST(NULL as <data_type>)`
   */
  if (insert_data_projection_required) {
    insert_data_node = ProjectionNode::make(output_expressions, insert_data_node);
  }

  AssertInput(insert_data_node->output_expressions().size() == static_cast<size_t>(target_table->column_count()),
              "INSERT: Column count mismatch");

  if (is_meta_table) {
    return ChangeMetaTableNode::make(table_name, MetaTableChangeType::Insert, DummyTableNode::make(), insert_data_node);
  }

  /**
   * NOTE: DataType checking has to be done at runtime, as Query could still contain Placeholder with unspecified type
   */
  return InsertNode::make(table_name, insert_data_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_delete(const hsql::DeleteStatement& delete_statement) {
  const auto table_name = std::string{delete_statement.tableName};

  const auto sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();
  const bool is_meta_table = MetaTableManager::is_meta_table_name(table_name);

  auto data_to_delete_node = std::shared_ptr<AbstractLQPNode>{};

  if (is_meta_table) {
    data_to_delete_node = _translate_meta_table(delete_statement.tableName, sql_identifier_resolver);
    AssertInput(Hyrise::get().meta_table_manager.can_delete_from(table_name), "Cannot delete from " + table_name);
  } else {
    data_to_delete_node = _translate_stored_table(delete_statement.tableName, sql_identifier_resolver);
    Assert(lqp_is_validated(data_to_delete_node), "DELETE expects rows to be deleted to have been validated");
  }

  if (delete_statement.expr) {
    const auto delete_where_expression = _translate_hsql_expr(*delete_statement.expr, sql_identifier_resolver);
    data_to_delete_node = _translate_predicate_expression(delete_where_expression, data_to_delete_node);
  }

  if (is_meta_table) {
    return ChangeMetaTableNode::make(table_name, MetaTableChangeType::Delete, data_to_delete_node,
                                     DummyTableNode::make());
  }
  return DeleteNode::make(data_to_delete_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_update(const hsql::UpdateStatement& update) {
  AssertInput(update.table->type == hsql::kTableName, "UPDATE can only reference table by name");

  const auto table_name = std::string{update.table->name};

  auto translation_state = _translate_table_ref(*update.table);

  const bool is_meta_table = MetaTableManager::is_meta_table_name(table_name);

  auto target_table = std::shared_ptr<Table>{};
  if (is_meta_table) {
    AssertInput(Hyrise::get().meta_table_manager.can_update(table_name), "Cannot update " + table_name);
    target_table = _meta_tables->at(trim_meta_table_name(table_name));
  } else {
    AssertInput(Hyrise::get().storage_manager.has_table(table_name),
                std::string{"Did not find a table with name "} + table_name);
    target_table = Hyrise::get().storage_manager.get_table(table_name);
  }

  // The LQP that selects the fields to update
  auto selection_lqp = translation_state.lqp;

  // Take a copy intentionally, we're going to replace some of these later
  auto update_expressions = selection_lqp->output_expressions();

  // The update operator wants ReferenceSegments on its left side. Also, we should make sure that we do not update
  // invalid rows.
  Assert(is_meta_table || lqp_is_validated(selection_lqp), "UPDATE expects rows to be updated to have been validated");

  if (update.where) {
    const auto where_expression = _translate_hsql_expr(*update.where, translation_state.sql_identifier_resolver);
    selection_lqp = _translate_predicate_expression(where_expression, selection_lqp);
  }

  for (const auto* update_clause : *update.updates) {
    const auto column_name = std::string{update_clause->column};
    const auto column_expression = translation_state.sql_identifier_resolver->resolve_identifier_relaxed(column_name);
    const auto column_id = selection_lqp->get_column_id(*column_expression);

    update_expressions[column_id] =
        _translate_hsql_expr(*update_clause->value, translation_state.sql_identifier_resolver);
  }

  // Perform type conversions if necessary so the types of the inserted data exactly matches the table column types
  for (auto column_id = ColumnID{0}; column_id < target_table->column_count(); ++column_id) {
    // Always cast if the expression contains a placeholder, since we can't know the actual data type of the expression
    // until it is replaced.
    if (expression_contains_placeholder(update_expressions[column_id]) ||
        target_table->column_data_type(column_id) != update_expressions[column_id]->data_type()) {
      update_expressions[column_id] = cast_(update_expressions[column_id], target_table->column_data_type(column_id));
    }
  }

  // LQP that computes the updated values
  const auto updated_values_lqp = ProjectionNode::make(update_expressions, selection_lqp);

  if (is_meta_table) {
    return ChangeMetaTableNode::make(table_name, MetaTableChangeType::Update, selection_lqp, updated_values_lqp);
  }
  return UpdateNode::make(table_name, selection_lqp, updated_values_lqp);
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
  }
  Fail("Invalid enum value");
}

SQLTranslator::TableSourceState SQLTranslator::_translate_table_origin(const hsql::TableRef& hsql_table_ref) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // Each element in the FROM list needs to have a unique table name (i.e. Subqueries are required to have an ALIAS)
  auto table_name = std::string{};
  auto sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();
  auto select_list_elements = std::vector<SelectListElement>{};

  switch (hsql_table_ref.type) {
    case hsql::kTableName: {
      // WITH descriptions or subqueries are treated as though they were inline views or tables
      // They mask existing tables or views with the same name.
      const auto with_descriptions_iter = _with_descriptions.find(hsql_table_ref.name);
      if (with_descriptions_iter != _with_descriptions.end()) {
        const auto lqp_view = with_descriptions_iter->second->deep_copy();
        lqp = lqp_view->lqp;

        // Add all named columns to the IdentifierContext
        const auto output_expressions = lqp_view->lqp->output_expressions();
        const auto output_expression_count = output_expressions.size();
        for (auto column_id = ColumnID{0}; column_id < output_expression_count; ++column_id) {
          const auto& expression = output_expressions[column_id];

          const auto column_name_iter = lqp_view->column_names.find(column_id);
          if (column_name_iter != lqp_view->column_names.end()) {
            sql_identifier_resolver->add_column_name(expression, column_name_iter->second);
          }
          sql_identifier_resolver->set_table_name(expression, hsql_table_ref.name);
        }

      } else if (Hyrise::get().storage_manager.has_table(hsql_table_ref.name)) {
        lqp = _translate_stored_table(hsql_table_ref.name, sql_identifier_resolver);

      } else if (MetaTableManager::is_meta_table_name(hsql_table_ref.name)) {
        lqp = _translate_meta_table(hsql_table_ref.name, sql_identifier_resolver);

      } else if (Hyrise::get().storage_manager.has_view(hsql_table_ref.name)) {
        const auto view = Hyrise::get().storage_manager.get_view(hsql_table_ref.name);
        lqp = view->lqp;

        /**
         * Add all named columns from the view to the IdentifierContext
         */
        const auto output_expressions = view->lqp->output_expressions();
        const auto output_expression_count = output_expressions.size();
        for (auto column_id = ColumnID{0}; column_id < output_expression_count; ++column_id) {
          const auto& expression = output_expressions[column_id];

          const auto column_name_iter = view->column_names.find(column_id);
          if (column_name_iter != view->column_names.end()) {
            sql_identifier_resolver->add_column_name(expression, column_name_iter->second);
          }
          sql_identifier_resolver->set_table_name(expression, hsql_table_ref.name);
        }

        AssertInput(_use_mvcc == (lqp_is_validated(view->lqp) ? UseMvcc::Yes : UseMvcc::No),
                    "Mismatch between validation of View and query it is used in");
      } else {
        FailInput(std::string("Did not find a table or view with name ") + hsql_table_ref.name);
      }
      table_name = hsql_table_ref.alias ? hsql_table_ref.alias->name : hsql_table_ref.name;

      for (const auto& expression : lqp->output_expressions()) {
        const auto identifiers = sql_identifier_resolver->get_expression_identifiers(expression);
        select_list_elements.emplace_back(expression, identifiers);
      }
    } break;

    case hsql::kTableSelect: {
      AssertInput(hsql_table_ref.alias && hsql_table_ref.alias->name, "Every nested SELECT must have its own alias");
      table_name = hsql_table_ref.alias->name;

      auto subquery_translator = SQLTranslator{_use_mvcc, _external_sql_identifier_resolver_proxy,
                                               _parameter_id_allocator, _with_descriptions, _meta_tables};
      lqp = subquery_translator._translate_select_statement(*hsql_table_ref.select);

      // If this statement or any of the subquery's statements is not cacheable (because of meta tables),
      // this statement should not be cacheable.
      _cacheable &= subquery_translator._cacheable;

      auto identifiers = std::vector<std::vector<SQLIdentifier>>{};
      identifiers.reserve(subquery_translator._inflated_select_list_elements.size());
      for (const auto& element : subquery_translator._inflated_select_list_elements) {
        identifiers.emplace_back(element.identifiers);
      }

      const auto output_expressions = lqp->output_expressions();
      const auto output_expression_count = output_expressions.size();
      Assert(identifiers.size() == output_expression_count,
             "There have to be as many identifier lists as output expressions");
      for (auto select_list_element_idx = size_t{0}; select_list_element_idx < output_expression_count;
           ++select_list_element_idx) {
        const auto& subquery_expression = output_expressions[select_list_element_idx];

        // Make sure each column from the Subquery has a name
        if (identifiers.empty()) {
          sql_identifier_resolver->add_column_name(subquery_expression, subquery_expression->as_column_name());
        }
        for (const auto& identifier : identifiers[select_list_element_idx]) {
          sql_identifier_resolver->add_column_name(subquery_expression, identifier.column_name);
        }

        select_list_elements.emplace_back(subquery_expression, identifiers[select_list_element_idx]);
      }

      table_name = hsql_table_ref.alias->name;
    } break;

    case hsql::kTableJoin:
    case hsql::kTableCrossProduct:
      // These should not make it this far.
      Fail("Unexpected table reference type");
  }

  // Rename columns as in "SELECT * FROM t AS x (y,z)"
  if (hsql_table_ref.alias && hsql_table_ref.alias->columns) {
    const auto& output_expressions = lqp->output_expressions();

    const auto table_ref_alias_column_count = hsql_table_ref.alias->columns->size();
    AssertInput(table_ref_alias_column_count == output_expressions.size(),
                "Must specify a name for exactly each column");
    Assert(table_ref_alias_column_count == select_list_elements.size(),
           "There have to be as many aliases as output expressions");

    std::set<std::shared_ptr<AbstractExpression>> renamed_expressions;
    for (auto column_id = ColumnID{0}; column_id < table_ref_alias_column_count; ++column_id) {
      const auto& expression = output_expressions[column_id];

      if (renamed_expressions.find(expression) == renamed_expressions.end()) {
        // The original column names should not be accessible anymore because the table schema is renamed.
        sql_identifier_resolver->reset_column_names(expression);
        renamed_expressions.insert(expression);
      }

      const auto& column_name = (*hsql_table_ref.alias->columns)[column_id];
      sql_identifier_resolver->add_column_name(expression, column_name);
      select_list_elements[column_id].identifiers.clear();
      select_list_elements[column_id].identifiers.emplace_back(column_name);
    }
  }

  for (const auto& expression : lqp->output_expressions()) {
    sql_identifier_resolver->set_table_name(expression, table_name);
  }

  return {lqp,
          {{
              {table_name, select_list_elements},
          }},
          {select_list_elements},
          sql_identifier_resolver};
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_stored_table(
    const std::string& name, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) {
  AssertInput(Hyrise::get().storage_manager.has_table(name), std::string{"Did not find a table with name "} + name);

  const auto stored_table_node = StoredTableNode::make(name);
  auto validated_stored_table_node = _validate_if_active(stored_table_node);

  const auto table = Hyrise::get().storage_manager.get_table(name);

  // Publish the columns of the table in the SQLIdentifierResolver
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    const auto& column_definition = table->column_definitions()[column_id];
    const auto column_expression = lqp_column_(stored_table_node, column_id);
    sql_identifier_resolver->add_column_name(column_expression, column_definition.name);
    sql_identifier_resolver->set_table_name(column_expression, name);
  }

  return validated_stored_table_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_meta_table(
    const std::string& name, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) {
  AssertInput(Hyrise::get().meta_table_manager.has_table(name), std::string{"Did not find a table with name "} + name);

  // MetaTables are non-cacheable because they might contain information about the general system state
  // that can change at any time
  _cacheable = false;

  const auto meta_table_name = trim_meta_table_name(name);

  // Meta tables are integrated in the LQP as static table nodes in order to avoid regeneration at every
  // access in the pipeline afterwards.
  std::shared_ptr<Table> meta_table;
  if (_meta_tables->contains(meta_table_name)) {
    meta_table = _meta_tables->at(meta_table_name);
  } else {
    meta_table = Hyrise::get().meta_table_manager.generate_table(meta_table_name);
    (*_meta_tables)[meta_table_name] = meta_table;
  }

  const auto static_table_node = StaticTableNode::make(meta_table);

  // Publish the columns of the table in the SQLIdentifierResolver
  for (auto column_id = ColumnID{0}; column_id < meta_table->column_count(); ++column_id) {
    const auto& column_definition = meta_table->column_definitions()[column_id];
    const auto column_expression = lqp_column_(static_table_node, column_id);
    sql_identifier_resolver->add_column_name(column_expression, column_definition.name);
    sql_identifier_resolver->set_table_name(column_expression, name);
  }

  return static_table_node;
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
  const auto raw_join_predicate_cnf = flatten_logical_expressions(raw_join_predicate, LogicalOperator::And);

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

  AssertInput(join_mode != JoinMode::FullOuter || (left_local_predicates.empty() && right_local_predicates.empty()),
              "Local predicates not supported for full outer joins. See #1436");
  AssertInput(join_mode != JoinMode::Left || left_local_predicates.empty(),
              "Local predicates not supported on left side of left outer join. See #1436");
  AssertInput(join_mode != JoinMode::Right || right_local_predicates.empty(),
              "Local predicates not supported on right side of right outer join. See #1436");

  /**
   * Add local predicates - ignore local predicates on the preserving side of OUTER JOINs
   */
  if (join_mode != JoinMode::Left && join_mode != JoinMode::FullOuter) {
    for (const auto& left_local_predicate : left_local_predicates) {
      left_input_lqp = _translate_predicate_expression(left_local_predicate, left_input_lqp);
    }
  }
  if (join_mode != JoinMode::Right && join_mode != JoinMode::FullOuter) {
    for (const auto& right_local_predicate : right_local_predicates) {
      right_input_lqp = _translate_predicate_expression(right_local_predicate, right_input_lqp);
    }
  }

  /**
   * Add the join predicates
   */
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  if (join_mode != JoinMode::Inner && join_predicates.size() > 1) {
    lqp = JoinNode::make(join_mode, join_predicates, left_input_lqp, right_input_lqp);
  } else {
    const auto join_predicate_iter =
        std::find_if(join_predicates.begin(), join_predicates.end(), [&](const auto& join_predicate) {
          return is_trivial_join_predicate(*join_predicate, *left_input_lqp, *right_input_lqp);
        });

    // Inner Joins with predicates like `5 + t0.a = 6+ t1.b` can be supported via Cross join + Scan. For all other join
    // modes such predicates are not supported.
    AssertInput(join_mode == JoinMode::Inner || join_predicate_iter != join_predicates.end(),
                "Non column-to-column comparison in join predicate only supported for inner joins");

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
  for (const auto& right_element : right_state.elements_in_order) {
    const auto& right_expression = right_element.expression;
    const auto& right_identifiers = right_element.identifiers;

    if (!right_identifiers.empty()) {
      // Ignore previous names if there is an alias
      const auto right_identifier = right_identifiers.back();

      const auto left_expression =
          left_sql_identifier_resolver->resolve_identifier_relaxed({right_identifier.column_name});

      if (left_expression) {
        // Two columns match, let's join on them.
        join_predicates.emplace_back(equals_(left_expression, right_expression));
        continue;
      }

      // No matching column in the left input found, add the column from the right input to the output
      result_state.elements_in_order.emplace_back(right_element);
      result_state.sql_identifier_resolver->add_column_name(right_expression, right_identifier.column_name);
      if (right_identifier.table_name) {
        result_state.elements_by_table_name[*right_identifier.table_name].emplace_back(right_element);
        result_state.sql_identifier_resolver->set_table_name(right_expression, *right_identifier.table_name);
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
  const auto join_predicate_count = join_predicates.size();
  for (auto join_predicate_idx = size_t{1}; join_predicate_idx < join_predicate_count; ++join_predicate_idx) {
    lqp = PredicateNode::make(join_predicates[join_predicate_idx], lqp);
  }

  if (!join_predicates.empty()) {
    // Projection Node to remove duplicate columns
    lqp = ProjectionNode::make(unwrap_elements(result_state.elements_in_order), lqp);
  }

  // Create output TableSourceState
  result_state.lqp = lqp;

  return result_state;
}

SQLTranslator::TableSourceState SQLTranslator::_translate_cross_product(const std::vector<hsql::TableRef*>& tables) {
  Assert(!tables.empty(), "Cannot translate cross product without tables");

  auto result_table_source_state = _translate_table_ref(*tables.front());

  const auto table_count = tables.size();
  for (auto table_idx = size_t{1}; table_idx < table_count; ++table_idx) {
    auto table_source_state = _translate_table_ref(*tables[table_idx]);
    result_table_source_state.lqp =
        JoinNode::make(JoinMode::Cross, result_table_source_state.lqp, table_source_state.lqp);
    result_table_source_state.append(std::move(table_source_state));
  }

  return result_table_source_state;
}

std::vector<SQLTranslator::SelectListElement> SQLTranslator::_translate_select_list(
    const std::vector<hsql::Expr*>& select_list) {
  // Build the select_list_elements
  // Each expression of a select_list_element is either an Expression or nullptr if the element is a Wildcard
  // Create an SQLIdentifierResolver that knows the aliases
  auto select_list_elements = std::vector<SelectListElement>{};
  auto post_select_sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>(*_sql_identifier_resolver);
  for (const auto& hsql_select_expr : select_list) {
    if (hsql_select_expr->type == hsql::kExprStar) {
      select_list_elements.emplace_back(nullptr);
    } else if (hsql_select_expr->type == hsql::kExprLiteralInterval) {
      FailInput("Interval can only be added to or substracted from a date");
    } else {
      const auto expression = _translate_hsql_expr(*hsql_select_expr, _sql_identifier_resolver, true);
      select_list_elements.emplace_back(expression);
      if (hsql_select_expr->name && hsql_select_expr->type != hsql::kExprFunctionRef &&
          hsql_select_expr->type != hsql::kExprExtract) {
        select_list_elements.back().identifiers.emplace_back(hsql_select_expr->name);
      }

      if (hsql_select_expr->alias) {
        auto identifier = SQLIdentifier{hsql_select_expr->alias};
        if (hsql_select_expr->table) {
          identifier.table_name = hsql_select_expr->table;
        }
        post_select_sql_identifier_resolver->add_column_name(expression, hsql_select_expr->alias);
        select_list_elements.back().identifiers.emplace_back(identifier);
      }
    }
  }
  _sql_identifier_resolver = post_select_sql_identifier_resolver;
  return select_list_elements;
}

void SQLTranslator::_translate_select_groupby_having(const hsql::SelectStatement& select,
                                                     const std::vector<SelectListElement>& select_list_elements) {
  auto pre_aggregate_expression_set = ExpressionUnorderedSet{};
  auto pre_aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto aggregate_expression_set = ExpressionUnorderedSet{};
  auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto window_expression_set = ExpressionUnorderedSet{};
  auto window_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  // Visitor that identifies still uncomputed WindowFunctionExpressions and their arguments.
  const auto find_uncomputed_aggregates_and_arguments = [&](auto& sub_expression) {
    /**
     * If the WindowFunctionExpression has already been computed in a previous node (consider "x" in
     * "SELECT x FROM (SELECT MIN(a) as x FROM t) AS y)", it does not count as a new Aggregate and is therefore not
     * considered an "Aggregate" in the current SELECT list. Handling this as a special case seems hacky, but it is the
     * best solution we came up with.
     */
    if (_current_lqp->find_column_id(*sub_expression)) {
      return ExpressionVisitation::DoNotVisitArguments;
    }

    if (sub_expression->type != ExpressionType::WindowFunction) {
      return ExpressionVisitation::VisitArguments;
    }

    const auto& window_expression = std::static_pointer_cast<WindowFunctionExpression>(sub_expression);
    const auto& window = window_expression->window();
    if (window && window_expression_set.emplace(window_expression).second) {
      window_expressions.emplace_back(window_expression);
      return ExpressionVisitation::VisitArguments;
    }

    if (aggregate_expression_set.emplace(window_expression).second) {
      aggregate_expressions.emplace_back(window_expression);
      for (const auto& argument : window_expression->arguments) {
        if (pre_aggregate_expression_set.emplace(argument).second) {
          // Handle COUNT(*)
          const auto* const column_expression = dynamic_cast<const LQPColumnExpression*>(&*argument);
          if (!column_expression || column_expression->original_column_id != INVALID_COLUMN_ID) {
            pre_aggregate_expressions.emplace_back(argument);
          }
        }
      }
    }

    return ExpressionVisitation::DoNotVisitArguments;
  };

  // Identify all WindowExpressions and their arguments needed for SELECT.
  for (const auto& element : select_list_elements) {
    if (element.expression) {
      visit_expression(element.expression, find_uncomputed_aggregates_and_arguments);
    }
  }

  // Identify all GROUP BY expressions.
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

  // Gather all WindowExpressions and arguments from HAVING.
  auto having_expression = std::shared_ptr<AbstractExpression>{};
  if (select.groupBy && select.groupBy->having) {
    having_expression = _translate_hsql_expr(*select.groupBy->having, _sql_identifier_resolver);
    visit_expression(having_expression, find_uncomputed_aggregates_and_arguments);
  }

  const auto is_aggregate = !aggregate_expressions.empty() || !group_by_expressions.empty();

  const auto pre_aggregate_lqp = _current_lqp;

  // Build AggregateNodes.
  if (is_aggregate) {
    // If needed, add a ProjectionNode to evaluate all expressions required for GROUP BY/aggregates.
    if (!pre_aggregate_expressions.empty()) {
      const auto& output_expressions = _current_lqp->output_expressions();
      const auto any_expression_not_yet_available = std::any_of(
          pre_aggregate_expressions.cbegin(), pre_aggregate_expressions.cend(),
          [&](const auto& expression) { return !find_expression_idx(*expression, output_expressions).has_value(); });

      if (any_expression_not_yet_available) {
        _current_lqp = ProjectionNode::make(pre_aggregate_expressions, _current_lqp);
      }
    }
    _current_lqp = AggregateNode::make(group_by_expressions, aggregate_expressions, _current_lqp);
  }

  // Build HAVING.
  if (having_expression) {
    AssertInput(expression_evaluable_on_lqp(having_expression, *_current_lqp),
                "HAVING references columns not accessible after Aggregation");
    _current_lqp = _translate_predicate_expression(having_expression, _current_lqp);
  }

  // Build WindowNodes.
  if (!window_expressions.empty()) {
    auto computed_expressions = _current_lqp->output_expressions();
    const auto computed_expression_set =
        ExpressionUnorderedSet{computed_expressions.begin(), computed_expressions.end()};
    auto required_expressions = ExpressionUnorderedSet{};
    auto window_nodes = std::vector<std::shared_ptr<WindowNode>>{};
    window_nodes.reserve(window_expressions.size());

    // The argument of the window function, PARTITION BY expressions, and ORDER BY expressions must either be present
    // or can be computed by a ProjectionNode. This also means a window function cannot depend on the result of
    // another window function.
    for (const auto& expression : window_expressions) {
      const auto& window_function = static_cast<const WindowFunctionExpression&>(*expression);
      const auto& argument = window_function.argument();
      AssertInput(!argument || expression_evaluable_on_lqp(argument, *_current_lqp),
                  "Argument of window function " + window_function.as_column_name() + " is not available.");
      if (argument && !computed_expression_set.contains(argument)) {
        required_expressions.emplace(argument);
      }

      // We ensured there is a window above.
      const auto& window = static_cast<const WindowExpression&>(*window_function.window());
      for (const auto& required_expression : window.arguments) {
        AssertInput(
            expression_evaluable_on_lqp(required_expression, *_current_lqp),
            "Required column " + required_expression->as_column_name() + " for window definition is not available.");
        if (!computed_expression_set.contains(required_expression)) {
          required_expressions.emplace(required_expression);
        }
      }
      window_nodes.emplace_back(WindowNode::make(expression));
    }

    // Add a ProjectionNode for uncomputed expressions.
    if (!required_expressions.empty()) {
      computed_expressions.insert(computed_expressions.end(), required_expressions.cbegin(),
                                  required_expressions.cend());
      _current_lqp = ProjectionNode::make(computed_expressions, _current_lqp);
    }

    // Add WindowNodes on top of the LQP. For now, their order is just the same as in the SELECT list.
    for (const auto& window_node : window_nodes) {
      window_node->set_left_input(_current_lqp);
      _current_lqp = window_node;
    }
  }

  const auto select_list_size = select.selectList->size();
  for (auto select_list_idx = size_t{0}; select_list_idx < select_list_size; ++select_list_idx) {
    const auto* hsql_expr = (*select.selectList)[select_list_idx];

    if (hsql_expr->type == hsql::kExprStar) {
      AssertInput(_from_clause_result, "Can't SELECT with wildcards since there are no FROM tables specified");

      if (is_aggregate) {
        // SELECT * is only valid if every input column is named in the GROUP BY clause
        for (const auto& pre_aggregate_expression : pre_aggregate_lqp->output_expressions()) {
          if (hsql_expr->table) {
            // Dealing with SELECT t.* here
            auto identifiers = _sql_identifier_resolver->get_expression_identifiers(pre_aggregate_expression);
            if (std::any_of(identifiers.begin(), identifiers.end(),
                            [&](const auto& identifier) { return identifier.table_name != hsql_expr->table; })) {
              // The pre_aggregate_expression may or may not be part of the GROUP BY clause, but since it comes from a
              // different table, it is not included in the `SELECT t.*`.
              continue;
            }
          }

          AssertInput(std::find_if(group_by_expressions.begin(), group_by_expressions.end(),
                                   [&](const auto& group_by_expression) {
                                     return *pre_aggregate_expression == *group_by_expression;
                                   }) != group_by_expressions.end(),
                      std::string("Expression ") + pre_aggregate_expression->as_column_name() +
                          " was added to SELECT list when resolving *, but it is not part of the GROUP BY clause");
        }
      }

      if (hsql_expr->table) {
        if (is_aggregate) {
          // Select all GROUP BY columns with the specified table name
          for (const auto& group_by_expression : group_by_expressions) {
            const auto identifiers = _sql_identifier_resolver->get_expression_identifiers(group_by_expression);
            for (const auto& identifier : identifiers) {
              if (identifier.table_name == hsql_expr->table) {
                _inflated_select_list_elements.emplace_back(group_by_expression);
              }
            }
          }
        } else {
          // Select all columns from the FROM element with the specified name
          const auto from_element_iter = _from_clause_result->elements_by_table_name.find(hsql_expr->table);
          AssertInput(from_element_iter != _from_clause_result->elements_by_table_name.end(),
                      std::string("No such element in FROM with table name '") + hsql_expr->table + "'");

          for (const auto& element : from_element_iter->second) {
            _inflated_select_list_elements.emplace_back(element);
          }
        }
      } else {
        if (is_aggregate) {
          // Select all GROUP BY columns
          for (const auto& expression : group_by_expressions) {
            _inflated_select_list_elements.emplace_back(expression);
          }
        } else {
          // Select all columns from the FROM elements
          _inflated_select_list_elements.insert(_inflated_select_list_elements.end(),
                                                _from_clause_result->elements_in_order.begin(),
                                                _from_clause_result->elements_in_order.end());
        }
      }
    } else {
      _inflated_select_list_elements.emplace_back(select_list_elements[select_list_idx]);
    }
  }
}

void SQLTranslator::_translate_set_operation(const hsql::SetOperation& set_operator) {
  const auto& left_input_lqp = _current_lqp;
  const auto left_output_expressions = left_input_lqp->output_expressions();

  // The right-hand side of the set operation has to be translated independently and must not access SQL identifiers
  // from the left-hand side. To ensure this, we create a new SQLTranslator with its own SQLIdentifierResolver.
  auto nested_set_translator = SQLTranslator{_use_mvcc, _external_sql_identifier_resolver_proxy,
                                             _parameter_id_allocator, _with_descriptions, _meta_tables};
  const auto right_input_lqp = nested_set_translator._translate_select_statement(*set_operator.nestedSelectStatement);
  const auto right_output_expressions = right_input_lqp->output_expressions();

  const auto left_output_expression_count = left_output_expressions.size();
  const auto right_output_expression_count = right_output_expressions.size();
  AssertInput(left_output_expression_count == right_output_expression_count,
              "Mismatching number of input columns for set operation");

  // Check to see if both input LQPs use the same data type for each column
  for (auto expression_idx = ColumnID{0}; expression_idx < left_output_expression_count; ++expression_idx) {
    const auto& left_expression = left_output_expressions[expression_idx];
    const auto& right_expression = right_output_expressions[expression_idx];

    AssertInput(left_expression->data_type() == right_expression->data_type(),
                "Mismatching input data types for left and right side of set operation");
  }

  auto lqp = std::shared_ptr<AbstractLQPNode>();

  // Choose the set operation mode. SQL only knows UNION and UNION ALL; the Positions mode is only used for internal
  // LQP optimizations and should not be needed in the SQLTranslator.
  auto set_operation_mode = set_operator.isAll ? SetOperationMode::All : SetOperationMode::Unique;

  // Create corresponding node depending on the SetType
  switch (set_operator.setType) {
    case hsql::kSetExcept:
      lqp = ExceptNode::make(set_operation_mode, left_input_lqp, right_input_lqp);
      break;
    case hsql::kSetIntersect:
      lqp = IntersectNode::make(set_operation_mode, left_input_lqp, right_input_lqp);
      break;
    case hsql::kSetUnion:
      lqp = UnionNode::make(set_operation_mode, left_input_lqp, right_input_lqp);
      break;
  }

  _current_lqp = lqp;
}

void SQLTranslator::_translate_distinct_order_by(const std::vector<hsql::OrderDescription*>* order_list,
                                                 const std::vector<std::shared_ptr<AbstractExpression>>& select_list,
                                                 const bool distinct) {
  const auto perform_sort = order_list && !order_list->empty();
  auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto sort_modes = std::vector<SortMode>{};

  if (perform_sort) {
    const auto& hsql_order_expressions = *order_list;
    const auto order_list_size = hsql_order_expressions.size();
    expressions.resize(order_list_size);
    sort_modes.resize(order_list_size);
    for (auto expression_idx = size_t{0}; expression_idx < order_list_size; ++expression_idx) {
      const auto& order_description = hsql_order_expressions[expression_idx];
      expressions[expression_idx] = _translate_hsql_expr(*order_description->expr, _sql_identifier_resolver);
      sort_modes[expression_idx] = order_type_to_sort_mode.at(order_description->type);
    }

    _current_lqp = add_expressions_if_unavailable(_current_lqp, expressions);
  }

  // For SELECT DISTINCT, we add an AggregateNode that groups by all output columns, but does not use any aggregate
  // functions, e.g.: `SELECT DISTINCT a, b ...` becomes `SELECT a, b ... GROUP BY a, b`.
  //
  // This might create unnecessary AggregateNodes when we already have an aggregation that creates unique results:
  // `SELECT DISTINCT a, MIN(b) FROM t GROUP BY a` would have one aggregate that groups by a and calculates MIN(b), and
  // one that groups by both a and MIN(b) without calculating anything. Fixing this is done by an optimizer rule
  // (DependentGroupByReductionRule) that checks if the respective columns are already unique.
  if (distinct) {
    if (perform_sort) {
      // If we later sort the table by the ORDER BY expression, we must ensure they are also part of the SELECT list
      // (DISTINCT will be applied before ORDER BY).
      const auto& select_expressions_set = ExpressionUnorderedSet{select_list.begin(), select_list.end()};
      AssertInput(std::all_of(expressions.cbegin(), expressions.cend(),
                              [&](const auto& expression) { return select_expressions_set.contains(expression); }),
                  "For SELECT DISTINCT, ORDER BY expressions must appear in the SELECT list.");
    }

    // Add currently uncomputed expressions, e.g., a + 1 for SELECT DISTINCT a + 1 FROM table_a.
    _current_lqp = add_expressions_if_unavailable(_current_lqp, select_list);

    _current_lqp = AggregateNode::make(select_list, expression_vector(), _current_lqp);
  }

  // If any expressions were added to perform the sorting, we must add a ProjectionNode later and remove them again in
  // _translate_select_statement(...).
  if (perform_sort) {
    _current_lqp = SortNode::make(expressions, sort_modes, _current_lqp);
  }
}

void SQLTranslator::_translate_limit(const hsql::LimitDescription& limit) {
  AssertInput(!limit.offset, "OFFSET not supported");
  const auto num_rows_expression = _translate_hsql_expr(*limit.limit, _sql_identifier_resolver);
  _current_lqp = LimitNode::make(num_rows_expression, _current_lqp);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_show(const hsql::ShowStatement& show_statement) {
  _cacheable = false;

  switch (show_statement.type) {
    case hsql::ShowType::kShowTables: {
      const auto tables_meta_table = Hyrise::get().meta_table_manager.generate_table("tables");
      return StaticTableNode::make(tables_meta_table);
    }
    case hsql::ShowType::kShowColumns: {
      const auto columns_meta_table = Hyrise::get().meta_table_manager.generate_table("columns");
      const auto static_table_node = StaticTableNode::make(columns_meta_table);
      const auto table_name_column = lqp_column_(static_table_node, ColumnID{0});
      const auto predicate = equals_(table_name_column, value_(show_statement.name));
      return PredicateNode::make(predicate, static_table_node);
    }
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create(const hsql::CreateStatement& create_statement) {
  switch (create_statement.type) {
    case hsql::CreateType::kCreateView:
      return _translate_create_view(create_statement);
    case hsql::CreateType::kCreateTable:
      return _translate_create_table(create_statement);
    case hsql::CreateType::kCreateTableFromTbl:
      FailInput("CREATE TABLE FROM is not yet supported");
    case hsql::CreateType::kCreateIndex:
      FailInput("CREATE INDEX is not yet supported");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create_view(const hsql::CreateStatement& create_statement) {
  auto lqp = _translate_select_statement(static_cast<const hsql::SelectStatement&>(*create_statement.select));
  const auto output_expressions = lqp->output_expressions();

  auto column_names = std::unordered_map<ColumnID, std::string>{};

  if (create_statement.viewColumns) {
    // The CREATE VIEW statement has renamed the columns: CREATE VIEW myview (foo, bar) AS SELECT ...
    const auto view_column_count = create_statement.viewColumns->size();
    AssertInput(view_column_count == output_expressions.size(),
                "Number of Columns in CREATE VIEW does not match SELECT statement");

    for (auto column_id = ColumnID{0}; column_id < view_column_count; ++column_id) {
      column_names.insert_or_assign(column_id, (*create_statement.viewColumns)[column_id]);
    }
  } else {
    const auto output_expression_count = output_expressions.size();
    for (auto column_id = ColumnID{0}; column_id < output_expression_count; ++column_id) {
      for (const auto& identifier : _inflated_select_list_elements[column_id].identifiers) {
        column_names.insert_or_assign(column_id, identifier.column_name);
      }
    }
  }

  return CreateViewNode::make(create_statement.tableName, std::make_shared<LQPView>(lqp, column_names),
                              create_statement.ifNotExists);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create_table(const hsql::CreateStatement& create_statement) {
  Assert(create_statement.columns || create_statement.select, "CREATE TABLE: No columns specified. Parser bug?");

  auto input_node = std::shared_ptr<AbstractLQPNode>{};

  if (create_statement.select) {
    input_node = _translate_select_statement(*create_statement.select);
  } else {
    auto column_definitions = TableColumnDefinitions{create_statement.columns->size()};
    auto table_key_constraints = TableKeyConstraints{};

    for (auto column_id = ColumnID{0}; column_id < create_statement.columns->size(); ++column_id) {
      const auto* parser_column_definition = create_statement.columns->at(column_id);
      auto& column_definition = column_definitions[column_id];

      // TODO(anybody) SQLParser is missing support for Hyrise's other types
      switch (parser_column_definition->type.data_type) {
        case hsql::DataType::SMALLINT:
          std::cout << "WARNING: Implicitly converting SMALLINT to INT\n";
          [[fallthrough]];
        case hsql::DataType::INT:
          column_definition.data_type = DataType::Int;
          break;
        case hsql::DataType::BIGINT:
        case hsql::DataType::LONG:
          column_definition.data_type = DataType::Long;
          break;
        case hsql::DataType::DECIMAL:
          std::cout << "WARNING: Implicitly converting DECIMAL to FLOAT\n";
          column_definition.data_type = DataType::Float;
          break;
        case hsql::DataType::REAL:
          std::cout << "WARNING: Implicitly converting REAL to FLOAT\n";
          column_definition.data_type = DataType::Float;
          break;
        case hsql::DataType::FLOAT:
          column_definition.data_type = DataType::Float;
          break;
        case hsql::DataType::DOUBLE:
          column_definition.data_type = DataType::Double;
          break;
        case hsql::DataType::CHAR:
          std::cout << "WARNING: Ignoring the length of CHAR. Hyrise's strings are not length-limited\n";
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::VARCHAR:
          std::cout << "WARNING: Ignoring the length of VARCHAR. Hyrise's strings are not length-limited\n";
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::TEXT:
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::DATE:
          std::cout << "WARNING: Parsing DATE to string since date and time data types are not yet supported\n";
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::DATETIME:
          std::cout << "WARNING: Parsing DATETIME to string since date and time data types are not yet supported\n";
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::TIME:
          std::cout << "WARNING: Parsing TIME to string since date and time data types are not yet supported\n";
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::BOOLEAN:
          std::cout << "WARNING: Parsing BOOLEAN to string\n";
          column_definition.data_type = DataType::String;
          break;
        case hsql::DataType::UNKNOWN:
          Fail("UNKNOWN data type cannot be handled here");
      }

      column_definition.name = parser_column_definition->name;
      column_definition.nullable = parser_column_definition->nullable;

      // Translate column constraints. We only address UNIQUE/PRIMARY KEY constraints for now.
      DebugAssert(parser_column_definition->column_constraints,
                  "Column " + column_definition.name + " is missing constraint information");
      for (const auto& column_constraint : *parser_column_definition->column_constraints) {
        if (column_constraint != hsql::ConstraintType::Unique &&
            column_constraint != hsql::ConstraintType::PrimaryKey) {
          continue;
        }
        const auto constraint_type = column_constraint == hsql::ConstraintType::PrimaryKey
                                         ? KeyConstraintType::PRIMARY_KEY
                                         : KeyConstraintType::UNIQUE;
        table_key_constraints.emplace(std::set<ColumnID>{column_id}, constraint_type);
        std::cout << "WARNING: " << magic_enum::enum_name(constraint_type) << " constraint for column "
                  << column_definition.name << " will not be enforced\n";
      }
    }

    // Translate table constraints. Note that a table constraint is either a unique constraint, a referential con-
    // straint, or a table check constraint per SQL standard. Some constraints can be set (i) when describing a single
    // column, see above, or (ii) as a property of the table, containing multiple columns. We only address UNIQUE/
    // PRIMARY KEY constraints for now.
    const auto column_count = column_definitions.size();
    for (const auto& table_constraint : *create_statement.tableConstraints) {
      Assert(table_constraint->type == hsql::ConstraintType::PrimaryKey ||
                 table_constraint->type == hsql::ConstraintType::Unique,
             "Only UNIQUE and PRIMARY KEY constraints are expected on a table level");
      const auto constraint_type = table_constraint->type == hsql::ConstraintType::PrimaryKey
                                       ? KeyConstraintType::PRIMARY_KEY
                                       : KeyConstraintType::UNIQUE;

      // Resolve column IDs
      DebugAssert(table_constraint->columnNames,
                  std::string{magic_enum::enum_name(constraint_type)} + " table constraint must contain columns");
      auto column_ids = std::set<ColumnID>{};
      for (const auto& constraint_column_name : *table_constraint->columnNames) {
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          auto& column_definition = column_definitions[column_id];
          if (column_definition.name == constraint_column_name) {
            column_ids.emplace(column_id);
            if (constraint_type == KeyConstraintType::PRIMARY_KEY) {
              column_definition.nullable = false;
              AssertInput(
                  !create_statement.columns->at(column_id)->column_constraints->contains(hsql::ConstraintType::Null),
                  "PRIMARY KEY column " + constraint_column_name + " must not be nullable");
            }
            break;
          }
        }
      }
      AssertInput(
          column_ids.size() == table_constraint->columnNames->size(),
          "Could not resolve columns of " + std::string{magic_enum::enum_name(constraint_type)} + " table constraint");
      table_key_constraints.emplace(column_ids, constraint_type);
      std::cout << "WARNING: " << magic_enum::enum_name(constraint_type) << " table constraint will not be enforced\n";
    }

    // Set table key constraints
    const auto table = Table::create_dummy_table(column_definitions);
    for (const auto& table_key_constraint : table_key_constraints) {
      table->add_soft_key_constraint(table_key_constraint);
    }
    input_node = StaticTableNode::make(table);
  }

  return CreateTableNode::make(create_statement.tableName, create_statement.ifNotExists, input_node);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_drop(const hsql::DropStatement& drop_statement) {
  switch (drop_statement.type) {
    case hsql::DropType::kDropView:
      return DropViewNode::make(drop_statement.name, drop_statement.ifExists);
    case hsql::DropType::kDropTable:
      return DropTableNode::make(drop_statement.name, drop_statement.ifExists);
    case hsql::DropType::kDropSchema:
    case hsql::DropType::kDropIndex:
    case hsql::DropType::kDropPreparedStatement:
      FailInput("This DROP type is not implemented yet");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_prepare(const hsql::PrepareStatement& prepare_statement) {
  auto parse_result = hsql::SQLParserResult{};
  hsql::SQLParser::parse(prepare_statement.query, &parse_result);

  AssertInput(parse_result.isValid(), create_sql_parser_error_message(prepare_statement.query, parse_result));
  AssertInput(parse_result.size() == 1u, "PREPAREd statement can only contain a single SQL statement");

  auto prepared_plan_translator = SQLTranslator{_use_mvcc};

  const auto translation_result = prepared_plan_translator.translate_parser_result(parse_result);
  Assert(translation_result.translation_info.cacheable, "Non-cacheable LQP nodes can't be part of prepared statements");

  const auto lqp = translation_result.lqp_nodes.at(0);

  const auto parameter_ids = translation_result.translation_info.parameter_ids_of_value_placeholders;

  const auto lqp_prepared_plan = std::make_shared<PreparedPlan>(lqp, parameter_ids);

  return CreatePreparedPlanNode::make(prepare_statement.name, lqp_prepared_plan);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_execute(const hsql::ExecuteStatement& execute_statement) {
  const auto num_parameters = execute_statement.parameters ? execute_statement.parameters->size() : 0;
  auto parameters = std::vector<std::shared_ptr<AbstractExpression>>{num_parameters};
  for (auto parameter_idx = size_t{0}; parameter_idx < num_parameters; ++parameter_idx) {
    parameters[parameter_idx] = translate_hsql_expr(*(*execute_statement.parameters)[parameter_idx], _use_mvcc);
  }

  const auto prepared_plan = Hyrise::get().storage_manager.get_prepared_plan(execute_statement.name);

  AssertInput(_use_mvcc == (lqp_is_validated(prepared_plan->lqp) ? UseMvcc::Yes : UseMvcc::No),
              "Mismatch between validation of Prepared statement and query it is used in");

  return prepared_plan->instantiate(parameters);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_import(const hsql::ImportStatement& import_statement) {
  // Querying tables that are freshly loaded is not easy as we need meta information, such as column names and data
  // types, to resolve queries and build the query plans. For instance, we need an origin node for column expressions
  // and to provide correct output expressions for subsequent nodes, some optimization rules access stored tables, and
  // so on. Anyway, we would need to decouple data loading and storing the table and have to load metadata or even the
  // whole table when translating the SQL statement.
  AssertInput(!import_statement.whereClause, "Predicates on imported files are not supported.");
  return ImportNode::make(import_statement.tableName, import_statement.filePath,
                          import_type_to_file_type(import_statement.type));
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_export(const hsql::ExportStatement& export_statement) {
  auto sql_identifier_resolver = std::make_shared<SQLIdentifierResolver>();
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  if (export_statement.select) {
    lqp = _translate_select_statement(*export_statement.select);
  } else {
    AssertInput(export_statement.tableName, "ExportStatement must either specify a table name or a SelectStatement.");
    const auto table_name = std::string{export_statement.tableName};
    if (MetaTableManager::is_meta_table_name(table_name)) {
      lqp = _translate_meta_table(table_name, sql_identifier_resolver);
    } else {
      // Get stored table as input (validated if MVCC is enabled)
      lqp = _translate_stored_table(export_statement.tableName, sql_identifier_resolver);
    }
  }

  return ExportNode::make(export_statement.filePath, import_type_to_file_type(export_statement.type), lqp);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_validate_if_active(
    const std::shared_ptr<AbstractLQPNode>& input_node) {
  if (_use_mvcc == UseMvcc::No) {
    return input_node;
  }

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
      return PredicateNode::make(expression, current_node);
    }

    case ExpressionType::Logical: {
      const auto logical_expression = std::static_pointer_cast<LogicalExpression>(expression);

      switch (logical_expression->logical_operator) {
        case LogicalOperator::And: {
          current_node = _translate_predicate_expression(logical_expression->right_operand(), current_node);
          return _translate_predicate_expression(logical_expression->left_operand(), current_node);
        }
        case LogicalOperator::Or:
          return PredicateNode::make(expression, current_node);
      }
    } break;

    case ExpressionType::Exists:
      return PredicateNode::make(expression, current_node);

    default:
      FailInput("Cannot use this ExpressionType as predicate");
  }

  Fail("Invalid enum value");
}

std::shared_ptr<AbstractExpression> SQLTranslator::_translate_hsql_expr(
    const hsql::Expr& expr, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver,
    bool allow_window_functions) {
  auto name = expr.name ? std::string(expr.name) : "";

  auto left = expr.expr ? _translate_hsql_expr(*expr.expr, sql_identifier_resolver, allow_window_functions) : nullptr;
  const auto right =
      expr.expr2 ? _translate_hsql_expr(*expr.expr2, sql_identifier_resolver, allow_window_functions) : nullptr;

  if (left) {
    AssertInput(left->type != ExpressionType::Interval, "IntervalExpression must follow another expression.");
  }
  if (right && right->type == ExpressionType::Interval) {
    AssertInput(expr.type == hsql::kExprOperator && (expr.opType == hsql::kOpPlus || expr.opType == hsql::kOpMinus),
                "Intervals can only be added or substracted");
  }

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
      return value_(expr.fval);

    case hsql::kExprLiteralString:
      AssertInput(expr.name, "No value given for string literal");
      return value_(pmr_string{name});

    case hsql::kExprLiteralInt:
      if (static_cast<int32_t>(expr.ival) == expr.ival) {
        return value_(static_cast<int32_t>(expr.ival));
      } else {
        return value_(expr.ival);
      }

    case hsql::kExprLiteralNull:
      return null_();

    case hsql::kExprLiteralDate: {
      if (name.size() == 10) {
        const auto timestamp = string_to_timestamp(name);
        if (timestamp) {
          return value_(pmr_string{name});
        }
      }
      FailInput("'" + name + "' is not a valid ISO 8601 extended date");
    }

    case hsql::kExprParameter: {
      Assert(expr.ival >= 0 && expr.ival <= std::numeric_limits<ValuePlaceholderID::base_type>::max(),
             "ValuePlaceholderID out of range");
      auto value_placeholder_id = ValuePlaceholderID{static_cast<uint16_t>(expr.ival)};
      return placeholder_(_parameter_id_allocator->allocate_for_value_placeholder(value_placeholder_id));
    }

    case hsql::kExprExtract: {
      Assert(expr.datetimeField != hsql::kDatetimeNone, "No DatetimeField specified in EXTRACT. Bug in sqlparser?");

      auto datetime_component = hsql_datetime_field.at(expr.datetimeField);
      return extract_(datetime_component, left);
    }

    case hsql::kExprFunctionRef: {
      // Translate window definition.
      auto window_description = std::shared_ptr<WindowExpression>();
      if (expr.windowDescription) {
        AssertInput(allow_window_functions,
                    "Window functions are only allowed in the SELECT list and must not be nested.");
        const auto& hsql_window_description = *expr.windowDescription;
        auto partition_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
        if (hsql_window_description.partitionList) {
          partition_by_expressions.reserve(hsql_window_description.partitionList->size());
          for (const auto expression : *hsql_window_description.partitionList) {
            partition_by_expressions.emplace_back(_translate_hsql_expr(*expression, sql_identifier_resolver));
          }
        }
        auto order_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
        auto sort_modes = std::vector<SortMode>{};
        if (hsql_window_description.orderList) {
          const auto& order_list = *hsql_window_description.orderList;
          const auto expression_count = order_list.size();
          order_by_expressions.reserve(expression_count);
          sort_modes.reserve(expression_count);
          for (const auto order_description : order_list) {
            order_by_expressions.emplace_back(_translate_hsql_expr(*order_description->expr, sql_identifier_resolver));
            sort_modes.emplace_back(order_type_to_sort_mode.at(order_description->type));
          }
        }

        Assert(hsql_window_description.frameDescription, "Window function has no FrameDescription. Bug in sqlparser?");
        const auto& hsql_frame_description = *hsql_window_description.frameDescription;
        auto frame_type = FrameType::Rows;
        switch (hsql_frame_description.type) {
          case hsql::FrameType::kRows:
            break;
          case hsql::FrameType::kRange:
            frame_type = FrameType::Range;
            break;
          case hsql::FrameType::kGroups:
            FailInput("GROUPS frames are not supported.");
        }

        Assert(hsql_frame_description.start && hsql_frame_description.end,
               "FrameDescription has no frame bounds. Bug in sqlparser?");
        const auto start = translate_frame_bound(*hsql_frame_description.start);
        const auto end = translate_frame_bound(*hsql_frame_description.end);

        // "Restrictions are that `frame_start` cannot be UNBOUNDED FOLLOWING, `frame_end` cannot be UNBOUNDED
        // PRECEDING, and the `frame_end` choice cannot appear earlier [...] than the `frame_start` choice does — for
        // example RANGE BETWEEN CURRENT ROW AND offset PRECEDING is not allowed. But, for example, ROWS BETWEEN 7
        //  PRECEDING AND 8 PRECEDING is allowed, even though it would never select any rows."
        // - https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
        // To ensure this behavior, we treat CURRENT ROW as an offset of 0, PRECEDING as a negative offset, and
        // FOLLOWING as a positive offset. If end offset < start offset, the frame definition is invalid.
        const auto relative_frame_offset = [](const auto& frame_bound) {
          auto offset = int64_t{0};
          if (frame_bound.type == FrameBoundType::Preceding) {
            offset =
                frame_bound.unbounded ? std::numeric_limits<int64_t>::min() : -static_cast<int64_t>(frame_bound.offset);
          } else if (frame_bound.type == FrameBoundType::Following) {
            offset =
                frame_bound.unbounded ? std::numeric_limits<int64_t>::max() : static_cast<int64_t>(frame_bound.offset);
          }

          return offset;
        };

        const auto start_offset = relative_frame_offset(start);
        const auto end_offset = relative_frame_offset(end);
        AssertInput((start.type != FrameBoundType::Following || !start.unbounded) &&
                        (end.type != FrameBoundType::Preceding || !end.unbounded) && end_offset >= start_offset,
                    "Frame starting from " + start.description() + " cannot end at " + end.description() + ".");

        auto frame_description = FrameDescription{frame_type, start, end};
        window_description = window_(std::move(partition_by_expressions), std::move(order_by_expressions),
                                     std::move(sort_modes), std::move(frame_description));
      }

      // Convert to upper-case to find mapping.
      std::transform(name.cbegin(), name.cend(), name.begin(),
                     [](const auto character) { return std::toupper(character); });

      // Some SQL functions have aliases, which we map to one unique identifier here.
      static const auto function_aliases = std::unordered_map<std::string, std::string>{{{"SUBSTRING"}, {"SUBSTR"}}};
      const auto found_alias = function_aliases.find(name);
      if (found_alias != function_aliases.end()) {
        name = found_alias->second;
      }

      Assert(expr.exprList, "FunctionRef has no exprList. Bug in sqlparser?");

      /**
       * Window/aggregate function.
       */
      const auto window_function_iter = window_function_to_string.right.find(name);
      if (window_function_iter != window_function_to_string.right.end()) {
        auto window_function = window_function_iter->second;
        if (!aggregate_functions.contains(window_function)) {
          AssertInput(allow_window_functions,
                      "Window functions are only allowed in the SELECT list and must not be nested.");
          AssertInput(window_description, "Window function " + name + " requires a window definition.");
          AssertInput(!expr.exprList || expr.exprList->empty(), "Window functions must not have an argument.");
        } else {
          AssertInput(expr.exprList && expr.exprList->size() == 1,
                      "Expected exactly one argument for an aggregate function.");
        }

        if (window_function == WindowFunction::Count && expr.distinct) {
          window_function = WindowFunction::CountDistinct;
        }

        auto aggregate_expression = std::shared_ptr<WindowFunctionExpression>{};

        switch (window_function) {
          case WindowFunction::Min:
          case WindowFunction::Max:
          case WindowFunction::Sum:
          case WindowFunction::Avg:
          case WindowFunction::StandardDeviationSample: {
            aggregate_expression = std::make_shared<WindowFunctionExpression>(
                window_function, _translate_hsql_expr(*expr.exprList->front(), sql_identifier_resolver),
                window_description);
          } break;
          case WindowFunction::Any:
            Fail("ANY() is an internal aggregation function.");
          case WindowFunction::Count:
          case WindowFunction::CountDistinct: {
            if (expr.exprList->front()->type == hsql::kExprStar) {
              AssertInput(!expr.exprList->front()->name, "Illegal <t>.* in COUNT()");

              // Find any leaf node below COUNT(*).
              auto leaf_node = std::shared_ptr<AbstractLQPNode>{};
              visit_lqp(_current_lqp, [&](const auto& node) {
                if (!node->left_input() && !node->right_input()) {
                  leaf_node = node;
                  return LQPVisitation::DoNotVisitInputs;
                }
                return LQPVisitation::VisitInputs;
              });
              Assert(leaf_node, "No leaf node found below COUNT(*)");

              const auto column_expression = lqp_column_(leaf_node, INVALID_COLUMN_ID);

              aggregate_expression =
                  std::make_shared<WindowFunctionExpression>(window_function, column_expression, window_description);
            } else {
              aggregate_expression = std::make_shared<WindowFunctionExpression>(
                  window_function, _translate_hsql_expr(*expr.exprList->front(), sql_identifier_resolver),
                  window_description);
            }
          } break;

          case WindowFunction::CumeDist:
          case WindowFunction::DenseRank:
          case WindowFunction::PercentRank:
          case WindowFunction::Rank:
          case WindowFunction::RowNumber: {
            aggregate_expression =
                std::make_shared<WindowFunctionExpression>(window_function, nullptr, window_description);
          } break;
        }

        // Check that the aggregate can be calculated on the given expression.
        const auto result_type = aggregate_expression->data_type();
        AssertInput(result_type != DataType::Null,
                    std::string{"Invalid aggregate "} + aggregate_expression->as_column_name() +
                        " for input data type " +
                        data_type_to_string.left.at(aggregate_expression->argument()->data_type()));

        // Check for ambiguous expressions that occur both at the current node and in its input tables. Example:
        //   SELECT COUNT(a) FROM (SELECT a, COUNT(a) FROM t GROUP BY a) t2
        // Our current expression system cannot handle this case and would consider the two COUNT(a) to be identical,
        // see #1902 for details. This check here might have false positives, feel free to improve the check or tackle
        // the underlying issue if this ever becomes an issue.
        auto table_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
        DebugAssert(_from_clause_result, "_from_clause_result should be set by now");
        table_expressions.reserve(_from_clause_result->elements_in_order.size());
        for (const auto& select_list_element : _from_clause_result->elements_in_order) {
          table_expressions.emplace_back(select_list_element.expression);
        }

        AssertInput(std::none_of(table_expressions.cbegin(), table_expressions.cend(),
                                 [&aggregate_expression](const auto input_expression) {
                                   return *input_expression == *aggregate_expression;
                                 }),
                    "Hyrise cannot handle repeated aggregate expressions, see #1902 for details.");

        return aggregate_expression;
      }

      /**
       * "Normal" function.
       */
      const auto function_iter = function_type_to_string.right.find(name);

      if (function_iter != function_type_to_string.right.end()) {
        AssertInput(!window_description, "Function " + name + " is not a window function.");
        auto arguments = std::vector<std::shared_ptr<AbstractExpression>>{};
        arguments.reserve(expr.exprList->size());

        for (const auto* hsql_argument : *expr.exprList) {
          arguments.emplace_back(_translate_hsql_expr(*hsql_argument, sql_identifier_resolver));
        }

        return std::make_shared<FunctionExpression>(function_iter->second, arguments);
      }

      FailInput("Couldn't resolve function '"s + name + "'");
    }

    case hsql::kExprOperator: {
      // Translate ArithmeticExpression
      const auto arithmetic_operators_iter = hsql_arithmetic_operators.find(expr.opType);
      if (arithmetic_operators_iter != hsql_arithmetic_operators.end()) {
        Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary expression.");
        const auto arithmetic_operator = arithmetic_operators_iter->second;

        // Handle intervals
        if (right->type == ExpressionType::Interval) {
          AssertInput(left->type == ExpressionType::Value && left->data_type() == DataType::String,
                      "Interval can only be applied to ValueExpression with String value");
          const auto start_date_string =
              std::string{boost::get<pmr_string>(static_cast<ValueExpression&>(*left).value)};
          const auto start_timestamp = string_to_timestamp(start_date_string);
          AssertInput(start_timestamp, "'" + start_date_string + "' is not a valid ISO 8601 extended date");
          const auto& interval_expression = static_cast<IntervalExpression&>(*right);
          // We already ensured to have either Addition or Substraction right at the beginning
          const auto duration = arithmetic_operator == ArithmeticOperator::Addition ? interval_expression.duration
                                                                                    : -interval_expression.duration;
          const auto end_date = date_interval(start_timestamp->date(), duration, interval_expression.unit);
          return value_(pmr_string{date_to_string(end_date)});
        }
        return std::make_shared<ArithmeticExpression>(arithmetic_operator, left, right);
      }

      // Translate PredicateExpression
      const auto predicate_condition_iter = hsql_predicate_condition.find(expr.opType);
      if (predicate_condition_iter != hsql_predicate_condition.end()) {
        const auto predicate_condition = predicate_condition_iter->second;

        if (is_binary_predicate_condition(predicate_condition)) {
          Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary_expression");
          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
        }

        if (predicate_condition == PredicateCondition::BetweenInclusive) {
          Assert(expr.exprList && expr.exprList->size() == 2, "Expected two arguments for BETWEEN");
          return between_inclusive_(left, _translate_hsql_expr(*(*expr.exprList)[0], sql_identifier_resolver),
                                    _translate_hsql_expr(*(*expr.exprList)[1], sql_identifier_resolver));
        }
      }

      // Translate other expression types that can be expected at this point
      switch (expr.opType) {
        case hsql::kOpUnaryMinus:
          return unary_minus_(left);
        case hsql::kOpCase:
          return _translate_hsql_case(expr, sql_identifier_resolver);
        case hsql::kOpOr:
          return or_(left, right);
        case hsql::kOpAnd:
          return and_(left, right);
        case hsql::kOpIn: {
          if (expr.select) {
            // `a IN (SELECT ...)`
            const auto subquery = _translate_hsql_subquery(*expr.select, sql_identifier_resolver);
            return in_(left, subquery);
          }

          // `a IN (x, y, z)`
          std::vector<std::shared_ptr<AbstractExpression>> arguments;

          AssertInput(expr.exprList && !expr.exprList->empty(), "IN clauses with an empty list are invalid");

          arguments.reserve(expr.exprList->size());
          for (const auto* hsql_argument : *expr.exprList) {
            arguments.emplace_back(_translate_hsql_expr(*hsql_argument, sql_identifier_resolver));
          }

          const auto array = std::make_shared<ListExpression>(arguments);
          return in_(left, array);
        }

        case hsql::kOpIsNull:
          return is_null_(left);

        case hsql::kOpNot:
          return inverse_predicate(*left);

        case hsql::kOpExists:
          AssertInput(expr.select, "Expected SELECT argument for EXISTS");
          return exists_(_translate_hsql_subquery(*expr.select, sql_identifier_resolver));

        default:
          Fail("Unexpected expression type");  // There are 19 of these, so we make an exception here and use default
      }
    }

    case hsql::kExprSelect:
      return _translate_hsql_subquery(*expr.select, sql_identifier_resolver);

    case hsql::kExprArray:
      FailInput("Can't translate a standalone array, arrays only valid in IN expressions");

    case hsql::kExprStar:
      Fail("Star expression should have been handled earlier");

    case hsql::kExprArrayIndex:
      FailInput("Array indexes are not yet supported");

    case hsql::kExprHint:
      FailInput("Hints are not yet supported");

    case hsql::kExprCast: {
      const auto source_data_type = left->data_type();
      const auto target_hsql_data_type = expr.columnType.data_type;

      if (target_hsql_data_type == hsql::DataType::DATE || target_hsql_data_type == hsql::DataType::DATETIME) {
        AssertInput(source_data_type == DataType::String,
                    "Cannot cast " + left->as_column_name() + " as " +
                        std::string{magic_enum::enum_name(target_hsql_data_type)});
        // We do not know if an expression to be casted other than a ValueExpression actually contains date time
        // values, and we cannot check this later due to the lack of proper data types.
        AssertInput(left->type == ExpressionType::Value, "Only ValueExpressions can be casted as " +
                                                             std::string{magic_enum::enum_name(target_hsql_data_type)});
        const auto input_string = std::string{boost::get<pmr_string>(static_cast<ValueExpression&>(*left).value)};

        if (target_hsql_data_type == hsql::DataType::DATE) {
          // We do not have a Date data type, so we check if the date is valid and return its ValueExpression.
          const auto timestamp = string_to_timestamp(input_string);
          AssertInput(timestamp && input_string.size() == 10,
                      "'" + input_string + "' is not a valid ISO 8601 extended date");
          return left;
        }

        if (target_hsql_data_type == hsql::DataType::DATETIME) {
          const auto date_time = string_to_timestamp(input_string);
          AssertInput(date_time, "'" + input_string + "' is not a valid ISO 8601 extended timestamp");
          // Parsing valid timestamps is also possible for at first glance invalid strings (see
          // utils/date_time_utils.hpp for details). To always obtain a semantically meaningful result, we retrieve the
          // created timestamp's string representation.
          return value_(pmr_string{timestamp_to_string(*date_time)});
        }
      }

      const auto data_type_iter = supported_hsql_data_types.find(expr.columnType.data_type);
      AssertInput(data_type_iter != supported_hsql_data_types.cend(),
                  "CAST as " + std::string{magic_enum::enum_name(expr.columnType.data_type)} + " is not supported");
      const auto target_data_type = data_type_iter->second;
      // Omit redundant casts
      if (source_data_type == target_data_type) {
        return left;
      }
      return cast_(left, target_data_type);
    }

    case hsql::kExprLiteralInterval: {
      const auto unit = hsql_datetime_field.at(expr.datetimeField);
      AssertInput(unit == DatetimeComponent::Day || unit == DatetimeComponent::Month || unit == DatetimeComponent::Year,
                  "Only date intervals are supported yet");
      return interval_(expr.ival, unit);
    }
  }
  Fail("Invalid enum value");
}

std::shared_ptr<LQPSubqueryExpression> SQLTranslator::_translate_hsql_subquery(
    const hsql::SelectStatement& select, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) {
  const auto sql_identifier_proxy = std::make_shared<SQLIdentifierResolverProxy>(
      sql_identifier_resolver, _parameter_id_allocator, _external_sql_identifier_resolver_proxy);

  auto subquery_translator =
      SQLTranslator{_use_mvcc, sql_identifier_proxy, _parameter_id_allocator, _with_descriptions, _meta_tables};
  const auto subquery_lqp = subquery_translator._translate_select_statement(select);
  const auto parameter_count = sql_identifier_proxy->accessed_expressions().size();

  // If this statement or any of the subquery's statements is not cacheable (because of meta tables),
  // this statement should not be cacheable.
  _cacheable &= subquery_translator._cacheable;

  auto parameter_ids = std::vector<ParameterID>{};
  parameter_ids.reserve(parameter_count);

  auto parameter_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  parameter_expressions.reserve(parameter_count);

  for (const auto& expression_and_parameter_id : sql_identifier_proxy->accessed_expressions()) {
    parameter_ids.emplace_back(expression_and_parameter_id.second);
    parameter_expressions.emplace_back(expression_and_parameter_id.first);
  }

  return std::make_shared<LQPSubqueryExpression>(subquery_lqp, parameter_ids, parameter_expressions);
}

std::shared_ptr<AbstractExpression> SQLTranslator::_translate_hsql_case(
    const hsql::Expr& expr, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) {
  /**
   * There is a "simple" and a "searched" CASE syntax, see http://www.oratable.com/simple-case-searched-case/
   * Hyrise supports both.
   */

  Assert(expr.exprList, "Unexpected SQLParserResult. Case needs exprList");
  Assert(!expr.exprList->empty(), "Unexpected SQLParserResult. Case needs non-empty exprList");

  // "a + b" in "CASE a + b WHEN ... THEN ... END", or nullptr when using the "searched" CASE syntax
  auto simple_case_left_operand = std::shared_ptr<AbstractExpression>{};
  if (expr.expr) {
    simple_case_left_operand = _translate_hsql_expr(*expr.expr, sql_identifier_resolver);
  }

  // Initialize CASE with the ELSE expression and then put the remaining WHEN...THEN... clauses on top of that
  // in reverse order
  auto current_case_expression = std::shared_ptr<AbstractExpression>{};
  if (expr.expr2) {
    current_case_expression = _translate_hsql_expr(*expr.expr2, sql_identifier_resolver);
  } else {
    // No ELSE specified, use NULL
    current_case_expression = null_();
  }

  for (auto case_reverse_idx = size_t{0}; case_reverse_idx < expr.exprList->size(); ++case_reverse_idx) {
    const auto case_idx = expr.exprList->size() - case_reverse_idx - 1;
    const auto* const case_clause = (*expr.exprList)[case_idx];

    auto when = _translate_hsql_expr(*case_clause->expr, sql_identifier_resolver);
    if (simple_case_left_operand) {
      when = equals_(simple_case_left_operand, when);
    }

    const auto then = _translate_hsql_expr(*case_clause->expr2, sql_identifier_resolver);
    current_case_expression = case_(when, then, current_case_expression);
  }

  return current_case_expression;
}

SQLTranslator::SelectListElement::SelectListElement(const std::shared_ptr<AbstractExpression>& init_expression)
    : expression(init_expression) {}

SQLTranslator::SelectListElement::SelectListElement(const std::shared_ptr<AbstractExpression>& init_expression,
                                                    const std::vector<SQLIdentifier>& init_identifiers)
    : expression(init_expression), identifiers(init_identifiers) {}

SQLTranslator::TableSourceState::TableSourceState(
    const std::shared_ptr<AbstractLQPNode>& init_lqp,
    const std::unordered_map<std::string, std::vector<SelectListElement>>& init_elements_by_table_name,
    const std::vector<SelectListElement>& init_elements_in_order,
    const std::shared_ptr<SQLIdentifierResolver>& init_sql_identifier_resolver)
    : lqp(init_lqp),
      elements_by_table_name(init_elements_by_table_name),
      elements_in_order(init_elements_in_order),
      sql_identifier_resolver(init_sql_identifier_resolver) {}

void SQLTranslator::TableSourceState::append(TableSourceState&& rhs) {
  for (auto& table_name_and_elements : rhs.elements_by_table_name) {
    const auto unique = !elements_by_table_name.contains(table_name_and_elements.first);
    AssertInput(unique, "Table Name '"s + table_name_and_elements.first + "' in FROM clause is not unique");
  }

  elements_by_table_name.merge(std::move(rhs.elements_by_table_name));
  elements_in_order.insert(elements_in_order.end(), rhs.elements_in_order.begin(), rhs.elements_in_order.end());
  sql_identifier_resolver->append(std::move(*rhs.sql_identifier_resolver));
}

}  // namespace hyrise
