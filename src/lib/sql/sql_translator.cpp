#include "sql_translator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/in_expression.hpp"
//#include "expression/not_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/value_placeholder_expression.hpp"
//#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
//#include "logical_query_plan/create_view_node.hpp"
//#include "logical_query_plan/delete_node.hpp"
//#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
//#include "logical_query_plan/insert_node.hpp"
//#include "logical_query_plan/join_node.hpp"
//#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
//#include "logical_query_plan/show_columns_node.hpp"
//#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
//#include "logical_query_plan/update_node.hpp"
//#include "logical_query_plan/validate_node.hpp"
//#include "sql/hsql_expr_translator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
//#include "types.hpp"
//#include "util/sqlhelper.h"
//#include "utils/assert.hpp"
#include "constant_mappings.hpp"

#include "SQLParser.h"

//namespace {
//
//using namespace opossum;  // NOLINT
//
//JoinMode translate_join_mode(const hsql::JoinType join_type) {
//  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
//  {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinFull, JoinMode::Outer},      {hsql::kJoinLeft, JoinMode::Left},
//  {hsql::kJoinRight, JoinMode::Right}, {hsql::kJoinNatural, JoinMode::Natural}, {hsql::kJoinCross, JoinMode::Cross},
//  };
//
//  auto it = join_type_to_mode.find(join_type);
//  DebugAssert(it != join_type_to_mode.end(), "Unable to handle join type.");
//  return it->second;
//}
//
//}  // namespace

namespace {

using namespace opossum; // NOLINT

const std::unordered_map<hsql::OperatorType, ArithmeticOperator> hsql_arithmetic_operators = {
{hsql::kOpPlus, ArithmeticOperator::Addition},
{hsql::kOpMinus, ArithmeticOperator::Subtraction},
{hsql::kOpAsterisk, ArithmeticOperator::Multiplication},
{hsql::kOpSlash, ArithmeticOperator::Division},
{hsql::kOpPercentage, ArithmeticOperator::Modulo},
{hsql::kOpCaret, ArithmeticOperator::Power},
};

const std::unordered_map<hsql::OperatorType, LogicalOperator> hsql_logical_operators = {
{hsql::kOpAnd, LogicalOperator::And},
{hsql::kOpOr, LogicalOperator::Or}
};

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
{hsql::kOpIsNull, PredicateCondition::IsNull}
};
} // namespace

namespace opossum {

SQLTranslator::SQLTranslator(const UseMvcc use_mvcc, const std::shared_ptr<SQLIdentifierContextProxy>& external_sql_identifier_context_proxy):
  _use_mvcc(use_mvcc), _external_sql_identifier_context_proxy(external_sql_identifier_context_proxy) {}

std::shared_ptr<SQLIdentifierContext> SQLTranslator::sql_identifier_context() const {
  return _sql_identifier_context;
}

std::vector<std::shared_ptr<AbstractLQPNode>> SQLTranslator::translate_sql(const std::string& sql) {
  hsql::SQLParserResult parser_result;
  hsql::SQLParser::parse(sql, &parser_result);

  Assert(parser_result.isValid(), "Invalid SQL");
  Assert(parser_result.size() > 0, "Cannot create empty SQLPipeline.");

  return translate_parser_result(parser_result);
}

std::vector<std::shared_ptr<AbstractLQPNode>> SQLTranslator::translate_parser_result(
const hsql::SQLParserResult &result) {
  std::vector<std::shared_ptr<AbstractLQPNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::translate_statement(const hsql::SQLStatement &statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return translate_select_statement(static_cast<const hsql::SelectStatement &>(statement));
//    case hsql::kStmtInsert:
//      return _translate_insert(static_cast<const hsql::InsertStatement&>(statement));
//    case hsql::kStmtDelete:
//      return _translate_delete(static_cast<const hsql::DeleteStatement&>(statement));
//    case hsql::kStmtUpdate:
//      return _translate_update(static_cast<const hsql::UpdateStatement&>(statement));
//    case hsql::kStmtShow:
//      return _translate_show(static_cast<const hsql::ShowStatement&>(statement));
//    case hsql::kStmtCreate:
//      return _translate_create(static_cast<const hsql::CreateStatement&>(statement));
//    case hsql::kStmtDrop:
//      return _translate_drop(static_cast<const hsql::DropStatement&>(statement));
    default:
      Fail("SQL statement type not supported");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::translate_select_statement(const hsql::SelectStatement &select) {
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

  _sql_identifier_context = std::make_shared<SQLIdentifierContext>();

  if (select.fromTable) {
    _current_lqp = _translate_table_ref(*select.fromTable);
  } else {
    _current_lqp = std::make_shared<DummyTableNode>();
  }

  if (select.whereClause != nullptr) {
    const auto where_expression = _translate_hsql_expr(*select.whereClause);
    _current_lqp = _translate_predicate_expression(where_expression, _current_lqp);
  }

  _translate_select_list_groupby_having(select);

  if (select.order != nullptr) _translate_order_by(*select.order);


//  if (select.limit != nullptr) {
//    current_lqp = LimitNode::make(select.limit->limit, current_lqp, translation_state);
//  }

  return _current_lqp;
}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_insert(const hsql::InsertStatement& insert) {
//  const auto table_name = std::string{insert.tableName};
//  auto target_table = StorageManager::get().get_table(table_name);
//  auto target_table_node = std::make_shared<StoredTableNode>(table_name);
//
//  Assert(target_table, "INSERT: Invalid table name");
//
//  // Plan that generates the data to insert
//  auto insert_data_node = std::shared_ptr<AbstractLQPNode>{};
//
//  // Check for `INSERT ... INTO newtable FROM oldtable WHERE condition` query
//  if (insert.type == hsql::kInsertSelect) {
//    Assert(insert.select, "INSERT INTO ... SELECT ...: No SELECT statement given");
//
//    insert_data_node = translate_select_statement(*insert.select);
//  } else {
//    Assert(insert.values, "INSERT INTO ... VALUES: No values given");
//
//    std::vector<std::shared_ptr<AbstractExpression>> value_expressions(insert.values->size());
//    for (const auto* value : *insert.values) {
//      value_expressions.emplace_back(_translate_hsql_expr(*value, nullptr, _validate));
//    }
//
//    insert_data_node = ProjectionNode::make(value_expressions, DummyTableNode::make());
//  }
//
//  if (insert.columns) {
//    Assert(insert.columns->size() == insert_data_node->output_column_count(), "INSERT: Target column count and number of input columns mismatch");
//
//    // Certain columns have been specified. In this case we create a new expression list
//    // for the Projection, so that it contains as many columns as the target table.
//
//    // pre-fill new projection list with NULLs
//    std::vector<std::shared_ptr<AbstractExpression>> expressions(target_table->column_count(),
//                                                            std::make_shared<ValueExpression>(NullValue{}));
//
//    ColumnID source_column_idx{0};
//    for (const auto& column_name : *insert.columns) {
//      // retrieve correct ColumnID from the target table
//      const auto target_column_id = target_table->column_id_by_name(column_name);
//      expressions[target_column_id] = insert_data_node->output_column_expressions()[source_column_idx];
//      ++source_column_idx;
//    }
//
//    // create projection and add to the node chain
//    insert_data_node = ProjectionNode::make(expressions, insert_data_node);
//  }
//
//  Assert(insert_data_node->output_column_count() == target_table->column_count(), "INSERT: Column count mismatch");
//  // DataType checking has to be done at runtime, as Query could still contains Placeholder with unspecified type
//
//  return InsertNode::make(table_name, insert_data_node);
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_delete(const hsql::DeleteStatement& delete_) {
//  auto data_to_delete_node = StoredTableNode::make(delete_.tableName);
//  data_to_delete_node = _validate_if_active(data_to_delete_node);
//
//  auto translation_state = std::make_shared<SQLTranslationState>();
//  translation_state->add_columns(data_to_delete_node->output_column_expressions(), data_to_delete_node->output_column_names());
//
//  if (delete_.expr) {
//    const auto delete_where_expression = _translate_hsql_expr(*delete_.expr, translation_state, _validate);
//    data_to_delete_node = _translate_predicate_expression(delete_where_expression, data_to_delete_node);
//  }
//
//  return DeleteNode::make(delete_.tableName, data_to_delete_node);
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_update(const hsql::UpdateStatement& update) {
//  auto translation_state = std::make_shared<SQLTranslationState>();
//
//  std::shared_ptr<AbstractLQPNode> target_references_node = _translate_table_ref(*update.table, translation_state);
//  if (update.where) {
//    const auto where_expression = _translate_hsql_expr(*update.where, translation_state, _validate);
//    target_references_node = _translate_predicate_expression(where_expression, {target_references_node});
//  }
//
//  // The update operator wants ReferenceColumns on its left side
//  // TODO(anyone): fix this
//  Assert(!std::dynamic_pointer_cast<StoredTableNode>(target_references_node),
//         "Unconditional updates are currently not supported");
//
//  auto update_expressions = target_references_node->output_column_expressions();
//
//  for (const auto* update_clause : *update.updates) {
//    const auto column_reference = target_references_node->get_column(update_clause->column);
//    const auto column_expression = std::make_shared<LQPColumnExpression>(column_expression);
//    const auto column_id = target_references_node->get_output_column_id(column_expression);
//
//    auto value_expression = _translate_hsql_expr(*update_clause->value, translation_state, _validate);
//    update_expressions[column_id] = value_expression;
//  }
//
//  return UpdateNode::make((update.table)->name, update_expressions, target_references_node);
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_join(const hsql::JoinDefinition& join, SQLTranslationState& translation_state) {
//  const auto join_mode = translate_join_mode(join.type);
//
//  if (join_mode == JoinMode::Natural) {
//    return _translate_natural_join(join, translation_state);
//  }
//
//  auto left_input =_translate_table_ref(*join.left, translation_state);
//  auto right_input = _translate_table_ref(*join.right, translation_state);
//
//  const hsql::Expr& condition = *join.condition;
//
//  Assert(condition.type == hsql::kExprOperator, "Join condition must be operator.");
//
//  const auto join_condition_expression = _translate_hsql_expr(condition, translation_state, _validate);
//  Assert(join_condition_expression->type == ExpressionType::Predicate, "Join condition must be predicate");
//
//  const auto join_condition_predicate_expression =
//    std::static_pointer_cast<AbstractPredicateExpression>(join_condition_expression);
//  // The Join operators only support simple comparisons for now.
//  switch (join_condition_predicate_expression->predicate_condition) {
//    case PredicateCondition::Equals: case PredicateCondition::NotEquals: case PredicateCondition::LessThan:
//    case PredicateCondition::LessThanEquals:  case PredicateCondition::GreaterThan:
//    case PredicateCondition::GreaterThanEquals:
//      break;
//    default:
//      Fail("Join condition must be a simple comparison operator.");
//  }
//
//  const auto left_operand_expression = _translate_hsql_expr(*condition.expr, translation_state, _validate);
//  const auto right_operand_expression = _translate_hsql_expr(*condition.expr2, translation_state, _validate);
//
//  const auto join_expressions = std::make_pair(left_operand_expression, right_operand_expression);
//
//  return JoinNode::make(join_mode,
//                        join_expressions,
//                        join_condition_predicate_expression->predicate_condition,
//                        left_input,
//                        right_input);
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_natural_join(const hsql::JoinDefinition& join, SQLTranslationState& translation_state) {
//  DebugAssert(translate_join_mode(join.type) == JoinMode::Natural, "join must be a natural join");
//
//  const auto left_node = _translate_table_ref(*join.left);
//  const auto right_node = _translate_table_ref(*join.right);
//
//  // we need copies that we can sort on.
//  auto left_column_names = left_node->output_column_names();
//  auto right_column_names = right_node->output_column_names();
//
//  std::sort(left_column_names.begin(), left_column_names.end());
//  std::sort(right_column_names.begin(), right_column_names.end());
//
//  std::vector<std::string> join_column_names;
//  std::set_intersection(left_column_names.begin(), left_column_names.end(), right_column_names.begin(),
//                        right_column_names.end(), std::back_inserter(join_column_names));
//
//  Assert(!join_column_names.empty(), "No matching columns for natural join found");
//
//  auto current_node = JoinNode::make(JoinMode::Cross, left_node, right_node);
//
//  for (const auto& join_column_name : join_column_names) {
//    const auto left_column_reference = left_node->get_column({join_column_name});
//    const auto right_column_reference = right_node->get_column({join_column_name});
//
//    current_node = PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
//    PredicateCondition::Equals,
//    std::make_shared<LQPColumnExpression>(left_column_reference),
//    std::make_shared<LQPColumnExpression>(right_column_reference)
//    ), current_node);
//  }
//
//  // We need to collect the column origins so that we can remove the duplicate columns used in the join condition
//  std::vector<std::shared_ptr<AbstractExpression>> column_expressions;
//  for (auto column_id = ColumnID{0u}; column_id < current_node->output_column_count(); ++column_id) {
//    const auto& column_name = current_node->output_column_names()[column_id];
//
//    if (static_cast<size_t>(column_id) >= left_node->output_column_count() &&
//        std::find(join_column_names.begin(), join_column_names.end(), column_name) != join_column_names.end()) {
//      continue;
//    }
//
//    column_expressions.emplace_back(current_node->output_column_expressions()[column_id]);
//  }
//
//  return ProjectionNode::make(column_expressions, current_node);
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_cross_product(const std::vector<hsql::TableRef*>& tables) {
//  DebugAssert(!tables.empty(), "Cannot translate cross product without tables");
//  auto product = _translate_table_ref(*tables.front());
//
//  for (auto table_idx = size_t{0}; table_idx < tables.size(); ++table_idx) {
//    const auto node = _translate_table_ref(*tables[table_idx]);
//    product = JoinNode::make(JoinMode::Cross, product, node);
//  }
//
//  return product;
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_column_renamings(
//const std::shared_ptr<AbstractLQPNode> &node,
//const hsql::TableRef &table) {
//
//  // Check this here instead of on the caller side
//  if (!table.alias || !table.alias->columns) {
//    return node;
//  }
//
//  // Add a new projection node for table alias with column alias declarations
//  // e.g. select * from foo as bar(a, b)
//
//  Assert(table.type == hsql::kTableName || table.type == hsql::kTableSelect,
//         "Aliases are only applicable to table names and subselects");
//
//  // To stick to the sql standard there must be an alias for every column of the renamed table
//  // https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt 6.3
//  Assert(table.alias->columns->size() == node->output_column_count(),
//         "The number of column aliases must match the number of columns");
//
//  const auto& column_expressions = node->output_column_expressions();
//  std::vector<PlanColumnDefinition> column_definitions;
//  column_definitions.reserve(column_expressions.size());
//
//  auto column_idx = size_t{0};
//  for (const auto* alias : *(table.alias->columns)) {
//    column_definitions.emplace_back(column_expressions[column_idx], alias);
//    ++column_idx;
//  }
//
//  return ProjectionNode::make(column_definitions, node);
//}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_table_ref(const hsql::TableRef& hsql_table_ref) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // Each element in the FROM list needs to have a unique table name
  auto table_name = std::string{};

  switch (hsql_table_ref.type) {
    case hsql::kTableName:
    case hsql::kTableSelect: {
      if (hsql_table_ref.type == hsql::kTableName) {
        if (StorageManager::get().has_table(hsql_table_ref.name)) {
          const auto stored_table_node = StoredTableNode::make(hsql_table_ref.name);
          lqp = _validate_if_active(stored_table_node);

          const auto table = StorageManager::get().get_table(hsql_table_ref.name);

          for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
            const auto& column_definition = table->column_definitions()[column_id];
            const auto column_reference = LQPColumnReference{stored_table_node, column_id};
            const auto column_expression = std::make_shared<LQPColumnExpression>(column_reference);
            _sql_identifier_context->set_column_name(column_expression, column_definition.name);
            _sql_identifier_context->set_table_name(column_expression, hsql_table_ref.name);
          }

        } else if (StorageManager::get().has_view(hsql_table_ref.name)) {
          Fail("Views not supported yet");
          lqp = StorageManager::get().get_view(hsql_table_ref.name);
       //   Assert(!_validate || lqp->subplan_is_validated(), "Trying to add non-validated view to validated query");
        } else {
          Fail(std::string("Did not find a table or view with name ") + hsql_table_ref.name);
        }
        table_name = hsql_table_ref.name;
      } else if (hsql_table_ref.type == hsql::kTableSelect) {
        Assert(hsql_table_ref.alias && hsql_table_ref.alias->name, "Every SubSelect must have its own alias");
        table_name = hsql_table_ref.alias->name;

        SQLTranslator sub_select_translator{_use_mvcc};
        lqp = sub_select_translator.translate_select_statement(*hsql_table_ref.select);

        for (const auto& sub_select_expression : lqp->output_column_expressions()) {
          const auto identifier = sub_select_translator.sql_identifier_context()->get_expression_identifier(sub_select_expression);

          if (identifier) {
            _sql_identifier_context->set_column_name(sub_select_expression, identifier->column_name);
          }
          _sql_identifier_context->set_table_name(sub_select_expression, hsql_table_ref.alias->name);
        }
      } else {
        Fail("Unsupported TableRef");
      }
    } break;

//    case hsql::kTableJoin:
//      lqp = _translate_join(*hsql_table_ref.join, translation_state);
//      break;
//    case hsql::kTableCrossProduct:
//      lqp = _translate_cross_product(*hsql_table_ref.list, translation_state);
//      break;

    default:
      Fail("Unable to translate source table.");
  }

  const auto table_name_is_unique = _from_elements_by_table_name.emplace(table_name, lqp).second;
  Assert(table_name_is_unique, "Table name '" + table_name + "' in FROM is not unique");

  return lqp;
}

void SQLTranslator::_translate_select_list_groupby_having(
const hsql::SelectStatement &select) {
  const auto& input_expressions = _current_lqp->output_column_expressions();

  auto pre_aggregate_expression_set = ExpressionUnorderedSet{input_expressions.begin(), input_expressions.end()};
  auto pre_aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{input_expressions.begin(), input_expressions.end()};
  auto aggregate_expression_set = ExpressionUnorderedSet{};
  auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  // Visitor that identifies aggregates and their arguments
  const auto find_aggregates_and_arguments = [&](auto& sub_expression) {
    if (sub_expression->type != ExpressionType::Aggregate) return true;

    /**
     * If the AggregateExpression has already been computed in a previous node (consider "x" in
     * "SELECT x FROM (SELECT MIN(a) as x FROM t) AS y)", it doesn't count as a new Aggregate and is therefore not
     * considered a "Aggregate" in the current SELECT list. Handling this as a special case seems hacky to me as well,
     * but it's the best solution I can come up with right now.
     */
    if (_current_lqp->find_column_id(*sub_expression)) return false;

    auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(sub_expression);
    if (aggregate_expression_set.emplace(aggregate_expression).second) {
      aggregate_expressions.emplace_back(aggregate_expression);
      for (const auto& argument : aggregate_expression->arguments) {
        if (argument->requires_calculation()) {
          if (pre_aggregate_expression_set.emplace(argument).second) {
            pre_aggregate_expressions.emplace_back(argument);
          }
        }
      }
    }

    return false;
  };

  // Identify all Aggregates and their arguments needed for SELECT and build the select_list_elements
  // Each select_list_element is either an Expression or nullptr if the element is a Wildcard
  std::vector<std::shared_ptr<AbstractExpression>> select_list_elements;
  auto post_select_sql_identifier_context = std::make_shared<SQLIdentifierContext>(*_sql_identifier_context);
  for (const auto& hsql_select_expr : *select.selectList) {
    if (hsql_select_expr->type == hsql::kExprStar) {
      select_list_elements.emplace_back(nullptr);
    } else {
      auto expression = _translate_hsql_expr(*hsql_select_expr);
      visit_expression(expression, find_aggregates_and_arguments);
      select_list_elements.emplace_back(expression);

      if (hsql_select_expr->alias) {
        post_select_sql_identifier_context->set_column_name(expression, hsql_select_expr->alias);
      }
    }
  }
  _sql_identifier_context = post_select_sql_identifier_context;

  // Identify all GROUP BY expressions
  auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  if (select.groupBy && select.groupBy->columns) {
    group_by_expressions.reserve(select.groupBy->columns->size());
    for (const auto* group_by_hsql_expr : *select.groupBy->columns) {
      const auto group_by_expression = _translate_hsql_expr(*group_by_hsql_expr);
      group_by_expressions.emplace_back(group_by_expression);
      if (pre_aggregate_expression_set.emplace(group_by_expression).second) {
        pre_aggregate_expressions.emplace_back(group_by_expression);
      }
    }
  }

  // Gather all aggregates and arguments from HAVING
  auto having_expression = std::shared_ptr<AbstractExpression>{};
  if (select.groupBy && select.groupBy->having) {
    having_expression = _translate_hsql_expr(*select.groupBy->having);
    visit_expression(having_expression, find_aggregates_and_arguments);
  }

  // Build pre_aggregate_projection
  if (pre_aggregate_expressions.size() != _current_lqp->output_column_expressions().size()) {
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
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions;
  std::unordered_map<std::shared_ptr<AbstractExpression>, std::string> column_aliases;

  for (auto select_list_idx = size_t{0}; select_list_idx < select.selectList->size(); ++select_list_idx) {
    const auto* hsql_expr = (*select.selectList)[select_list_idx];

    if (hsql_expr->type == hsql::kExprStar) {
      Assert(!_from_elements_by_table_name.empty(), "Can't SELECT with wildcards if since there are no FROM tables specified");

      if (hsql_expr->table) {
        if (is_aggregate) {
          // Select all GROUP BY columns with the specified table name
          for (const auto& group_by_expression : group_by_expressions) {
            const auto identifier = _sql_identifier_context->get_expression_identifier(group_by_expression);
            if (identifier && identifier->table_name == hsql_expr->table) {
              output_expressions.emplace_back(group_by_expression);
            }
          }
        } else {
          // Select all columns from the FROM element with the specified name
          const auto from_element_iter = _from_elements_by_table_name.find(hsql_expr->table);
          Assert(from_element_iter != _from_elements_by_table_name.end(), std::string("No such element in FROM with table name '") + hsql_expr->table + "'");

          for (const auto& expression : from_element_iter->second->output_column_expressions()) {
            output_expressions.emplace_back(expression);
          }
        }
      } else {
        if (is_aggregate) {
          // Select all GROUP BY columns
          output_expressions.insert(output_expressions.end(), group_by_expressions.begin(), group_by_expressions.end());
        } else {
          // Select all columns from the FROM elements
          for (const auto& table_name_and_from_element : _from_elements_by_table_name) {
            const auto& from_element_expressions = table_name_and_from_element.second->output_column_expressions();
            output_expressions.insert(output_expressions.end(), from_element_expressions.begin(), from_element_expressions.end());
          }
        }
      }
    } else {
      auto output_expression = select_list_elements[select_list_idx];
      output_expressions.emplace_back(output_expression);

      if (hsql_expr->alias) {
        _sql_identifier_context->set_column_name(output_expression, hsql_expr->alias);
      }
    }
  }

  // Only add a ProjectionNode if necessary
  if (!expressions_equal(_current_lqp->output_column_expressions(), output_expressions)) {
    _current_lqp = ProjectionNode::make(output_expressions, _current_lqp);
  }

  // Check whether we need to create an AliasNode - this is the case whenever a Expression was assigned a column_name
  // that is not its generated name.
  const auto need_alias_node = std::any_of(output_expressions.begin(), output_expressions.end(), [&](const auto& expression) {
    const auto identifier = _sql_identifier_context->get_expression_identifier(expression);
    return identifier && identifier->column_name != expression->as_column_name();
  }) ;

  if (need_alias_node) {
    std::vector<std::string> aliases;
    for (const auto& output_expression : output_expressions) {
      const auto identifier = _sql_identifier_context->get_expression_identifier(output_expression);
      if (identifier) {
        aliases.emplace_back(identifier->column_name);
      } else {
        aliases.emplace_back(output_expression->as_column_name());
      }
    }

    _current_lqp = AliasNode::make(output_expressions, aliases, _current_lqp);
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
    expressions[expression_idx] = _translate_hsql_expr(*order_description->expr);
    order_by_modes[expression_idx] = order_type_to_order_by_mode.at(order_description->type);
  }

  _current_lqp =  _add_expressions_if_unavailable(_current_lqp, expressions);

  _current_lqp = SortNode::make(expressions, order_by_modes, _current_lqp);

  // If any Expressions were added to perform the sorting, remove them again
  if (input_lqp->output_column_expressions().size() != _current_lqp->output_column_expressions().size()) {
    _current_lqp = ProjectionNode::make(input_lqp->output_column_expressions(), _current_lqp);
  }
}

//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_show(const hsql::ShowStatement& show_statement) {
//  switch (show_statement.type) {
//    case hsql::ShowType::kShowTables:
//      return ShowTablesNode::make();
//    case hsql::ShowType::kShowColumns:
//      return ShowColumnsNode::make(std::string(show_statement.name));
//    default:
//      Fail("hsql::ShowType is not supported.");
//  }
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create(const hsql::CreateStatement& create_statement) {
//  switch (create_statement.type) {
//    case hsql::CreateType::kCreateView: {
//      auto view = translate_select_statement((const hsql::SelectStatement &) *create_statement.select);
//
//      if (create_statement.viewColumns) {
//        // The CREATE VIEW statement has renamed the columns: CREATE VIEW myview (foo, bar) AS SELECT ...
//        Assert(create_statement.viewColumns->size() == view->output_column_count(),
//               "Number of Columns in CREATE VIEW does not match SELECT statement");
//
//        // Create a list of renamed column expressions
//        std::vector<std::shared_ptr<LQPExpression>> projections;
//        ColumnID column_id{0};
//        for (const auto& alias : *create_statement.viewColumns) {
//          const auto column_reference = view->output_column_references()[column_id];
//          // rename columns so they match the view definition
//          projections.push_back(LQPExpression::create_column(column_reference, alias));
//          ++column_id;
//        }
//
//        // Create a projection node for this renaming
//        auto projection_node = ProjectionNode::make(projections);
//        projection_node->set_left_input(view);
//        view = projection_node;
//      }
//
//      return std::make_shared<CreateViewNode>(create_statement.tableName, view);
//    }
//    default:
//      Fail("hsql::CreateType is not supported.");
//  }
//}
//
//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_drop(const hsql::DropStatement& drop_statement) {
//  switch (drop_statement.type) {
//    case hsql::DropType::kDropView: {
//      return DropViewNode::make(drop_statement.name);
//    }
//    default:
//      Fail("hsql::DropType is not supported.");
//  }
//}
//
std::shared_ptr<AbstractLQPNode> SQLTranslator::_validate_if_active(
    const std::shared_ptr<AbstractLQPNode>& input_node) {
//  if (!_validate) return input_node;
//
//  auto validate_node = ValidateNode::make();
//  validate_node->set_left_input(input_node);
  return input_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_predicate_expression(const std::shared_ptr<AbstractExpression> &expression,
                                                                                std::shared_ptr<AbstractLQPNode> current_node) const {
  /**
   * Translate AbstractPredicateExpression
   */
  if (expression->type == ExpressionType::Predicate) {
    current_node = _add_expressions_if_unavailable(current_node, expression->arguments);
    return PredicateNode::make(expression, current_node);
  }

  /**
   * Translate LogicalExpression
   */
  if (expression->type == ExpressionType::Logical) {
    const auto logical_expression = std::static_pointer_cast<LogicalExpression>(expression);

    switch (logical_expression->logical_operator) {
      case LogicalOperator::And: {
        current_node = _translate_predicate_expression(logical_expression->right_operand(), current_node);
        return _translate_predicate_expression(logical_expression->left_operand(), current_node);
      }
      case LogicalOperator::Or: {
        const auto input_expressions = current_node->output_column_expressions();

        const auto left_input = _translate_predicate_expression(logical_expression->left_operand(), current_node);
        const auto right_input = _translate_predicate_expression(logical_expression->right_operand(), current_node);

        // For Union to work we need to eliminate all potential temporary columns added in the branches of the Union
        // E.g. "a+b" in "a+b > 5 OR a < 3"
        return UnionNode::make(UnionMode::Positions,
                               _prune_expressions(left_input, input_expressions),
                               _prune_expressions(right_input, input_expressions));
      }
    }
  }

  /**
   * Translate NotExpression
   */
  if (expression->type == ExpressionType::Not) {
    // Fallback implementation for NOT, for now
    current_node = _add_expressions_if_unavailable(current_node, {expression});
    return PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
    PredicateCondition::NotEquals, expression, std::make_shared<ValueExpression>(0)), current_node);
  }

  Fail("Non-predicate Expression used as Predicate");
}


std::shared_ptr<AbstractLQPNode> SQLTranslator::_prune_expressions(const std::shared_ptr<AbstractLQPNode>& node,
                                                            const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  if (expressions_equal(node->output_column_expressions(), expressions)) return node;
  return ProjectionNode::make(expressions, node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_add_expressions_if_unavailable(const std::shared_ptr<AbstractLQPNode>& node,
                                                 const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  std::vector<std::shared_ptr<AbstractExpression>> projection_expressions;

  for (const auto& expression : expressions) {
    // The required expression is already available or doesn't need to be computed (e.g. when it is a literal)
    if (!expression->requires_calculation() || node->find_column_id(*expression)) continue;
    projection_expressions.emplace_back(expression);
  }

  // All requested expressions are available, no need to create a projection
  if (projection_expressions.empty()) return node;

  projection_expressions.insert(projection_expressions.end(),
                                node->output_column_expressions().begin(),
                                node->output_column_expressions().end());

  return ProjectionNode::make(projection_expressions, node);
}

std::shared_ptr<AbstractExpression> SQLTranslator::_translate_hsql_expr(const hsql::Expr& expr) const {
  auto name = expr.name != nullptr ? std::string(expr.name) : "";

  std::shared_ptr<AbstractExpression> left;
  std::shared_ptr<AbstractExpression> right;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;

  // TODO fix case parsing
  if (!(expr.type == hsql::kExprOperator && expr.opType == hsql::kOpCase)) {
    if (expr.exprList) {
      arguments.reserve(expr.exprList->size());
      for (const auto *hsql_argument : *(expr.exprList)) {
        arguments.emplace_back(_translate_hsql_expr(*hsql_argument));
      }
    }
  }

  if (expr.expr) left = _translate_hsql_expr(*expr.expr);
  if (expr.expr2) right = _translate_hsql_expr(*expr.expr2);

  switch (expr.type) {
    case hsql::kExprColumnRef: {
      const auto table_name = expr.table ? std::optional<std::string>(std::string(expr.table)) : std::nullopt;
      const auto identifier = SQLIdentifier{name, table_name};

      auto expression = _sql_identifier_context->resolve_identifier_relaxed(identifier);
      if (!expression && _external_sql_identifier_context_proxy) {
        expression = _external_sql_identifier_context_proxy->resolve_identifier_relaxed(identifier);
      }
      Assert(expression, "Couldn't resolve identifier '" + identifier.as_string() + "'");

      return expression;
    }

    case hsql::kExprLiteralFloat:
      return std::make_shared<ValueExpression>(expr.fval);

    case hsql::kExprLiteralString:
      Assert(expr.name, "No value given for string literal");
      return std::make_shared<ValueExpression>(name);

    case hsql::kExprLiteralInt:
      if (static_cast<int32_t>(expr.ival) == expr.ival) {
        return std::make_shared<ValueExpression>(static_cast<int32_t>(expr.ival));
      } else {
        return std::make_shared<ValueExpression>(expr.ival);
      }

    case hsql::kExprLiteralNull:
      return std::make_shared<ValueExpression>(NullValue{});

    case hsql::kExprParameter:
      return std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{static_cast<uint16_t>(expr.ival)});

    case hsql::kExprFunctionRef: {
      Assert(expr.exprList, "FunctionRef has no exprList. Bug in sqlparser?");

      // convert to upper-case to find mapping
      std::transform(name.begin(), name.end(), name.begin(), [](const auto c) { return std::toupper(c); });

      const auto aggregate_iter = aggregate_function_to_string.right.find(name);
      if (aggregate_iter != aggregate_function_to_string.right.end()) {
        auto aggregate_function = aggregate_iter->second;

        if (aggregate_function == AggregateFunction::Count && expr.distinct) {
          aggregate_function = AggregateFunction::CountDistinct;
        }

        switch (aggregate_function) {
          case AggregateFunction::Min: case AggregateFunction::Max: case AggregateFunction::Sum:
          case AggregateFunction::Avg:
            Assert(arguments.size() == 1, "Expected exactly one argument for this AggregateFunction");
            return std::make_shared<AggregateExpression>(aggregate_function, arguments[0]);

          case AggregateFunction::Count: case AggregateFunction::CountDistinct:
            return std::make_shared<AggregateExpression>(aggregate_function);
        }

      } else {
//        const auto function_iter = function_type_to_string.right.find(name);

//        if (function_iter != function_type_to_string.right.end()) {
//          return std::make_shared<FunctionExpression>(function_iter->second, arguments);
//        } else {
        Fail(std::string{"Couldn't resolve function '"} + name + "'");
//        }
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

        if (is_ordering_predicate_condition(predicate_condition)) {
          Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary_expression");
          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
        } else if (predicate_condition == PredicateCondition::Between) {
          Assert(arguments.size() == 2, "Expected two arguments for BETWEEN");
          return std::make_shared<BetweenExpression>(left, arguments[0], arguments[1]);
        }
      }

      switch (expr.opType) {
        case hsql::kOpCase: return _translate_hsql_case(expr);
        case hsql::kOpOr: return std::make_shared<LogicalExpression>(LogicalOperator::Or, left, right);
        case hsql::kOpAnd: return std::make_shared<LogicalExpression>(LogicalOperator::And, left, right);
        case hsql::kOpIn: {
          const auto array = std::make_shared<ArrayExpression>(arguments);
          return std::make_shared<InExpression>(left, array);
        }

        default:
          Fail("Not handling this OperatorType yet");
      }

//      const auto predicate_iter = hsql_predicate_condition.find(expr.opType);
//      if (predicate_iter != hsql_predicate_condition.end()) {
//        const auto predicate_condition = predicate_iter->second;

//        if (predicate_condition == PredicateCondition::Between) {
//          Assert(left && !right, "Illegal arguments for BETWEEN. Bug in sqlparser?");
//          Assert(expr.exprList && (*expr.exprList)[0] && (*expr.exprList)[1], "Illegal arguments for BETWEEN. Bug in sqlparser?");

//          const auto lower_bound = _translate_hsql_expr(*(*expr.exprList)[0], translation_state);
//          const auto upper_bound = _translate_hsql_expr(*(*expr.exprList)[1], translation_state);

//          return std::make_shared<BetweenExpression>(left, lower_bound, upper_bound);
//        } else if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
//          Assert(left && !right, "Illegal arguments for IsNull/IsNotNull. Bug in sqlparser?");
//          return std::make_shared<IsNullExpression>(predicate_condition, left);
//        } else {
//          Assert(left && right, "Illegal arguments for binary predicate. Bug in sqlparser?");
//          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
//        }
//      }

//      const auto logical_iter = hsql_logical_operators.find(expr.opType);
//      if (logical_iter != hsql_logical_operators.end()) {
//        Assert(left && right, "Wrong number of arguments. Bug in sqlparser?");
//        return std::make_shared<LogicalExpression>(logical_iter->second, left, right);
//      }

//      if (expr.opType == hsql::kOpNot){
//        Assert(left && !right, "Wrong number of arguments. Bug in sqlparser?");
//        return std::make_shared<NotExpression>(left);
//      }

//      Fail("Unsupported expression type");
    }

    case hsql::kExprSelect: {
      const auto sql_identifier_proxy = std::make_shared<SQLIdentifierContextProxy>(_sql_identifier_context,
                                                                                            _external_sql_identifier_context_proxy);

      auto sub_select_translator = SQLTranslator{_use_mvcc, sql_identifier_proxy};
      const auto sub_select_lqp = sub_select_translator.translate_select_statement(*expr.select);

      return std::make_shared<LQPSelectExpression>(sub_select_lqp, sql_identifier_proxy->accessed_expressions());
    }

    case hsql::kExprArray:
      Fail("Nyi");
      //return std::make_shared<ArrayExpression>(arguments);

    case hsql::kExprHint:
    case hsql::kExprStar:
    case hsql::kExprArrayIndex:
      Fail("Can't translate this hsql expression into a Hyrise expression");

    default:
      Fail("Nyie");
  }
}

std::shared_ptr<AbstractExpression> SQLTranslator::_translate_hsql_case(const hsql::Expr& expr) const {
  /**
   * There is a "simple" and a "searched" CASE syntax, see http://www.oratable.com/simple-case-searched-case/
   * Hyrise supports both.
   */

  Assert(expr.exprList, "Unexpected SQLParserResult. Case needs exprList");
  Assert(!expr.exprList->empty(), "Unexpected SQLParserResult. Case needs non-empty exprList");

  // "a + b" in "CASE a + b WHEN ... THEN ... END", or nullptr when using the "searched" CASE syntax
  auto simple_case_left_operand = std::shared_ptr<AbstractExpression>{};
  if (expr.expr) simple_case_left_operand = _translate_hsql_expr(*expr.expr);

  // Initialize CASE with the ELSE expression and then put the remaining WHEN...THEN... clauses on top of that
  // in reverse order
  auto current_case_expression = std::shared_ptr<AbstractExpression>{};
  if (expr.expr2) {
    current_case_expression = _translate_hsql_expr(*expr.expr2);
  } else {
    // No ELSE specified, use NULL
    current_case_expression = std::make_shared<ValueExpression>(NullValue{});
  }

  for (auto case_reverse_idx = size_t{0}; case_reverse_idx < expr.exprList->size(); ++case_reverse_idx) {
    const auto case_idx = expr.exprList->size() - case_reverse_idx - 1;
    const auto case_clause = (*expr.exprList)[case_idx];

    auto when = _translate_hsql_expr(*case_clause->expr);
    if (simple_case_left_operand) {
      when = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, simple_case_left_operand, when);
    }

    const auto then = _translate_hsql_expr(*case_clause->expr2);
    current_case_expression = std::make_shared<CaseExpression>(when, then, current_case_expression);
  }

  return current_case_expression;
}

}  // namespace opossum
