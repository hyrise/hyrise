#include "sql_translator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

//#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/is_null_expression.hpp"
//#include "expression/not_expression.hpp"
#include "expression/value_expression.hpp"
//#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
//#include "logical_query_plan/create_view_node.hpp"
//#include "logical_query_plan/delete_node.hpp"
//#include "logical_query_plan/drop_view_node.hpp"
//#include "logical_query_plan/dummy_table_node.hpp"
//#include "logical_query_plan/insert_node.hpp"
//#include "logical_query_plan/join_node.hpp"
//#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
//#include "logical_query_plan/show_columns_node.hpp"
//#include "logical_query_plan/show_tables_node.hpp"
//#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
//#include "logical_query_plan/update_node.hpp"
//#include "logical_query_plan/validate_node.hpp"
//#include "sql/hsql_expr_translator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
//#include "types.hpp"
#include "translate_hsql_expr.hpp"
//#include "util/sqlhelper.h"
//#include "utils/assert.hpp"

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

namespace opossum {

SQLTranslator::SQLTranslator(const UseMvcc use_mvcc):
  _use_mvcc(use_mvcc) {}

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

  _current_lqp = _translate_table_ref(*select.fromTable);

  if (select.whereClause != nullptr) {
    const auto where_expression = translate_hsql_expr(*select.whereClause, _sql_identifier_context, _use_mvcc);
    _current_lqp = _translate_predicate_expression(where_expression, _current_lqp);
  }

  _translate_select_list_groupby_having(select);

//  if (select.order != nullptr) {
//    current_lqp = _translate_order_by(*select.order, current_lqp, translation_state);
//  }

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
//      value_expressions.emplace_back(translate_hsql_expr(*value, nullptr, _validate));
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
//    const auto delete_where_expression = translate_hsql_expr(*delete_.expr, translation_state, _validate);
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
//    const auto where_expression = translate_hsql_expr(*update.where, translation_state, _validate);
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
//    auto value_expression = translate_hsql_expr(*update_clause->value, translation_state, _validate);
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
//  const auto join_condition_expression = translate_hsql_expr(condition, translation_state, _validate);
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
//  const auto left_operand_expression = translate_hsql_expr(*condition.expr, translation_state, _validate);
//  const auto right_operand_expression = translate_hsql_expr(*condition.expr2, translation_state, _validate);
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
      } else  { // hsql::kTableSelect
        Assert(hsql_table_ref.alias, "Every derived table must have its own alias");

        // Subselect in FROM can't refer to external names, thus we're not passing in a ExternalColumnIdentifierProxy
        lqp = translate_select_statement(*hsql_table_ref.select);
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
//
//  if (hsql_table_ref.alias) {
//    if (hsql_table_ref.alias->columns) {
//      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
//        translation_state.set_column_names(lqp->output_column_expressions(), *hsql_table_ref.alias->columns);
//      }
//    }
//    if (hsql_table_ref.alias->name) {
//      translation_state.set_table_name(lqp->output_column_expressions(), hsql_table_ref.alias->name);
//    }
//  }

  return lqp;
}

void SQLTranslator::_translate_select_list_groupby_having(
const hsql::SelectStatement &select) {
//  const auto input_node = translation_state.lqp;
//
  const auto& input_expressions = _current_lqp->output_column_expressions();

  auto pre_aggregate_expression_set = ExpressionUnorderedSet{input_expressions.begin(), input_expressions.end()};
  auto pre_aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{input_expressions.begin(), input_expressions.end()};
  auto aggregate_expression_set = ExpressionUnorderedSet{};
  auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  // Add all expressions required for GROUP BY
  auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  if (select.groupBy && select.groupBy->columns) {
    group_by_expressions.reserve(select.groupBy->columns->size());
    for (const auto* group_by_hsql_expr : *select.groupBy->columns) {
      const auto group_by_expression = translate_hsql_expr(*group_by_hsql_expr, _sql_identifier_context, _use_mvcc);
      group_by_expressions.emplace_back(group_by_expression);
      if (pre_aggregate_expression_set.emplace(group_by_expression).second) {
        pre_aggregate_expressions.emplace_back(group_by_expression);
      }

      if (group_by_hsql_expr->alias) {
        _sql_identifier_context->set_column_name(group_by_expression, group_by_hsql_expr->alias);
      }
    }
  }

  // Visitor that identifies aggregates and their arguments
  const auto find_aggregates_and_arguments = [&](auto& sub_expression) {
    if (sub_expression->type != ExpressionType::Aggregate) return true;
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

  // Gather all aggregates and arguments from having
  auto having_expression = std::shared_ptr<AbstractExpression>{};
  if (select.groupBy && select.groupBy->having) {
    having_expression = translate_hsql_expr(*select.groupBy->having, _sql_identifier_context, _use_mvcc);
    visit_expression(having_expression, find_aggregates_and_arguments);
  }

  // Identify all aggregates and arguments needed for SELECT and build the select_list_elements
  std::vector<std::shared_ptr<AbstractExpression>> select_list_elements;
  for (const auto& hsql_select_expr : *select.selectList) {
    if (hsql_select_expr->type == hsql::kExprStar) {
      select_list_elements.emplace_back(nullptr);
    } else {
      auto expression = translate_hsql_expr(*hsql_select_expr, _sql_identifier_context, _use_mvcc);
      visit_expression(expression, find_aggregates_and_arguments);
      select_list_elements.emplace_back(expression);
    }
  }

  // Build pre_aggregate_projection
  if (pre_aggregate_expressions.size() != _current_lqp->output_column_expressions().size()) {
    _current_lqp = ProjectionNode::make(pre_aggregate_expressions, _current_lqp);
  }

  // Build Aggregate
  if (!aggregate_expressions.empty() || !group_by_expressions.empty()) {
    _current_lqp = AggregateNode::make(group_by_expressions, aggregate_expressions, _current_lqp);
  }

  // Build Having
  if (having_expression) {
    _current_lqp = _translate_predicate_expression(having_expression, _current_lqp);
  }

  // Create output_expressions from SELECT list, including column wildcards
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions;
  std::unordered_map<std::shared_ptr<AbstractExpression>, std::string> column_aliases;

  auto output_column_id = ColumnID{0};

  for (auto select_list_idx = size_t{0}; select_list_idx < select.selectList->size(); ++select_list_idx) {
    const auto* hsql_expr = (*select.selectList)[select_list_idx];

    if (hsql_expr->type == hsql::kExprStar) {
      if (hsql_expr->table) {
        const auto expressions = _sql_identifier_context->resolve_table_name(hsql_expr->table);
        for (const auto& expression : expressions) {
          // If GROUP BY is present, only select GROUP BY expressions that have the specified table name.
          // Otherwise, select all still available expressions with that name
          if (select.groupBy) {
            const auto group_by_expression_iter = std::find_if(group_by_expressions.begin(), group_by_expressions.end(),
            [&](const auto& group_by_expression) { return group_by_expression->deep_equals(*expression); });

            if (group_by_expression_iter != group_by_expressions.end()) {
              output_expressions.emplace_back(expression);
            }
          } else {
            const auto input_expression_iter = std::find_if(input_expressions.begin(), input_expressions.end(),
                                                               [&](const auto& input_expression) { return input_expression->deep_equals(*expression); });

            if (input_expression_iter != input_expressions.end()) {
              output_expressions.emplace_back(expression);
            }
          }
        }
      } else {
        // When GROUP BY is present, a * in the SELECT list refers to all GROUP BY expressions.
        // When no GROUP BY is present, all input expressions are selected.
        if (select.groupBy) {
          output_expressions.reserve(output_expressions.size() + group_by_expressions.size());
          for (const auto& group_by_expression : group_by_expressions) {
            output_expressions.emplace_back(group_by_expression);
          }
        } else {
          output_expressions.reserve(output_expressions.size() + input_expressions.size());
          for (const auto& input_expression : input_expressions) {
            output_expressions.emplace_back(input_expression);
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

  _current_lqp = ProjectionNode::make(output_expressions, _current_lqp);

  // Check whether we need to create an AliasNode
  const auto need_alias_node = std::any_of(select.selectList->begin(), select.selectList->end(), [](const auto * hsql_expr) {
    return hsql_expr->alias != nullptr;
  });

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

//std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_order_by(
//    const std::vector<hsql::OrderDescription*>& order_list, std::shared_ptr<AbstractLQPNode> current_node) {
//  if (order_list.empty()) {
//    return current_node;
//  }
//
//  std::vector<OrderByDefinition> order_by_definitions;
//  order_by_definitions.reserve(order_list.size());
//  for (const auto& order_description : order_list) {
//    const auto order_by_expression = translate_hsql_expr(*order_description->expr, {current_node});
//    const auto order_by_mode = order_type_to_order_by_mode.at(order_description->type);
//    order_by_definitions.emplace_back(order_by_expression, order_by_mode);
//
//    if (!current_node->find_column(*order_by_expression) && order_by_expression->requires_calculation()) {
//      current_node = _add_expression_if_unavailable(current_node, order_by_expression);
//    }
//  }
//
//  auto sort_node = SortNode::make(order_by_definitions, current_node);
//
//  return sort_node;
//}
//
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

}  // namespace opossum
