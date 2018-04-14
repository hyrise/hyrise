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
#include "expression/aggregate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
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

std::vector<std::shared_ptr<AbstractLQPNode>> SQLTranslator::translate_parser_result(
const hsql::SQLParserResult &result) {
  std::vector<std::shared_ptr<AbstractLQPNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = translate_parser_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::translate_parser_statement(const hsql::SQLStatement &statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return translate_select_statement(static_cast<const hsql::SelectStatement &>(statement));
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
  const auto table_name = std::string{insert.tableName};
  auto target_table = StorageManager::get().get_table(table_name);
  auto target_table_node = std::make_shared<StoredTableNode>(table_name);

  Assert(target_table, "INSERT: Invalid table name");

  // Plan that generates the data to insert
  auto insert_data_node = std::shared_ptr<AbstractLQPNode>{};

  // Check for `INSERT ... INTO newtable FROM oldtable WHERE condition` query
  if (insert.type == hsql::kInsertSelect) {
    Assert(insert.select, "INSERT INTO ... SELECT ...: No SELECT statement given");

    insert_data_node = translate_select_statement(*insert.select);
  } else {
    Assert(insert.values, "INSERT INTO ... VALUES: No values given");

    std::vector<std::shared_ptr<AbstractExpression>> value_expressions(insert.values->size());
    for (const auto* value : *insert.values) {
      value_expressions.emplace_back(translate_hsql_expr(*value, {}));
    }

    insert_data_node = ProjectionNode::make(value_expressions, DummyTableNode::make());
  }

  if (insert.columns) {
    Assert(insert.columns->size() == insert_data_node->output_column_count(), "INSERT: Target column count and number of input columns mismatch");

    // Certain columns have been specified. In this case we create a new expression list
    // for the Projection, so that it contains as many columns as the target table.

    // pre-fill new projection list with NULLs
    std::vector<std::shared_ptr<AbstractExpression>> expressions(target_table->column_count(),
                                                            std::make_shared<ValueExpression>(NullValue{}));

    ColumnID source_column_idx{0};
    for (const auto& column_name : *insert.columns) {
      // retrieve correct ColumnID from the target table
      const auto target_column_id = target_table->column_id_by_name(column_name);
      expressions[target_column_id] = insert_data_node->output_column_expressions()[source_column_idx];
      ++source_column_idx;
    }

    // create projection and add to the node chain
    insert_data_node = ProjectionNode::make(expressions, insert_data_node);
  }

  Assert(insert_data_node->output_column_count() == target_table->column_count(), "INSERT: Column count mismatch");
  // DataType checking has to be done at runtime, as Query could still contains Placeholder with unspecified type

  return InsertNode::make(table_name, insert_data_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_delete(const hsql::DeleteStatement& delete_) {
  auto data_to_delete_node = StoredTableNode::make(delete_.tableName);
  data_to_delete_node = _validate_if_active(data_to_delete_node);
  if (delete_.expr) {
    const auto delete_where_expression = translate_hsql_expr(*delete_.expr, {data_to_delete_node});
    data_to_delete_node = _translate_predicate_expression(delete_where_expression, data_to_delete_node);
  }

  return DeleteNode::make(delete_.tableName, data_to_delete_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_update(const hsql::UpdateStatement& update) {
  std::shared_ptr<AbstractLQPNode> target_references_node = _translate_table_ref(*update.table);
  if (update.where) {
    const auto where_expression = translate_hsql_expr(*update.where, {target_references_node});
    target_references_node = _translate_predicate_expression(where_expression, {target_references_node});
  }

  // The update operator wants ReferenceColumns on its left side
  // TODO(anyone): fix this
  Assert(!std::dynamic_pointer_cast<StoredTableNode>(target_references_node),
         "Unconditional updates are currently not supported");

  auto update_expressions = target_references_node->output_column_expressions();

  for (const auto* update_clause : *update.updates) {
    const auto column_reference = target_references_node->get_column(QualifiedColumnName{update_clause->column});
    const auto column_id = target_references_node->get_output_column_id(column_reference);

    auto value_expression = translate_hsql_expr(*update_clause->value, {target_references_node});
    update_expressions[column_id] = value_expression;
  }

  return UpdateNode::make((update.table)->name, update_expressions, target_references_node);
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

  auto translation_state = _translate_table_ref(*select.fromTable);

  if (select.whereClause != nullptr) {
    const auto where_expression = translate_hsql_expr(*select.whereClause, translation_state.qualified_column_name_lookup);
    _translate_predicate_expression(where_expression, translation_state);
  }

  _translate_select_list_groupby_having(select, translation_state);

  if (select.order != nullptr) {
    _translate_order_by(*select.order, translation_state);
  }

  if (select.limit != nullptr) {
    LimitNode::make(select.limit->limit, translation_state);
  }

  return translation_state.lqp;
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

  const auto join_condition_expression = translate_hsql_expr(condition, {left_node, right_node});
  Assert(join_condition_expression->type == ExpressionType::Predicate, "Join condition must be predicate");

  const auto join_condition_predicate_expression =
    std::static_pointer_cast<AbstractPredicateExpression>(join_condition_expression);
  // The Join operators only support simple comparisons for now.
  switch (join_condition_predicate_expression->predicate_condition) {
    case PredicateCondition::Equals: case PredicateCondition::NotEquals: case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:  case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      break;
    default:
      Fail("Join condition must be a simple comparison operator.");
  }

  const auto left_operand_expression = translate_hsql_expr(*condition.expr, {left_node, right_node});
  const auto right_operand_expression = translate_hsql_expr(*condition.expr2, {left_node, right_node});

  /**
   * `x_in_y_node` indicates whether the column identifier on the `x` side in the join expression is in the input node
   * on
   * the `y` side of the join. So in the query
   * `SELECT * FROM T1 JOIN T2 on person_id == customer_id`
   * We have to check whether `person_id` belongs to T1 (left_in_left_node == true) or to T2
   * (left_in_right_node == true). Later we make sure that one and only one of them is true, otherwise we either have
   * ambiguity or the column is simply not existing.
   */
  const auto left_in_left_node = left_node->find_column(*left_operand_expression);
  const auto left_in_right_node = right_node->find_column(*left_operand_expression);
  const auto right_in_left_node = left_node->find_column(*right_operand_expression);
  const auto right_in_right_node = right_node->find_column(*right_operand_expression);

  Assert(static_cast<bool>(left_in_left_node) ^ static_cast<bool>(left_in_right_node),
         std::string("Left operand ") + left_operand_expression->as_column_name() +
             " must be in exactly one of the input nodes");
  Assert(static_cast<bool>(right_in_left_node) ^ static_cast<bool>(right_in_right_node),
         std::string("Right operand ") + right_operand_expression->as_column_name() +
             " must be in exactly one of the input nodes");

  if (left_in_right_node) {
    std::swap(left_node, right_node);
  }

  const auto join_expressions = std::make_pair(left_operand_expression, right_operand_expression);

  return JoinNode::make(join_mode,
                        join_expressions,
                        join_condition_predicate_expression->predicate_condition,
                        left_node,
                        right_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_natural_join(const hsql::JoinDefinition& join) {
  DebugAssert(translate_join_mode(join.type) == JoinMode::Natural, "join must be a natural join");

  const auto left_node = _translate_table_ref(*join.left);
  const auto right_node = _translate_table_ref(*join.right);

  // we need copies that we can sort on.
  auto left_column_names = left_node->output_column_names();
  auto right_column_names = right_node->output_column_names();

  std::sort(left_column_names.begin(), left_column_names.end());
  std::sort(right_column_names.begin(), right_column_names.end());

  std::vector<std::string> join_column_names;
  std::set_intersection(left_column_names.begin(), left_column_names.end(), right_column_names.begin(),
                        right_column_names.end(), std::back_inserter(join_column_names));

  Assert(!join_column_names.empty(), "No matching columns for natural join found");

  auto current_node = JoinNode::make(JoinMode::Cross, left_node, right_node);

  for (const auto& join_column_name : join_column_names) {
    const auto left_column_reference = left_node->get_column({join_column_name});
    const auto right_column_reference = right_node->get_column({join_column_name});

    current_node = PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
    PredicateCondition::Equals,
    std::make_shared<LQPColumnExpression>(left_column_reference),
    std::make_shared<LQPColumnExpression>(right_column_reference)
    ), current_node);
  }

  // We need to collect the column origins so that we can remove the duplicate columns used in the join condition
  std::vector<std::shared_ptr<AbstractExpression>> column_expressions;
  for (auto column_id = ColumnID{0u}; column_id < current_node->output_column_count(); ++column_id) {
    const auto& column_name = current_node->output_column_names()[column_id];

    if (static_cast<size_t>(column_id) >= left_node->output_column_count() &&
        std::find(join_column_names.begin(), join_column_names.end(), column_name) != join_column_names.end()) {
      continue;
    }

    column_expressions.emplace_back(current_node->output_column_expressions()[column_id]);
  }

  return ProjectionNode::make(column_expressions, current_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_cross_product(const std::vector<hsql::TableRef*>& tables) {
  DebugAssert(!tables.empty(), "Cannot translate cross product without tables");
  auto product = _translate_table_ref(*tables.front());

  for (auto table_idx = size_t{0}; table_idx < tables.size(); ++table_idx) {
    const auto node = _translate_table_ref(*tables[table_idx]);
    product = JoinNode::make(JoinMode::Cross, product, node);
  }

  return product;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_column_renamings(
const std::shared_ptr<AbstractLQPNode> &node,
const hsql::TableRef &table) {

  // Check this here instead of on the caller side
  if (!table.alias || !table.alias->columns) {
    return node;
  }

  // Add a new projection node for table alias with column alias declarations
  // e.g. select * from foo as bar(a, b)

  Assert(table.type == hsql::kTableName || table.type == hsql::kTableSelect,
         "Aliases are only applicable to table names and subselects");

  // To stick to the sql standard there must be an alias for every column of the renamed table
  // https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt 6.3
  Assert(table.alias->columns->size() == node->output_column_count(),
         "The number of column aliases must match the number of columns");

  const auto& column_expressions = node->output_column_expressions();
  std::vector<PlanColumnDefinition> column_definitions;
  column_definitions.reserve(column_expressions.size());

  auto column_idx = size_t{0};
  for (const auto* alias : *(table.alias->columns)) {
    column_definitions.emplace_back(column_expressions[column_idx], alias);
    ++column_idx;
  }

  return ProjectionNode::make(column_definitions, node);
}

SQLTranslationState SQLTranslator::_translate_table_ref(const hsql::TableRef& hsql_table_ref) {
  const auto alias = hsql_table_ref.alias ? std::optional<std::string>(hsql_table_ref.alias->name) : std::nullopt;

  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  switch (hsql_table_ref.type) {
    case hsql::kTableName:
    case hsql::kTableSelect: {

      if (hsql_table_ref.type == hsql::kTableName) {
        if (StorageManager::get().has_table(hsql_table_ref.name)) {
          lqp = StoredTableNode::make(hsql_table_ref.name);
          lqp = _validate_if_active(lqp);

        } else if (StorageManager::get().has_view(hsql_table_ref.name)) {
          lqp = StorageManager::get().get_view(hsql_table_ref.name);
          Assert(!_validate || lqp->subplan_is_validated(), "Trying to add non-validated view to validated query");
        } else {
          Fail(std::string("Did not find a table or view with name ") + hsql_table_ref.name);
        }
      } else  { // hsql::kTableSelect
        Assert(hsql_table_ref.alias, "Every derived table must have its own alias");
        lqp = translate_select_statement(*hsql_table_ref.select);
      }

    } break;

    case hsql::kTableJoin:
      lqp = _translate_join(*hsql_table_ref.join);
      break;
    case hsql::kTableCrossProduct:
      lqp = _translate_cross_product(*hsql_table_ref.list);
      break;

    default:
      Fail("Unable to translate source table.");
  }

  auto translation_state = SQLTranslationState::from_leaf(lqp);

  if (hsql_table_ref.alias) {
    if (hsql_table_ref.alias->columns) {
      translation_state.set_column_names(*hsql_table_ref.alias->columns);
    }
    if (hsql_table_ref.alias->name) {
      translation_state.set_table_name(hsql_table_ref.alias->name);
    }
  }

  return translation_state;
}

void SQLTranslator::_translate_select_list_groupby_having(
const hsql::SelectStatement &select, SQLTranslationState &translation_state) {
  const auto input_node = translation_state.lqp;

  const auto input_expressions = translation_state.lqp->output_column_expressions();

  auto pre_aggregate_expressions = std::unordered_set<std::shared_ptr<AbstractExpression>, ExpressionHash, ExpressionEquals>{
  input_expressions.begin(), input_expressions.end()
  };

  auto aggregate_expressions = std::unordered_set<std::shared_ptr<AbstractExpression>, ExpressionHash, ExpressionEquals>{
  };

  // Add all expressions required for GROUP BY
  auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  if (select.groupBy && select.groupBy->columns) {
    pre_aggregate_expressions.reserve(pre_aggregate_expressions.size() + select.groupBy->columns->size());
    for (const auto* group_by_hsql_expr : *select.groupBy->columns) {
      const auto group_by_expression = translate_hsql_expr(*group_by_hsql_expr, translation_state.qualified_column_name_lookup);
      group_by_expressions.emplace_back(group_by_expression);
      pre_aggregate_expressions.insert(group_by_expression);
    }
  }

  // Visitor that identifies aggregates and their arguments
  const auto find_aggregates_and_arguments = [&](auto& sub_expression) {
    if (sub_expression.type != ExpressionType::Aggregate) return true;
    auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(sub_expression);
    aggregate_expressions.insert(aggregate_expression);

    for (const auto& argument : aggregate_expression->arguments) {
      if (argument->requires_calculation()) {
        pre_aggregate_expressions.insert(argument);
      }
    }
    return false;
  };

  // Gather all aggregates and arguments from having
  auto having_expression = std::shared_ptr<AbstractExpression>{};
  if (select.groupBy && select.groupBy->having) {
    having_expression = translate_hsql_expr(*select.groupBy->having, translation_state.qualified_column_name_lookup);
    visit_expression(having_expression, find_aggregates_and_arguments);
  }

  // Identify all aggregates and arguments needed for SELECT and build the select_list_elements
  std::vector<std::shared_ptr<AbstractExpression>> select_list_elements;
  for (const auto& hsql_select_expr : *select.selectList) {
    if (hsql_select_expr->type == hsql::kExprStar) {
      select_list_elements.emplace_back(nullptr);
    } else {
      auto expression = translate_hsql_expr(*hsql_select_expr, translation_state.qualified_column_name_lookup);
      visit_expression(expression, find_aggregates_and_arguments);
      select_list_elements.emplace_back(expression);
    }
  }

  // Build pre_aggregate_projection
  translation_state.lqp = ProjectionNode::make(std::vector<std::shared_ptr<AbstractExpression>>(
  pre_aggregate_expressions.begin(), pre_aggregate_expressions.end()
  ), translation_state.lqp);


  // Build Aggregate
  if (!aggregate_expressions.empty() || !group_by_expressions.empty()) {
    translation_state.lqp = AggregateNode::make(group_by_expressions, aggregate_expressions, translation_state.lqp);
  }

  // Build having
  if (having_expression) {
    translation_state.lqp = _translate_predicate_expression(having_expression, translation_state.lqp);
  }

  // Create output_expressions from SELECT list, including column wildcards
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions;
  std::unordered_map<std::shared_ptr<AbstractExpression>, std::string> column_aliases;

  auto output_column_id = ColumnID{0};

  for (auto select_list_idx = size_t{0}; select_list_idx < select.selectList->size(); ++select_list_idx) {
    const auto* hsql_expr = (*select.selectList)[select_list_idx];

    if (hsql_expr->type == hsql::kExprStar) {
      if (hsql_expr->table) {
        const auto expressions = translation_state.find_output_columns_with_table_name(*hsql_expr->table);
        output_expressions.reserve(output_expressions.size() + expressions.size());
        for (const auto& expression : expressions) {
          output_expressions.emplace_back(expression);
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
          const auto& input_expressions = input_node->output_column_expressions();
          output_expressions.reserve(output_expressions.size() + input_expressions.size());
          for (const auto& input_expression : input_expressions) {
            output_expressions.emplace_back(input_expression);
          }
        }
      }
    }

    auto output_expression = select_list_elements[select_list_idx];
    output_expressions.emplace_back(output_expression);

    if (hsql_expr->alias) {
      translation_state.qualified_column_name_lookup->set_column_name(output_expression, hsql_expr->alias);
    }
  }

  translation_state.lqp = ProjectionNode::make(output_expressions, translation_state.lqp);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_order_by(
    const std::vector<hsql::OrderDescription*>& order_list, std::shared_ptr<AbstractLQPNode> current_node) {
  if (order_list.empty()) {
    return current_node;
  }

  std::vector<OrderByDefinition> order_by_definitions;
  order_by_definitions.reserve(order_list.size());
  for (const auto& order_description : order_list) {
    const auto order_by_expression = translate_hsql_expr(*order_description->expr, {current_node});
    const auto order_by_mode = order_type_to_order_by_mode.at(order_description->type);
    order_by_definitions.emplace_back(order_by_expression, order_by_mode);

    if (!current_node->find_column(*order_by_expression) && order_by_expression->requires_calculation()) {
      current_node = _add_expression_if_unavailable(current_node, order_by_expression);
    }
  }

  auto sort_node = SortNode::make(order_by_definitions, current_node);

  return sort_node;
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
      auto view = translate_select_statement((const hsql::SelectStatement &) *create_statement.select);

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
    for (const auto &argument : expression->arguments) {
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
    current_node = _add_expression_if_unavailable(current_node, expression);
    return PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
    PredicateCondition::NotEquals, expression, std::make_shared<ValueExpression>(0)));
  }

  Assert(expression->type != ExpressionType::Aggregate, "Aggregate used in illegal location");
  return _add_expression_if_unavailable(current_node, expression);
}


std::shared_ptr<AbstractLQPNode> SQLTranslator::_prune_expressions(const std::shared_ptr<AbstractLQPNode>& node,
                                                            const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  if (expressions_equal(node->output_column_expressions(), expressions)) return node;
  return ProjectionNode::make(expressions, node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_add_expression_if_unavailable(const std::shared_ptr<AbstractLQPNode>& node,
                                                 const std::shared_ptr<AbstractExpression>& expression) const {
  // The required expression is already available or doesn't need to be computed (e.g. when it is a literal)
  if (!expression->requires_calculation() || node->find_column(expression)) return node;

  auto expressions = node->output_column_expressions();
  expressions.emplace_back(expression);
  return ProjectionNode::make(expressions, node);
}

}  // namespace opossum
