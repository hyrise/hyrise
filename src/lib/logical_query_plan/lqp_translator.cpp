#include "lqp_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "aggregate_node.hpp"
//#include "constant_mappings.hpp"
//#include "create_view_node.hpp"
//#include "delete_node.hpp"
//#include "drop_view_node.hpp"
//#include "dummy_table_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/value_placeholder_expression.hpp"
//#include "insert_node.hpp"
//#include "join_node.hpp"
//#include "limit_node.hpp"
#include "operators/aggregate.hpp"
//#include "operators/delete.hpp"
#include "operators/get_table.hpp"
//#include "operators/index_scan.hpp"
//#include "operators/insert.hpp"
//#include "operators/join_hash.hpp"
//#include "operators/join_sort_merge.hpp"
//#include "operators/limit.hpp"
//#include "operators/maintenance/create_view.hpp"
//#include "operators/maintenance/drop_view.hpp"
//#include "operators/maintenance/show_columns.hpp"
//#include "operators/maintenance/show_tables.hpp"
//#include "operators/pqp_expression.hpp"
//#include "operators/product.hpp"
#include "operators/alias_operator.hpp"
#include "operators/projection.hpp"
//#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
//#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
//#include "operators/update.hpp"
//#include "operators/validate.hpp"
#include "alias_node.hpp"
#include "predicate_node.hpp"
#include "projection_node.hpp"
//#include "show_columns_node.hpp"
//#include "sort_node.hpp"
//#include "storage/storage_manager.hpp"
#include "stored_table_node.hpp"
#include "union_node.hpp"
//#include "update_node.hpp"
//#include "utils/performance_warning.hpp"
//#include "validate_node.hpp"

namespace opossum {

std::shared_ptr<AbstractOperator> LQPTranslator::translate_node(const std::shared_ptr<AbstractLQPNode>& node) const {
  /**
   * Translate a node (i.e. call `_translate_by_node_type`) only if it hasn't been translated before, otherwise just
   * retrieve it from cache
   *
   * Without this caching, translating this kind of LQP
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *  \                /
   *   \__predicate_c_/
   *          |
   *     table_int_float2
   *
   * would result in multiple operators created from predicate_c and thus in performance drops
   */

  const auto operator_iter = _operator_by_lqp_node.find(node);
  if (operator_iter != _operator_by_lqp_node.end()) {
    return operator_iter->second;
  }

  const auto pqp = _translate_by_node_type(node->type, node);
  _operator_by_lqp_node.emplace(node, pqp);
  return pqp;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_by_node_type(
LQPNodeType type, const std::shared_ptr<AbstractLQPNode>& node) const {
  switch (type) {
    case LQPNodeType::Alias:        return _translate_alias_node(node);
    case LQPNodeType::StoredTable:
      return _translate_stored_table_node(node);
    case LQPNodeType::Predicate:
      return _translate_predicate_node(node);
    case LQPNodeType::Projection:
      return _translate_projection_node(node);
//    case LQPNodeType::Sort:
//      return _translate_sort_node(node);
//    case LQPNodeType::Join:
//      return _translate_join_node(node);
    case LQPNodeType::Aggregate:
      return _translate_aggregate_node(node);
//    case LQPNodeType::Limit:
//      return _translate_limit_node(node);
//    case LQPNodeType::Insert:
//      return _translate_insert_node(node);
//    case LQPNodeType::Delete:
//      return _translate_delete_node(node);
//    case LQPNodeType::DummyTable:
//      return _translate_dummy_table_node(node);
//    case LQPNodeType::Update:
//      return _translate_update_node(node);
//    case LQPNodeType::Validate:
//      return _translate_validate_node(node);
    case LQPNodeType::Union:
      return _translate_union_node(node);

//      // Maintenance operators
//    case LQPNodeType::ShowTables:
//      return _translate_show_tables_node(node);
//    case LQPNodeType::ShowColumns:
//      return _translate_show_columns_node(node);
//    case LQPNodeType::CreateView:
//      return _translate_create_view_node(node);
//    case LQPNodeType::DropView:
//      return _translate_drop_view_node(node);

    default:
      Fail("Unknown node type encountered.");
  }
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_stored_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
  const auto get_table = std::make_shared<GetTable>(stored_table_node->table_name);
  get_table->set_excluded_chunk_ids(stored_table_node->excluded_chunk_ids());
  return get_table;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_node = node->left_input();
  const auto input_operator = translate_node(input_node);
  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

  Assert(predicate_node->predicate->type == ExpressionType::Predicate, "Only PredicateExpressions can be translated to Table/IndexScans");

  if (const auto binary_predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate); binary_predicate_expression) {
    return _translate_predicate(*input_node,
                                input_operator,
                                *binary_predicate_expression->left_operand(),
                                binary_predicate_expression->predicate_condition,
                                *binary_predicate_expression->right_operand());

  } else if (const auto between_expression = std::dynamic_pointer_cast<BetweenExpression>(predicate_node->predicate); between_expression) {
    const auto lower_bound_op = _translate_predicate(*input_node,
                                                     input_operator,
                                                     *between_expression->value(),
                                                     PredicateCondition::GreaterThanEquals,
                                                     *between_expression->lower_bound());
    const auto upper_bound_op = _translate_predicate(*input_node,
                                                     lower_bound_op,
                                                     *between_expression->value(),
                                                     PredicateCondition::LessThanEquals,
                                                     *between_expression->upper_bound());
    return upper_bound_op;

  }

  Fail("Unsupported predicate type");
//  if (auto left_expression = predicate_node->predicate
//
//  const auto column_id = predicate_node->get_output_column_id(predicate_node->column_reference());
//
//  auto value = predicate_node->value();
//  if (is_lqp_column_reference(value)) {
//    value = predicate_node->get_output_column_id(boost::get<const LQPColumnReference>(value));
//  }
//
//  if (predicate_node->scan_type() == ScanType::IndexScan) {
//    return _translate_predicate_node_to_index_scan(predicate_node, value, column_id, input_operator);
//  }
//
//  /**
//   * The TableScan Operator doesn't support BETWEEN, so for `X BETWEEN a AND b` we create two TableScans: One for
//   * `X >= a` and one for `X <= b`
//   */
//  if (predicate_node->predicate_condition() == PredicateCondition::Between) {
//    DebugAssert(static_cast<bool>(predicate_node->value2()), "Predicate condition BETWEEN requires a second value");
//    PerformanceWarning("TableScan executes BETWEEN as two separate scans");
//
//    auto table_scan_gt =
//        std::make_shared<TableScan>(input_operator, column_id, PredicateCondition::GreaterThanEquals, value);
//    return std::make_shared<TableScan>(table_scan_gt, column_id, PredicateCondition::LessThanEquals,
//                                       *predicate_node->value2());
//  }
//
//  return std::make_shared<TableScan>(input_operator, column_id, predicate_node->predicate_condition(), value);
}

//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node_to_index_scan(
//    const std::shared_ptr<PredicateNode>& predicate_node, const AllParameterVariant& value, const ColumnID column_id,
//    const std::shared_ptr<AbstractOperator> input_operator) const {
//  DebugAssert(
//      is_variant(value),
//      "We do not support IndexScan on two-column predicates. Fail! The optimizer should have dealt with the problem.");
//
//  // Currently, we will only use IndexScans if the predicate node directly follows a StoredTableNode.
//  // Our IndexScan implementation does not work on reference columns yet.
//  DebugAssert(predicate_node->left_input()->type() == LQPNodeType::StoredTable,
//              "IndexScan must follow a StoredTableNode.");
//
//  const auto value_variant = boost::get<AllTypeVariant>(value);
//
//  // This is necessary because we currently support single column indexes only
//  const std::vector<ColumnID> column_ids = {column_id};
//  const std::vector<AllTypeVariant> right_values = {value_variant};
//  std::vector<AllTypeVariant> right_values2 = {};
//  if (predicate_node->value2()) right_values2.emplace_back(*predicate_node->value2());
//
//  auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(predicate_node->left_input());
//  const auto table_name = stored_table_node->table_name();
//  const auto table = StorageManager::get().get_table(table_name);
//  std::vector<ChunkID> indexed_chunks;
//
//  for (ChunkID chunk_id{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
//    const auto chunk = table->get_chunk(chunk_id);
//    if (chunk->get_index(ColumnIndexType::GroupKey, column_ids)) {
//      indexed_chunks.emplace_back(chunk_id);
//    }
//  }
//
//  // All chunks that have an index on column_ids are handled by an IndexScan. All other chunks are handled by
//  // TableScan(s).
//  auto index_scan = std::make_shared<IndexScan>(input_operator, ColumnIndexType::GroupKey, column_ids,
//                                                predicate_node->predicate_condition(), right_values, right_values2);
//
//  // See explanation for BETWEEN handling in _translate_predicate_node above.
//  std::shared_ptr<TableScan> table_scan;
//  if (predicate_node->predicate_condition() == PredicateCondition::Between) {
//    auto table_scan_gt =
//        std::make_shared<TableScan>(input_operator, column_id, PredicateCondition::GreaterThanEquals, value);
//    table_scan_gt->set_excluded_chunk_ids(indexed_chunks);
//
//    table_scan = std::make_shared<TableScan>(table_scan_gt, column_id, PredicateCondition::LessThanEquals,
//                                             *predicate_node->value2());
//  } else {
//    table_scan = std::make_shared<TableScan>(input_operator, column_id, predicate_node->predicate_condition(), value);
//  }
//
//  index_scan->set_included_chunk_ids(indexed_chunks);
//  table_scan->set_excluded_chunk_ids(indexed_chunks);
//
//  return std::make_shared<UnionPositions>(index_scan, table_scan);
//}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_alias_node(
const std::shared_ptr<opossum::AbstractLQPNode> &node) const {
  const auto alias_node = std::dynamic_pointer_cast<AliasNode>(node);
  const auto input_node = alias_node->left_input();
  const auto input_operator = translate_node(input_node);

  auto column_ids = std::vector<ColumnID>();
  column_ids.reserve(alias_node->output_column_expressions().size());

  for (const auto& expression : alias_node->output_column_expressions()) {
    column_ids.emplace_back(input_node->get_column_id(*expression));
  }

  return std::make_shared<AliasOperator>(input_operator, column_ids, alias_node->aliases);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_projection_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_node = node->left_input();
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);
  const auto input_operator = translate_node(input_node);

  return std::make_shared<Projection>(input_operator,
                                      _translate_expressions(projection_node->output_column_expressions(), input_node));
}

//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_sort_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
//  auto input_operator = translate_node(node->left_input());
//
//  /**
//   * Go through all the order descriptions and create a sort operator for each of them.
//   * Iterate in reverse because the sort operator does not support multiple columns, and instead relies on stable sort.
//   * We therefore sort by the n+1-th column before sorting by the n-th column.
//   */
//
//  std::shared_ptr<AbstractOperator> result_operator;
//  const auto& definitions = sort_node->order_by_definitions();
//  if (definitions.size() > 1) {
//    PerformanceWarning("Multiple ORDER BYs are executed one-by-one");
//  }
//  for (auto it = definitions.rbegin(); it != definitions.rend(); it++) {
//    const auto& definition = *it;
//    result_operator = std::make_shared<Sort>(input_operator, node->get_output_column_id(definition.column_reference),
//                                             definition.order_by_mode);
//    input_operator = result_operator;
//  }
//
//  return result_operator;
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_join_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto input_left_operator = translate_node(node->left_input());
//  const auto input_right_operator = translate_node(node->right_input());
//
//  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
//
//  if (join_node->join_mode() == JoinMode::Cross) {
//    PerformanceWarning("CROSS join used");
//    return std::make_shared<Product>(input_left_operator, input_right_operator);
//  }
//
//  DebugAssert(static_cast<bool>(join_node->join_column_references()), "Cannot translate Join without columns.");
//  DebugAssert(static_cast<bool>(join_node->predicate_condition()), "Cannot translate Join without PredicateCondition.");
//
//  ColumnIDPair join_column_ids;
//  join_column_ids.first = join_node->left_input()->get_output_column_id(join_node->join_column_references()->first);
//  join_column_ids.second = join_node->right_input()->get_output_column_id(join_node->join_column_references()->second);
//
//  if (*join_node->predicate_condition() == PredicateCondition::Equals && join_node->join_mode() != JoinMode::Outer) {
//    return std::make_shared<JoinHash>(input_left_operator, input_right_operator, join_node->join_mode(),
//                                      join_column_ids, *(join_node->predicate_condition()));
//  }
//
//  return std::make_shared<JoinSortMerge>(input_left_operator, input_right_operator, join_node->join_mode(),
//                                         join_column_ids, *(join_node->predicate_condition()));
//}
//
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);

  const auto input_operator = translate_node(node->left_input());

  const auto aggregate_pqp_expressions = _translate_expressions(aggregate_node->aggregate_expressions, node->left_input());
  const auto group_by_pqp_expressions = _translate_expressions(aggregate_node->group_by_expressions, node->left_input());

  // Create AggregateColumnDefinitions from AggregateExpressions
  // All aggregate_pqp_expressions have to be AggregateExpressions and their argument() has to be a PQPColumnExpression
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  aggregate_column_definitions.reserve(aggregate_pqp_expressions.size());
  for (const auto& expression : aggregate_pqp_expressions) {
    Assert(expression->type == ExpressionType::Aggregate, "Expression '" + expression->as_column_name() + "' used as AggregateExpression is not an AggregateExpression");

    const auto& aggregate_expression = std::static_pointer_cast<AggregateExpression>(expression);

    if (aggregate_expression->argument()) {
      Assert(aggregate_expression->argument()->type, "The argument of AggregateExpression '" + expression->as_column_name() + "' couldn't be resolved");

      const auto column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(aggregate_expression->argument());
      Assert(column_expression, "Only PQPColumnExpressions valid here.");

      aggregate_column_definitions.emplace_back(aggregate_expression->aggregate_function, column_expression->column_id, expression->as_column_name());

    } else {
      aggregate_column_definitions.emplace_back(aggregate_expression->aggregate_function, std::nullopt, expression->as_column_name());
    }
  }

  // Create GroupByColumns from the GroupBy expressions
  std::vector<ColumnID> group_by_column_ids;
  group_by_column_ids.reserve(group_by_pqp_expressions.size());

  for (const auto& group_by_expression : group_by_pqp_expressions) {
    const auto group_by_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(group_by_expression);
    Assert(group_by_column_expression, "Only PQPColumnExpressions valid here.");
    group_by_column_ids.emplace_back(group_by_column_expression->column_id);
  }

  return std::make_shared<Aggregate>(input_operator, aggregate_column_definitions, group_by_column_ids);
}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_limit_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto input_operator = translate_node(node->left_input());
//  auto limit_node = std::dynamic_pointer_cast<LimitNode>(node);
//  return std::make_shared<Limit>(input_operator, limit_node->num_rows());
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_insert_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto input_operator = translate_node(node->left_input());
//  auto insert_node = std::dynamic_pointer_cast<InsertNode>(node);
//  return std::make_shared<Insert>(insert_node->table_name(), input_operator);
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_delete_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto input_operator = translate_node(node->left_input());
//  auto delete_node = std::dynamic_pointer_cast<DeleteNode>(node);
//  return std::make_shared<Delete>(delete_node->table_name(), input_operator);
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_update_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto input_operator = translate_node(node->left_input());
//  auto update_node = std::dynamic_pointer_cast<UpdateNode>(node);
//
//  auto new_value_exprs = _translate_expressions(update_node->column_expressions(), node);
//
//  auto projection = std::make_shared<Projection>(input_operator, new_value_exprs);
//  return std::make_shared<Update>(update_node->table_name(), input_operator, projection);
//}
//
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_union_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto union_node = std::dynamic_pointer_cast<UnionNode>(node);

  const auto input_operator_left = translate_node(node->left_input());
  const auto input_operator_right = translate_node(node->right_input());

  switch (union_node->union_mode) {
    case UnionMode::Positions:
      return std::make_shared<UnionPositions>(input_operator_left, input_operator_right);
  }
}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_validate_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto input_operator = translate_node(node->left_input());
//  return std::make_shared<Validate>(input_operator);
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_show_tables_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  DebugAssert(node->left_input() == nullptr, "ShowTables should not have an input operator.");
//  return std::make_shared<ShowTables>();
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_show_columns_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  DebugAssert(node->left_input() == nullptr, "ShowColumns should not have an input operator.");
//  const auto show_columns_node = std::dynamic_pointer_cast<ShowColumnsNode>(node);
//  return std::make_shared<ShowColumns>(show_columns_node->table_name());
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_view_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(node);
//  return std::make_shared<CreateView>(create_view_node->view_name(), create_view_node->lqp());
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_drop_view_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  const auto drop_view_node = std::dynamic_pointer_cast<DropViewNode>(node);
//  return std::make_shared<DropView>(drop_view_node->view_name());
//}
//
//std::shared_ptr<AbstractOperator> LQPTranslator::_translate_dummy_table_node(
//    const std::shared_ptr<AbstractLQPNode>& node) const {
//  return std::make_shared<TableWrapper>(Projection::dummy_table());
//}

std::vector<std::shared_ptr<AbstractExpression>> LQPTranslator::_translate_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& lqp_expressions,
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto pqp_expressions = expressions_copy(lqp_expressions);

  for (auto& pqp_expression : pqp_expressions) {
    /**
     * Resolve Expressions to PQPColumnExpressions referencing columns from the input Operator. After this, no
     * LQPColumnExpressions remain in the pqp_expression and it is a valid PQP expression.
     */

    visit_expression(pqp_expression, [&](auto & expression) {
      // Resolve SubSelectExpression
      if (expression->type == ExpressionType::Select) {
        const auto lqp_select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(expression);
        Assert(lqp_select_expression, "Expected LQPSelectExpression");

        const auto sub_select_pqp = LQPTranslator{}.translate_node(lqp_select_expression->lqp);
        const auto sub_select_data_type = lqp_select_expression->data_type();
        const auto sub_select_nullable = lqp_select_expression->is_nullable();
        auto sub_select_parameters = std::vector<ColumnID>{};
        sub_select_parameters.reserve(lqp_select_expression->referenced_external_expressions().size());

        for (const auto& external_expression : lqp_select_expression->referenced_external_expressions()) {
          sub_select_parameters.emplace_back(node->get_column_id(*external_expression));
        }

        expression = std::make_shared<PQPSelectExpression>(
          sub_select_pqp,
          sub_select_data_type,
          sub_select_nullable,
          sub_select_parameters
        );
        return false;
      }

      // Try to resolve the Expression to a column from the input node
      const auto column_id = node->find_column_id(*expression);
      if (column_id) {
        const auto referenced_expression = node->output_column_expressions()[*column_id];
        expression = std::make_shared<PQPColumnExpression>(*column_id,
                                                           referenced_expression->data_type(),
                                                           referenced_expression->is_nullable(),
                                                           referenced_expression->as_column_name());
        return false;
      }

      Assert(expression->type != ExpressionType::Column, "Failed to resolve Column, LQP is invalid");

      return true;
    });
  }

  return pqp_expressions;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate(const AbstractLQPNode& input_node, const std::shared_ptr<AbstractOperator>& input_operator, const AbstractExpression& left, const PredicateCondition predicate_condition, const AbstractExpression& right) {
  // LeftOperand must be a column, if it isn't switch operands
  if (!input_node.find_column_id(left)) {
    Assert(input_node.find_column_id(right), "One Predicate argument must be a column");
    return _translate_predicate(input_node, input_operator, right, flip_predicate_condition(predicate_condition), left);
  }

  const auto left_column_id = input_node.get_column_id(left);

  auto right_parameter = AllParameterVariant{};

  // Except for Value and ValuePlaceholders every expression is resolved to a column
  if (right.type == ExpressionType::Value) {
    const auto& value_expression = static_cast<const ValueExpression&>(right);
    right_parameter = value_expression.value;
  } else if (right.type == ExpressionType::ValuePlaceholder) {
    const auto& value_placeholder_expression = static_cast<const ValuePlaceholderExpression&>(right);
    right_parameter = value_placeholder_expression.value_placeholder;
  } else {
    right_parameter = input_node.get_column_id(right);
  }

  return std::make_shared<TableScan>(input_operator,
                                     left_column_id,
                                     predicate_condition,
                                     right_parameter);
}

}  // namespace opossum
