#include "lqp_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "aggregate_node.hpp"
#include "constant_mappings.hpp"
#include "create_view_node.hpp"
#include "delete_node.hpp"
#include "drop_view_node.hpp"
#include "dummy_table_node.hpp"
#include "insert_node.hpp"
#include "join_node.hpp"
#include "limit_node.hpp"
#include "operators/aggregate.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/create_view.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "operators/maintenance/show_columns.hpp"
#include "operators/maintenance/show_tables.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "predicate_node.hpp"
#include "projection_node.hpp"
#include "show_columns_node.hpp"
#include "sort_node.hpp"
#include "stored_table_node.hpp"
#include "union_node.hpp"
#include "update_node.hpp"
#include "utils/performance_warning.hpp"
#include "validate_node.hpp"

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

  const auto iter = _operator_by_lqp_node.find(node);

  if (iter != _operator_by_lqp_node.end()) {
    return iter->second;
  }

  const auto pqp = _translate_by_node_type(node->type(), node);
  _operator_by_lqp_node.emplace(node, pqp);
  return pqp;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_stored_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());
  auto table_scan_node = std::dynamic_pointer_cast<PredicateNode>(node);

  if (table_scan_node->scan_type() == ScanType::Between) {
    DebugAssert(static_cast<bool>(table_scan_node->value2()), "Scan type BETWEEN requires a second value");
    PerformanceWarning("TableScan executes BETWEEN as two separate selects");

    auto table_scan1 = std::make_shared<TableScan>(input_operator, table_scan_node->column_id(),
                                                   ScanType::GreaterThanEquals, table_scan_node->value());

    return std::make_shared<TableScan>(table_scan1, table_scan_node->column_id(), ScanType::LessThanEquals,
                                       *table_scan_node->value2());
  }

  return std::make_shared<TableScan>(input_operator, table_scan_node->column_id(), table_scan_node->scan_type(),
                                     table_scan_node->value());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_projection_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto left_child = node->left_child();
  const auto input_operator = translate_node(node->left_child());
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);
  return std::make_shared<Projection>(input_operator, projection_node->column_expressions());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_sort_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  auto input_operator = translate_node(node->left_child());

  /**
   * Go through all the order descriptions and create a sort operator for each of them.
   * Iterate in reverse because the sort operator does not support multiple columns, and instead relies on stable sort.
   * We therefore sort by the n+1-th column before sorting by the n-th column.
   */

  std::shared_ptr<AbstractOperator> result_operator;
  const auto& definitions = sort_node->order_by_definitions();
  if (definitions.size() > 1) {
    PerformanceWarning("Multiple ORDER BYs are executed one-by-one");
  }
  for (auto it = definitions.rbegin(); it != definitions.rend(); it++) {
    const auto& definition = *it;
    result_operator = std::make_shared<Sort>(input_operator, definition.column_id, definition.order_by_mode);
    input_operator = result_operator;
  }

  return result_operator;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_join_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_left_operator = translate_node(node->left_child());
  const auto input_right_operator = translate_node(node->right_child());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

  if (join_node->join_mode() == JoinMode::Cross) {
    PerformanceWarning("CROSS join used");
    return std::make_shared<Product>(input_left_operator, input_right_operator);
  }

  DebugAssert(static_cast<bool>(join_node->join_column_ids()), "Cannot translate Join without join column ids.");
  DebugAssert(static_cast<bool>(join_node->scan_type()), "Cannot translate Join without ScanType.");

  if (*join_node->scan_type() == ScanType::Equals && join_node->join_mode() != JoinMode::Outer) {
    return std::make_shared<JoinHash>(input_left_operator, input_right_operator, join_node->join_mode(),
                                      *(join_node->join_column_ids()), *(join_node->scan_type()));
  }

  return std::make_shared<JoinSortMerge>(input_left_operator, input_right_operator, join_node->join_mode(),
                                         *(join_node->join_column_ids()), *(join_node->scan_type()));
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  auto aggregate_expressions = aggregate_node->aggregate_expressions();
  auto groupby_columns = aggregate_node->groupby_column_ids();

  auto aggregate_input_operator = input_operator;

  /**
   * 1. Handle arithmetic expressions in aggregate functions via Projection.
   *
   * In Hyrise, only Projections are supposed to be able to handle arithmetic expressions.
   * Therefore, if we encounter an expression within an aggregate function, we have to execute a Projection first.
   * The Aggregate will work with the output columns of that Projection.
   *
   * We only need a Projection before the aggregate if the function argument is an arithmetic expression.
   */
  auto need_projection = false;

  // Check if there are any arithmetic expressions.
  for (const auto& aggregate_expression : aggregate_expressions) {
    DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression is not a function.");

    // Check whether the expression that is the argument of the aggregate function, e.g. `a+b` in `SUM(a+b)` is, as in
    // this case, an arithmetic expression and therefore needs a Projection before the aggregate is performed. If all
    // Aggregates are in the style of `COUNT(b)` or there are just groupby columns, there is no need for a Projection.
    const auto& function_arg_expr = (aggregate_expression->expression_list())[0];
    if (function_arg_expr->is_arithmetic_operator()) {
      need_projection = true;
      break;
    }
  }

  /**
   * If there are arithmetic expressions create a Projection with:
   *
   *  - arithmetic expressions,
   *  - columns used in aggregate functions, and
   *  - GROUP BY columns
   *
   *  TODO(anybody): this might result in the same columns being created multiple times. Improve.
   */
  if (need_projection) {
    Projection::ColumnExpressions column_expressions = Expression::create_columns(groupby_columns);
    column_expressions.reserve(groupby_columns.size() + aggregate_expressions.size());

    // The Projection will only select columns used in the Aggregate, i.e., GROUP BY columns and expressions.
    // Unused columns are skipped – therefore, the ColumnIDs might change.
    // GROUP BY columns will be the first columns of the Projection.
    for (ColumnID column_id{0}; column_id < groupby_columns.size(); column_id++) {
      groupby_columns[column_id] = column_id;
    }

    // Aggregates will get consecutive ColumnIDs.
    auto current_column_id = static_cast<ColumnID::base_type>(groupby_columns.size());

    for (auto& aggregate_expression : aggregate_expressions) {
      Assert(aggregate_expression->expression_list().size(), "Aggregate: empty expression list");
      DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression is not a function.");

      // Do not project for COUNT(*)
      if (aggregate_expression->aggregate_function() == AggregateFunction::Count &&
          (aggregate_expression->expression_list())[0]->type() == ExpressionType::Star) {
        continue;
      }

      // Add original expression of the function to the Projection.
      column_expressions.emplace_back(aggregate_expression->expression_list()[0]);

      // Create a ColumnReference expression for the column id of the Projection.
      const auto column_ref_expr = Expression::create_column(ColumnID{current_column_id});
      current_column_id++;

      // Change the expression list of the expression representing the aggregate.
      aggregate_expression->set_expression_list({column_ref_expr});
    }

    aggregate_input_operator = std::make_shared<Projection>(aggregate_input_operator, column_expressions);
  }

  /**
   * 2. Build Aggregate
   */
  std::vector<AggregateDefinition> aggregate_definitions;
  aggregate_definitions.reserve(aggregate_expressions.size());

  for (const auto& aggregate_expression : aggregate_expressions) {
    DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Only functions are supported in Aggregates");

    const auto aggregate_function_type = aggregate_expression->aggregate_function();
    const auto root_expr = (aggregate_expression->expression_list())[0];

    if (aggregate_function_type == AggregateFunction::Count && root_expr->type() == ExpressionType::Star) {
      // COUNT(*) does not specify a ColumnID
      aggregate_definitions.emplace_back(CountStarID, AggregateFunction::Count, aggregate_expression->alias());
    } else {
      const auto column_id = root_expr->column_id();
      aggregate_definitions.emplace_back(column_id, aggregate_function_type, aggregate_expression->alias());
    }
  }

  return std::make_shared<Aggregate>(aggregate_input_operator, aggregate_definitions, groupby_columns);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_limit_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());
  auto limit_node = std::dynamic_pointer_cast<LimitNode>(node);
  return std::make_shared<Limit>(input_operator, limit_node->num_rows());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_insert_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(node);
  return std::make_shared<Insert>(insert_node->table_name(), input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_delete_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());
  auto delete_node = std::dynamic_pointer_cast<DeleteNode>(node);
  return std::make_shared<Delete>(delete_node->table_name(), input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_update_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());
  auto update_node = std::dynamic_pointer_cast<UpdateNode>(node);

  auto new_value_exprs = update_node->column_expressions();

  auto projection = std::make_shared<Projection>(input_operator, new_value_exprs);
  return std::make_shared<Update>(update_node->table_name(), input_operator, projection);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_union_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto union_node = std::dynamic_pointer_cast<UnionNode>(node);

  const auto input_operator_left = translate_node(node->left_child());
  const auto input_operator_right = translate_node(node->right_child());

  switch (union_node->union_mode()) {
    case UnionMode::Positions:
      return std::make_shared<UnionPositions>(input_operator_left, input_operator_right);
      break;
    default:
      Fail("UnionMode not supported");
  }
  return nullptr;  // Shouldn't be reached, but makes compilers happy
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_validate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_child());
  return std::make_shared<Validate>(input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_show_tables_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  DebugAssert(node->left_child() == nullptr, "ShowTables should not have an input operator.");
  return std::make_shared<ShowTables>();
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_show_columns_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  DebugAssert(node->left_child() == nullptr, "ShowColumns should not have an input operator.");
  const auto show_columns_node = std::dynamic_pointer_cast<ShowColumnsNode>(node);
  return std::make_shared<ShowColumns>(show_columns_node->table_name());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_view_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(node);
  return std::make_shared<CreateView>(create_view_node->view_name(), create_view_node->lqp());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_drop_view_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto drop_view_node = std::dynamic_pointer_cast<DropViewNode>(node);
  return std::make_shared<DropView>(drop_view_node->view_name());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_dummy_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  return std::make_shared<TableWrapper>(Projection::dummy_table());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_by_node_type(
    LQPNodeType type, const std::shared_ptr<AbstractLQPNode>& node) const {
  switch (type) {
    // SQL operators
    case LQPNodeType::StoredTable:
      return _translate_stored_table_node(node);
    case LQPNodeType::Predicate:
      return _translate_predicate_node(node);
    case LQPNodeType::Projection:
      return _translate_projection_node(node);
    case LQPNodeType::Sort:
      return _translate_sort_node(node);
    case LQPNodeType::Join:
      return _translate_join_node(node);
    case LQPNodeType::Aggregate:
      return _translate_aggregate_node(node);
    case LQPNodeType::Limit:
      return _translate_limit_node(node);
    case LQPNodeType::Insert:
      return _translate_insert_node(node);
    case LQPNodeType::Delete:
      return _translate_delete_node(node);
    case LQPNodeType::DummyTable:
      return _translate_dummy_table_node(node);
    case LQPNodeType::Update:
      return _translate_update_node(node);
    case LQPNodeType::Validate:
      return _translate_validate_node(node);
    case LQPNodeType::Union:
      return _translate_union_node(node);

    // Maintenance operators
    case LQPNodeType::ShowTables:
      return _translate_show_tables_node(node);
    case LQPNodeType::ShowColumns:
      return _translate_show_columns_node(node);
    case LQPNodeType::CreateView:
      return _translate_create_view_node(node);
    case LQPNodeType::DropView:
      return _translate_drop_view_node(node);

    default:
      Fail("Unknown node type encountered.");
      return nullptr;
  }
}

}  // namespace opossum
