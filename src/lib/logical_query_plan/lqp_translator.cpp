#include "lqp_translator.hpp"

#include <memory>
#include <string>
#include <vector>

#include <boost/hana/for_each.hpp>
#include <boost/hana/tuple.hpp>

#include "abstract_lqp_node.hpp"
#include "aggregate_node.hpp"
#include "alias_node.hpp"
#include "change_meta_table_node.hpp"
#include "create_prepared_plan_node.hpp"
#include "create_table_node.hpp"
#include "create_view_node.hpp"
#include "delete_node.hpp"
#include "drop_table_node.hpp"
#include "drop_view_node.hpp"
#include "except_node.hpp"
#include "export_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "hyrise.hpp"
#include "import_node.hpp"
#include "insert_node.hpp"
#include "intersect_node.hpp"
#include "join_node.hpp"
#include "limit_node.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/alias_operator.hpp"
#include "operators/change_meta_table.hpp"
#include "operators/delete.hpp"
#include "operators/export.hpp"
#include "operators/get_table.hpp"
#include "operators/import.hpp"
#include "operators/index_scan.hpp"
#include "operators/insert.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/maintenance/create_view.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "operators/union_positions.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "predicate_node.hpp"
#include "projection_node.hpp"
#include "sort_node.hpp"
#include "static_table_node.hpp"
#include "stored_table_node.hpp"
#include "union_node.hpp"
#include "update_node.hpp"

using namespace std::string_literals;  // NOLINT

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
   * would result in multiple operators created from predicate_c and thus in performance drops.
   *
   * Deduplication:
   * _operator_by_lqp_node compares entries by value (i.e., AbstractOperator::operator==), not by identity
   * (shared_ptr::operator==). As a result, two separate, but equal LQP nodes will be translated into a single PQP
   * node. This prevents us from executing the same operation twice.
   *   Excursus: You would be right to wonder why this is not done on the LQP by some type of optimizer rule. That would
   *   indeed be the cleaner way to do it. The problem is that self-joins are only representable in the LQP if we use
   *   two independent StoredTableNodes. If we deduplicate these StoredTableNodes, the LQPColumnExpressions of the two
   *   instances would also become indistinguishable. That breaks things left and right.
   */

  const auto operator_iter = _operator_by_lqp_node.find(node);
  if (operator_iter != _operator_by_lqp_node.end()) {
    return operator_iter->second;
  }

  auto pqp = _translate_by_node_type(node->type, node);

  // Adding the actual LQP node that led to the creation of the PQP node.  Note, the LQP needs to be set in
  // _translate_predicate_node_to_index_scan() as well, because the function creates two scans operators and returns
  // only the merging union node (all three PQP nodes share the same originating LQP node).
  pqp->lqp_node = node;
  _operator_by_lqp_node.emplace(node, pqp);

  return pqp;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_by_node_type(
    LQPNodeType type, const std::shared_ptr<AbstractLQPNode>& node) const {
  switch (type) {
    // clang-format off
    case LQPNodeType::Alias:              return _translate_alias_node(node);
    case LQPNodeType::StoredTable:        return _translate_stored_table_node(node);
    case LQPNodeType::Predicate:          return _translate_predicate_node(node);
    case LQPNodeType::Projection:         return _translate_projection_node(node);
    case LQPNodeType::Sort:               return _translate_sort_node(node);
    case LQPNodeType::Join:               return _translate_join_node(node);
    case LQPNodeType::Aggregate:          return _translate_aggregate_node(node);
    case LQPNodeType::Limit:              return _translate_limit_node(node);
    case LQPNodeType::Insert:             return _translate_insert_node(node);
    case LQPNodeType::Delete:             return _translate_delete_node(node);
    case LQPNodeType::DummyTable:         return _translate_dummy_table_node(node);
    case LQPNodeType::StaticTable:        return _translate_static_table_node(node);
    case LQPNodeType::Update:             return _translate_update_node(node);
    case LQPNodeType::Validate:           return _translate_validate_node(node);
    case LQPNodeType::Union:              return _translate_union_node(node);
    case LQPNodeType::Intersect:          return _translate_intersect_node(node);
    case LQPNodeType::Except:             return _translate_except_node(node);
    case LQPNodeType::ChangeMetaTable:    return _translate_change_meta_table_node(node);

    // Maintenance operators
    case LQPNodeType::CreateView:         return _translate_create_view_node(node);
    case LQPNodeType::DropView:           return _translate_drop_view_node(node);
    case LQPNodeType::CreateTable:        return _translate_create_table_node(node);
    case LQPNodeType::DropTable:          return _translate_drop_table_node(node);
    case LQPNodeType::Import:             return _translate_import_node(node);
    case LQPNodeType::Export:             return _translate_export_node(node);
    case LQPNodeType::CreatePreparedPlan: return _translate_create_prepared_plan_node(node);
      // clang-format on

    default:
      Fail("Unknown node type encountered.");
  }
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_stored_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
  return std::make_shared<GetTable>(stored_table_node->table_name, stored_table_node->pruned_chunk_ids(),
                                    stored_table_node->pruned_column_ids());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_node = node->left_input();
  const auto input_operator = translate_node(input_node);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

  switch (predicate_node->scan_type) {
    case ScanType::TableScan:
      return _translate_predicate_node_to_table_scan(predicate_node, input_operator);
    case ScanType::IndexScan:
      return _translate_predicate_node_to_index_scan(predicate_node, input_operator);
  }

  Fail("Invalid enum value");
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node_to_index_scan(
    const std::shared_ptr<PredicateNode>& node, const std::shared_ptr<AbstractOperator>& input_operator) const {
  /**
   * Not using OperatorScanPredicate, since it splits up BETWEEN into two scans for some cases that TableScan cannot handle
   */

  auto column_id = ColumnID{0};
  auto value_variant = AllTypeVariant{NullValue{}};
  auto value2_variant = std::optional<AllTypeVariant>{};

  // Currently, we will only use IndexScans if the PredicateNode directly follows a StoredTableNode.
  // Our IndexScan implementation does not work on reference segments yet.
  Assert(node->left_input()->type == LQPNodeType::StoredTable, "IndexScan must follow a StoredTableNode.");

  const auto predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(node->predicate());
  Assert(predicate, "Expected predicate");
  Assert(!predicate->arguments.empty(), "Expected arguments");

  column_id = node->left_input()->get_column_id(*predicate->arguments[0]);
  if (predicate->arguments.size() > 1) {
    const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->arguments[1]);
    // This is necessary because we currently support single column indexes only
    Assert(value_expression, "Expected value as second argument for IndexScan");
    value_variant = value_expression->value;
  }
  if (predicate->arguments.size() > 2) {
    const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->arguments[2]);
    // This is necessary because we currently support single column indexes only
    Assert(value_expression, "Expected value as third argument for IndexScan");
    value2_variant = value_expression->value;
  }

  const std::vector<ColumnID> column_ids = {column_id};
  const std::vector<AllTypeVariant> right_values = {value_variant};
  std::vector<AllTypeVariant> right_values2 = {};
  if (value2_variant) right_values2.emplace_back(*value2_variant);

  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node->left_input());
  const auto& pruned_chunk_ids = stored_table_node->pruned_chunk_ids();

  DebugAssert(std::is_sorted(pruned_chunk_ids.cbegin(), pruned_chunk_ids.cend()),
              "Expected sorted vector of ColumnIDs");

  const auto table_name = stored_table_node->table_name;
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  std::vector<ChunkID> indexed_chunks;

  auto pruned_table_chunk_id = ChunkID{0};
  auto pruned_chunk_ids_iter = pruned_chunk_ids.cbegin();

  // Create a vector of chunk ids that have a GroupKey index and are not pruned.
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // Check if chunk is pruned
    if (pruned_chunk_ids_iter != pruned_chunk_ids.cend() && chunk_id == *pruned_chunk_ids_iter) {
      ++pruned_chunk_ids_iter;
      continue;
    }
    // Check if chunk has GroupKey index
    const auto chunk = table->get_chunk(chunk_id);
    if (chunk && chunk->get_index(SegmentIndexType::GroupKey, column_ids)) {
      indexed_chunks.emplace_back(pruned_table_chunk_id);
    }
    ++pruned_table_chunk_id;
  }

  // All chunks that have an index on column_ids are handled by an IndexScan. All other chunks are handled by
  // TableScan(s).
  auto index_scan = std::make_shared<IndexScan>(input_operator, SegmentIndexType::GroupKey, column_ids,
                                                predicate->predicate_condition, right_values, right_values2);

  const auto table_scan = _translate_predicate_node_to_table_scan(node, input_operator);

  index_scan->included_chunk_ids = indexed_chunks;
  table_scan->excluded_chunk_ids = indexed_chunks;

  // set lqp node
  index_scan->lqp_node = node;
  table_scan->lqp_node = node;

  return std::make_shared<UnionAll>(index_scan, table_scan);
}

std::shared_ptr<TableScan> LQPTranslator::_translate_predicate_node_to_table_scan(
    const std::shared_ptr<PredicateNode>& node, const std::shared_ptr<AbstractOperator>& input_operator) const {
  return std::make_shared<TableScan>(input_operator, _translate_expression(node->predicate(), node->left_input()));
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_alias_node(
    const std::shared_ptr<opossum::AbstractLQPNode>& node) const {
  const auto alias_node = std::dynamic_pointer_cast<AliasNode>(node);
  const auto input_node = alias_node->left_input();
  const auto input_operator = translate_node(input_node);

  auto column_ids = std::vector<ColumnID>();
  column_ids.reserve(alias_node->output_expressions().size());

  for (const auto& expression : alias_node->output_expressions()) {
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
                                      _translate_expressions(projection_node->node_expressions, input_node));
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_sort_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  auto input_operator = translate_node(node->left_input());

  std::shared_ptr<AbstractOperator> current_pqp = input_operator;
  const auto& pqp_expressions = _translate_expressions(sort_node->node_expressions, node->left_input());

  auto pqp_expression_iter = pqp_expressions.begin();
  auto sort_mode_iter = sort_node->sort_modes.begin();

  std::vector<SortColumnDefinition> column_definitions;
  column_definitions.reserve(pqp_expressions.size());
  for (; pqp_expression_iter != pqp_expressions.end(); ++pqp_expression_iter, ++sort_mode_iter) {
    const auto& pqp_expression = *pqp_expression_iter;
    const auto pqp_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(pqp_expression);
    Assert(pqp_column_expression,
           "Sort Expression '"s + pqp_expression->as_column_name() + "' must be available as column, LQP is invalid");

    column_definitions.emplace_back(SortColumnDefinition{pqp_column_expression->column_id, *sort_mode_iter});
  }
  current_pqp = std::make_shared<Sort>(current_pqp, column_definitions);

  return current_pqp;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_join_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto left_input_operator = translate_node(node->left_input());
  const auto right_input_operator = translate_node(node->right_input());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

  if (join_node->join_mode == JoinMode::Cross) {
    PerformanceWarning("CROSS join used");
    return std::make_shared<Product>(left_input_operator, right_input_operator);
  }

  Assert(!join_node->join_predicates().empty(), "Need predicate for non Cross Join");

  std::vector<OperatorJoinPredicate> join_predicates;
  join_predicates.reserve(join_node->join_predicates().size());

  for (const auto& predicate_expression : join_node->join_predicates()) {
    auto join_predicate =
        OperatorJoinPredicate::from_expression(*predicate_expression, *node->left_input(), *node->right_input());
    // Assert that the Join Predicates are simple, e.g. of the form <column_a> <predicate> <column_b>.
    // <column_a> and <column_b> must be on separate sides, but <column_a> need not be on the left.
    Assert(join_predicate, "Couldn't translate join predicate: "s + predicate_expression->as_column_name());
    join_predicates.emplace_back(*join_predicate);
  }

  const auto& primary_join_predicate = join_predicates.front();
  std::vector<OperatorJoinPredicate> secondary_join_predicates(join_predicates.cbegin() + 1, join_predicates.cend());

  auto join_operator = std::shared_ptr<AbstractOperator>{};

  const auto left_data_type = join_node->join_predicates().front()->arguments[0]->data_type();
  const auto right_data_type = join_node->join_predicates().front()->arguments[1]->data_type();

  // Lacking a proper cost model, we assume JoinHash is always faster than JoinSortMerge, which is faster than
  // JoinNestedLoop and thus check for an operator compatible with the JoinNode in that order
  constexpr auto JOIN_OPERATOR_PREFERENCE_ORDER =
      hana::to_tuple(hana::tuple_t<JoinHash, JoinSortMerge, JoinNestedLoop>);

  boost::hana::for_each(JOIN_OPERATOR_PREFERENCE_ORDER, [&](const auto join_operator_t) {
    using JoinOperator = typename decltype(join_operator_t)::type;

    if (join_operator) return;

    if (JoinOperator::supports({join_node->join_mode, primary_join_predicate.predicate_condition, left_data_type,
                                right_data_type, !secondary_join_predicates.empty()})) {
      join_operator = std::make_shared<JoinOperator>(left_input_operator, right_input_operator, join_node->join_mode,
                                                     primary_join_predicate, std::move(secondary_join_predicates));
    }
  });
  Assert(join_operator, "No operator implementation available for join '"s + join_node->description() + "'");

  return join_operator;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);

  const auto input_operator = translate_node(node->left_input());

  std::vector<std::shared_ptr<AggregateExpression>> pqp_aggregate_expressions;
  pqp_aggregate_expressions.reserve(aggregate_node->node_expressions.size() -
                                    aggregate_node->aggregate_expressions_begin_idx);
  for (auto expression_idx = aggregate_node->aggregate_expressions_begin_idx;
       expression_idx < aggregate_node->node_expressions.size(); ++expression_idx) {
    const auto& lqp_expression = aggregate_node->node_expressions[expression_idx];

    Assert(lqp_expression->type == ExpressionType::Aggregate,
           "Expression '" + lqp_expression->as_column_name() +
               "' used as AggregateExpression is not an AggregateExpression");

    const auto pqp_expression = _translate_expression(lqp_expression, node->left_input());
    const auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(pqp_expression);
    pqp_aggregate_expressions.emplace_back(aggregate_expression);
  }

  // Create GroupByColumns from the GroupBy expressions. For now, we expect all GroupBy expressions to be already
  // present, i.e., we do not calculate them on the fly.
  std::vector<ColumnID> group_by_column_ids;
  group_by_column_ids.reserve(aggregate_node->node_expressions.size() -
                              aggregate_node->aggregate_expressions_begin_idx);

  for (auto expression_idx = size_t{0}; expression_idx < aggregate_node->aggregate_expressions_begin_idx;
       ++expression_idx) {
    const auto& expression = aggregate_node->node_expressions[expression_idx];
    const auto column_id = node->left_input()->find_column_id(*expression);
    Assert(column_id, "GroupBy expression '"s + expression->as_column_name() + "' not available as column");
    group_by_column_ids.emplace_back(*column_id);
  }

  return std::make_shared<AggregateHash>(input_operator, pqp_aggregate_expressions, group_by_column_ids);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_limit_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  auto limit_node = std::dynamic_pointer_cast<LimitNode>(node);
  return std::make_shared<Limit>(
      input_operator, _translate_expressions({limit_node->num_rows_expression()}, node->left_input()).front());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_insert_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(node);
  return std::make_shared<Insert>(insert_node->table_name, input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_delete_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  auto delete_node = std::dynamic_pointer_cast<DeleteNode>(node);
  return std::make_shared<Delete>(input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_update_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto update_node = std::dynamic_pointer_cast<UpdateNode>(node);

  const auto input_operator_left = translate_node(node->left_input());
  const auto input_operator_right = translate_node(node->right_input());

  return std::make_shared<Update>(update_node->table_name, input_operator_left, input_operator_right);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_union_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto union_node = std::dynamic_pointer_cast<UnionNode>(node);

  const auto input_operator_left = translate_node(node->left_input());
  const auto input_operator_right = translate_node(node->right_input());

  switch (union_node->set_operation_mode) {
    case SetOperationMode::Unique:
      Fail("Currently, only the All and Positions modes are implemented for the union operation");
    case SetOperationMode::All:
      return std::make_shared<UnionAll>(input_operator_left, input_operator_right);
    case SetOperationMode::Positions:
      return std::make_shared<UnionPositions>(input_operator_left, input_operator_right);
  }
  Fail("Invalid enum value.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_intersect_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  FailInput("Hyrise does not yet support set operations");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_except_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  FailInput("Hyrise does not yet support set operations");
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_validate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  return std::make_shared<Validate>(input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_change_meta_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator_left = translate_node(node->left_input());
  const auto input_operator_right = translate_node(node->right_input());
  const auto change_meta_table_node = std::dynamic_pointer_cast<ChangeMetaTableNode>(node);
  return std::make_shared<ChangeMetaTable>(change_meta_table_node->table_name, change_meta_table_node->change_type,
                                           input_operator_left, input_operator_right);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_view_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(node);
  return std::make_shared<CreateView>(create_view_node->view_name, create_view_node->view,
                                      create_view_node->if_not_exists);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_drop_view_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto drop_view_node = std::dynamic_pointer_cast<DropViewNode>(node);
  return std::make_shared<DropView>(drop_view_node->view_name, drop_view_node->if_exists);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto create_table_node = std::dynamic_pointer_cast<CreateTableNode>(node);
  const auto input_node = create_table_node->left_input();
  return std::make_shared<CreateTable>(create_table_node->table_name, create_table_node->if_not_exists,
                                       translate_node(input_node));
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_static_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto static_table_node = std::dynamic_pointer_cast<StaticTableNode>(node);
  return std::make_shared<TableWrapper>(static_table_node->table);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_drop_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto drop_table_node = std::dynamic_pointer_cast<DropTableNode>(node);
  return std::make_shared<DropTable>(drop_table_node->table_name, drop_table_node->if_exists);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_import_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto import_node = std::dynamic_pointer_cast<ImportNode>(node);
  return std::make_shared<Import>(import_node->file_name, import_node->table_name, Chunk::DEFAULT_SIZE,
                                  import_node->file_type);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_export_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  const auto export_node = std::dynamic_pointer_cast<ExportNode>(node);
  return std::make_shared<Export>(input_operator, export_node->file_name, export_node->file_type);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_prepared_plan_node(
    const std::shared_ptr<opossum::AbstractLQPNode>& node) const {
  const auto create_prepared_plan_node = std::dynamic_pointer_cast<CreatePreparedPlanNode>(node);
  return std::make_shared<CreatePreparedPlan>(create_prepared_plan_node->name,
                                              create_prepared_plan_node->prepared_plan);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperator> LQPTranslator::_translate_dummy_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  return std::make_shared<TableWrapper>(Projection::dummy_table());
}

std::shared_ptr<AbstractExpression> LQPTranslator::_translate_expression(
    const std::shared_ptr<AbstractExpression>& lqp_expression, const std::shared_ptr<AbstractLQPNode>& node) const {
  auto pqp_expression = lqp_expression->deep_copy();

  /**
    * Resolve Expressions to PQPColumnExpressions referencing columns from the input Operator. After this, no
    * LQPColumnExpressions remain in the pqp_expression and it is a valid PQP expression.
    */
  visit_expression(pqp_expression, [&](auto& expression) {
    // Try to resolve the Expression to a column from the input node
    const auto column_id = node->find_column_id(*expression);
    if (column_id) {
      const auto referenced_expression = node->output_expressions()[*column_id];
      expression =
          std::make_shared<PQPColumnExpression>(*column_id, referenced_expression->data_type(),
                                                node->is_column_nullable(node->get_column_id(*referenced_expression)),
                                                referenced_expression->as_column_name());
      return ExpressionVisitation::DoNotVisitArguments;
    }

    // Resolve COUNT(*)
    if (AggregateExpression::is_count_star(*expression)) {
      const auto star = std::make_shared<PQPColumnExpression>(INVALID_COLUMN_ID, DataType::Long, false, "*");
      expression = std::make_shared<AggregateExpression>(AggregateFunction::Count, star);
      return ExpressionVisitation::DoNotVisitArguments;
    }

    // Resolve SubqueryExpression
    if (expression->type == ExpressionType::LQPSubquery) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(expression);
      Assert(subquery_expression, "Expected LQPSubqueryExpression");

      /**
       * Notes on generating subquery PQPs:
       *  a) For uncorrelated subqueries, operator results can be shared between identical parts in uncorrelated
       *     subqueries and outer queries. Therefore, this LQPTranslator instance is used to deduplicate subquery PQPs
       *     with _operator_by_lqp_node.
       *
       *  b) In contrast to uncorrelated subqueries, correlated subqueries cannot share identical parts with outer
       *     queries because ExpressionEvaluator::_evaluate_subquery_expression_for_row always deep-copies the whole PQP
       *     at evaluation time. The deep copy includes both correlated and uncorrelated parts.
       *     Consequently, a new LQPTranslator instance is used for correlated subqueries to avoid deduplication
       *     with outer queries. This prevents correlated subqueries from increasing the consumer count of
       *     outer query operators, which would otherwise block the automatic clearing of results.
       */
      auto subquery_pqp = std::shared_ptr<AbstractOperator>();
      if (subquery_expression->is_correlated()) {
        subquery_pqp = LQPTranslator{}.translate_node(subquery_expression->lqp);
      } else {
        subquery_pqp = translate_node(subquery_expression->lqp);
      }

      auto subquery_parameters = PQPSubqueryExpression::Parameters{};
      subquery_parameters.reserve(subquery_expression->parameter_count());

      for (auto parameter_idx = size_t{0}; parameter_idx < subquery_expression->parameter_count(); ++parameter_idx) {
        const auto parameter_column_id = node->get_column_id(*subquery_expression->parameter_expression(parameter_idx));
        subquery_parameters.emplace_back(subquery_expression->parameter_ids[parameter_idx], parameter_column_id);
      }

      // Only specify a type for the Subquery if it has exactly one column. Otherwise the DataType of the Expression
      // is undefined and obtaining it will result in a runtime error.
      if (subquery_expression->lqp->output_expressions().size() == 1u) {
        const auto subquery_data_type = subquery_expression->data_type();
        const auto subquery_nullable = subquery_expression->lqp->is_column_nullable(ColumnID{0});

        expression = std::make_shared<PQPSubqueryExpression>(subquery_pqp, subquery_data_type, subquery_nullable,
                                                             subquery_parameters);
      } else {
        expression = std::make_shared<PQPSubqueryExpression>(subquery_pqp, subquery_parameters);
      }
      return ExpressionVisitation::DoNotVisitArguments;
    }

    AssertInput(expression->type != ExpressionType::LQPColumn,
                "Failed to resolve Column '"s + expression->as_column_name() + "', LQP is invalid");

    return ExpressionVisitation::VisitArguments;
  });

  return pqp_expression;
}

std::vector<std::shared_ptr<AbstractExpression>> LQPTranslator::_translate_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& lqp_expressions,
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto pqp_expressions = std::vector<std::shared_ptr<AbstractExpression>>(lqp_expressions.size());

  for (auto expression_idx = size_t{0}; expression_idx < pqp_expressions.size(); ++expression_idx) {
    pqp_expressions[expression_idx] = _translate_expression(lqp_expressions[expression_idx], node);
  }

  return pqp_expressions;
}

}  // namespace opossum
