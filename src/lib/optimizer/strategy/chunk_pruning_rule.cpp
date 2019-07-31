#include "chunk_pruning_rule.hpp"

#include <algorithm>
#include <iostream>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "lossless_cast.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string ChunkPruningRule::name() const { return "Chunk Pruning Rule"; }

void ChunkPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // we only want to follow chains of predicates
  if (node->type != LQPNodeType::Predicate) {
    _apply_to_inputs(node);
    return;
  }
  DebugAssert(node->input_count() == 1, "Predicate nodes should only have 1 input");
  // try to find a chain of predicate nodes that ends in a leaf
  std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

  // Gather PredicateNodes on top of a StoredTableNode. Ignore non-filtering and ValidateNodes.
  auto current_node = node;
  while (current_node->type == LQPNodeType::Predicate || current_node->type == LQPNodeType::Validate ||
         _is_non_filtering_node(*current_node)) {
    if (current_node->type == LQPNodeType::Predicate) {
      predicate_nodes.emplace_back(std::static_pointer_cast<PredicateNode>(current_node));
    }

    // Once a node has multiple outputs, we're not talking about a Predicate chain anymore
    if (current_node->output_count() > 1) {
      _apply_to_inputs(node);
      return;
    }

    current_node = current_node->left_input();
  }

  if (current_node->type != LQPNodeType::StoredTable) {
    _apply_to_inputs(node);
    return;
  }
  const auto stored_table = std::static_pointer_cast<StoredTableNode>(current_node);
  DebugAssert(stored_table->input_count() == 0, "Stored table nodes should not have inputs.");

  /**
   * A chain of predicates followed by a stored table node was found.
   */
  auto table = StorageManager::get().get_table(stored_table->table_name);

  std::set<ChunkID> pruned_chunk_ids;
  for (auto& predicate : predicate_nodes) {
    auto new_exclusions = _compute_exclude_list(*table, *predicate->predicate(), stored_table);
    pruned_chunk_ids.insert(new_exclusions.begin(), new_exclusions.end());
  }

  // wanted side effect of using sets: pruned_chunk_ids vector is sorted
  auto& already_pruned_chunk_ids = stored_table->pruned_chunk_ids();
  if (!already_pruned_chunk_ids.empty()) {
    std::vector<ChunkID> intersection;
    std::set_intersection(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), pruned_chunk_ids.begin(),
                          pruned_chunk_ids.end(), std::back_inserter(intersection));
    stored_table->set_pruned_chunk_ids(intersection);
  } else {
    stored_table->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
  }
}

std::set<ChunkID> ChunkPruningRule::_compute_exclude_list(
    const Table& table, const AbstractExpression& predicate,
    const std::shared_ptr<StoredTableNode>& stored_table_node) const {
  // Hacky:
  // `table->table_statistics()` contains AttributeStatistics for all columns, even those that are pruned in
  // `stored_table_node`.
  // To be able to build a OperatorScanPredicate that contains a ColumnID referring to the correct AttributeStatistics
  // in `table->table_statistics()`, we create a clone of `stored_table_node` without the pruning info.
  auto stored_table_node_without_column_pruning =
      std::static_pointer_cast<StoredTableNode>(stored_table_node->deep_copy());
  stored_table_node_without_column_pruning->set_pruned_column_ids({});
  const auto predicate_without_column_pruning = expression_copy_and_adapt_to_different_lqp(
      predicate, {{stored_table_node, stored_table_node_without_column_pruning}});
  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_without_column_pruning,
                                                                          *stored_table_node_without_column_pruning);
  // End of hacky

  if (!operator_predicates) return {};

  std::set<ChunkID> result;

  for (const auto& operator_predicate : *operator_predicates) {
    // Cannot prune column-to-column predicates, at the moment. Column-to-placeholder predicates are never prunable.
    if (!is_variant(operator_predicate.value)) {
      continue;
    }

    const auto column_data_type =
        stored_table_node_without_column_pruning->column_expressions()[operator_predicate.column_id]->data_type();

    // If `value` cannot be converted losslessly to the column data type, we rather skip pruning than running into
    // errors with lossful casting and pruning Chunks that we shouldn't have pruned.
    auto value = lossless_variant_cast(boost::get<AllTypeVariant>(operator_predicate.value), column_data_type);
    if (!value) {
      continue;
    }

    auto value2 = std::optional<AllTypeVariant>{};
    if (operator_predicate.value2) {
      // Cannot prune column-to-column predicates, at the moment. Column-to-placeholder predicates are never prunable.
      if (!is_variant(*operator_predicate.value2)) {
        continue;
      }

      // If `value2` cannot be converted losslessly to the column data type, we rather skip pruning than running into
      // errors with lossful casting and pruning Chunks that we shouldn't have pruned.
      value2 = lossless_variant_cast(boost::get<AllTypeVariant>(*operator_predicate.value2), column_data_type);
      if (!value2) {
        continue;
      }
    }

    auto condition = operator_predicate.predicate_condition;

    const auto chunk_count = table.chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table.get_chunk(chunk_id);
      if (!chunk) continue;

      const auto pruning_statistics = chunk->pruning_statistics();
      if (!pruning_statistics) continue;

      const auto segment_statistics = (*pruning_statistics)[operator_predicate.column_id];
      if (_can_prune(*segment_statistics, condition, *value, value2)) {
        result.insert(chunk_id);
      }
    }
  }

  return result;
}

bool ChunkPruningRule::_can_prune(const BaseAttributeStatistics& base_segment_statistics,
                                  const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                                  const std::optional<AllTypeVariant>& variant_value2) const {
  auto can_prune = false;

  resolve_data_type(base_segment_statistics.data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto& segment_statistics = static_cast<const AttributeStatistics<ColumnDataType>&>(base_segment_statistics);

    // Range filters are only available for arithmetic (non-string) types.
    if constexpr (std::is_arithmetic_v<ColumnDataType>) {
      if (segment_statistics.range_filter) {
        if (segment_statistics.range_filter->does_not_contain(predicate_condition, variant_value, variant_value2)) {
          can_prune = true;
        }
      }
    }

    if (segment_statistics.min_max_filter) {
      if (segment_statistics.min_max_filter->does_not_contain(predicate_condition, variant_value, variant_value2)) {
        can_prune = true;
      }
    }
  });

  return can_prune;
}

bool ChunkPruningRule::_is_non_filtering_node(const AbstractLQPNode& node) const {
  return node.type == LQPNodeType::Alias || node.type == LQPNodeType::Projection || node.type == LQPNodeType::Sort;
}

}  // namespace opossum
