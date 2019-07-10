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

  // Gather consecutive PredicateNodes
  auto current_node = node;
  while (current_node->type == LQPNodeType::Predicate) {
    predicate_nodes.emplace_back(std::static_pointer_cast<PredicateNode>(current_node));
    current_node = current_node->left_input();
    // Once a node has multiple outputs, we're not talking about a Predicate chain anymore
    if (current_node->type == LQPNodeType::Predicate && current_node->output_count() > 1) {
      _apply_to_inputs(node);
      return;
    }
  }

  // skip over validation nodes
  if (current_node->type == LQPNodeType::Validate) {
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
    auto new_exclusions = _compute_exclude_list(statistics, predicate);
    excluded_chunk_ids.insert(new_exclusions.begin(), new_exclusions.end());
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
    const std::vector<std::shared_ptr<ChunkStatistics>>& statistics,
    const std::shared_ptr<PredicateNode>& predicate_node) const {
  const auto operator_predicates =
      OperatorScanPredicate::from_expression(*predicate_node->predicate(), *predicate_node);
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

    for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
      const auto pruning_statistics = table.get_chunk(chunk_id)->pruning_statistics();
      if (!pruning_statistics) {
        continue;
      }

      const auto segment_statistics = (*pruning_statistics)[operator_predicate.column_id];
      if (_can_prune(*segment_statistics, condition, *value, value2)) {
        result.insert(chunk_id);
      }
    }
  }

  return result;
}

}  // namespace opossum
