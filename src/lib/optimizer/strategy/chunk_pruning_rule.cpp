#include "chunk_pruning_rule.hpp"

#include <algorithm>
#include <iostream>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string ChunkPruningRule::name() const { return "Chunk Pruning Rule"; }

bool ChunkPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // we only want to follow chains of predicates
  if (node->type != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
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
      return _apply_to_inputs(node);
    }
  }

  // skip over validation nodes
  if (current_node->type == LQPNodeType::Validate) {
    current_node = current_node->left_input();
  }

  if (current_node->type != LQPNodeType::StoredTable) {
    return _apply_to_inputs(node);
  }
  auto stored_table = std::static_pointer_cast<StoredTableNode>(current_node);
  DebugAssert(stored_table->input_count() == 0, "Stored table nodes should not have inputs.");

  /**
   * A chain of predicates followed by a stored table node was found.
   */
  auto table = StorageManager::get().get_table(stored_table->table_name);
  std::vector<std::shared_ptr<ChunkStatistics>> statistics;
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    statistics.push_back(table->get_chunk(chunk_id)->statistics());
  }
  std::set<ChunkID> excluded_chunk_ids;
  for (auto& predicate : predicate_nodes) {
    auto new_exclusions = _compute_exclude_list(statistics, predicate);
    excluded_chunk_ids.insert(new_exclusions.begin(), new_exclusions.end());
  }

  // wanted side effect of usings sets: excluded_chunk_ids vector is sorted
  auto& already_excluded_chunk_ids = stored_table->excluded_chunk_ids();
  if (!already_excluded_chunk_ids.empty()) {
    std::vector<ChunkID> intersection;
    std::set_intersection(already_excluded_chunk_ids.begin(), already_excluded_chunk_ids.end(),
                          excluded_chunk_ids.begin(), excluded_chunk_ids.end(), std::back_inserter(intersection));
    stored_table->set_excluded_chunk_ids(intersection);
  } else {
    stored_table->set_excluded_chunk_ids(std::vector<ChunkID>(excluded_chunk_ids.begin(), excluded_chunk_ids.end()));
  }

  // always returns false as we never modify the LQP
  return false;
}

std::set<ChunkID> ChunkPruningRule::_compute_exclude_list(
    const std::vector<std::shared_ptr<ChunkStatistics>>& statistics,
    const std::shared_ptr<PredicateNode>& predicate_node) const {
  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate, *predicate_node);
  if (!operator_predicates) return {};

  std::set<ChunkID> result;

  for (const auto& operator_predicate : *operator_predicates) {
    if (!is_variant(operator_predicate.value)) {
      return std::set<ChunkID>();
    }
    auto& value = boost::get<AllTypeVariant>(operator_predicate.value);
    auto condition = operator_predicate.predicate_condition;
    for (size_t chunk_id = 0; chunk_id < statistics.size(); ++chunk_id) {
      // statistics[chunk_id] can be a shared_ptr initialized with a nullptr
      if (statistics[chunk_id] && statistics[chunk_id]->can_prune(operator_predicate.column_id, value, condition)) {
        result.insert(ChunkID(chunk_id));
      }
    }
  }
  return result;
}

}  // namespace opossum
