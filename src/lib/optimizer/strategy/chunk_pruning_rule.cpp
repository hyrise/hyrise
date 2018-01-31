#include "chunk_pruning_rule.hpp"

#include <algorithm>
#include <iostream>

#include <boost/variant.hpp>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/chunk_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string ChunkPruningRule::name() const { return "Chunk Pruning Rule"; }

bool ChunkPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {

  if (node->type() == LQPNodeType::Predicate && node->child_count() == 1) {
    // try to find a chain of predicate nodes that ends in a leaf
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type() == LQPNodeType::Predicate) {
      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(current_node));
      current_node = current_node->left_child();
      // Once a node has multiple parents, we're not talking about a Predicate chain anymore
      if (current_node->type() == LQPNodeType::Predicate && current_node->parents().size() > 1) {
        _apply_to_children(node);
        return false;
      }
    }

    // skip over validation nodes
    if(current_node->type() == LQPNodeType::Validate) {
      current_node = current_node->left_child();
    }

    if (current_node->type() != LQPNodeType::StoredTable) {
        _apply_to_children(node);
        return false;
    }
    auto stored_table = std::dynamic_pointer_cast<StoredTableNode>(current_node);
    DebugAssert(stored_table->is_leaf(), "Stored table nodes should be leafs.");

    /**
     * A chain of predicates was found.
     */

    auto table = StorageManager::get().get_table(stored_table->table_name());
    std::vector<std::shared_ptr<ChunkStatistics>> statistics;
    for (ChunkID chunk_id = ChunkID(0); chunk_id < table->chunk_count(); ++chunk_id) {
      auto chunk_statistics = table->get_chunk(chunk_id)->statistics();
      if(chunk_statistics) {
        statistics.push_back(table->get_chunk(chunk_id)->statistics());
      }
    }
    std::set<ChunkID> excluded_chunks;
    for (auto & predicate : predicate_nodes) {
      auto new_exlusions = _calculate_exclude_list(statistics, predicate);
      excluded_chunks.insert(new_exlusions.begin(), new_exlusions.end());
    }

    // wanted side effect of usings sets: excluded_chunks vector is sorted
    if (!stored_table->excluded_chunks().empty()) {
      std::vector<ChunkID> intersection;
      std::set_intersection(stored_table->excluded_chunks().begin(), stored_table->excluded_chunks().end(),
                        excluded_chunks.begin(), excluded_chunks.end(),
                        std::back_inserter(intersection));
      stored_table->excluded_chunks() = intersection;
    } else {
      stored_table->excluded_chunks() = std::vector<ChunkID>(
        excluded_chunks.begin(), excluded_chunks.end());
    }

  } else {
    _apply_to_children(node);
  }

  return false;
}

std::set<ChunkID>
ChunkPruningRule::_calculate_exclude_list(const std::vector<std::shared_ptr<ChunkStatistics>>& stats,
                                          std::shared_ptr<PredicateNode> predicate) {
  std::set<ChunkID> result;
  for (uint32_t i = 0; i < stats.size(); ++i) {
    DebugAssert(is_variant(predicate->value()), "we need an AllTypeVariant");
    if (stats[i]->can_prune(predicate->column_reference().original_column_id(), boost::get<AllTypeVariant>(predicate->value()), predicate->predicate_condition())) {
      result.insert(ChunkID(i));
    }
  }
  return result;
}

}  // namespace opossum
