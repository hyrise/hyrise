#include "chunk_pruning_rule.hpp"

#include <algorithm>
#include <iostream>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "lossless_cast.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

using PredicateChain = std::vector<std::shared_ptr<PredicateNode>>;

void ChunkPruningRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  std::unordered_map<std::shared_ptr<StoredTableNode>, std::vector<PredicateChain>>
      predicate_chains_by_stored_table_node;

  // (1) Collect StoredTableNodes
  const auto stored_table_nodes = lqp_find_leafs<StoredTableNode>(lqp_root);
  for (const auto& stored_table_node : stored_table_nodes) {
    predicate_chains_by_stored_table_node.emplace(stored_table_node,
                                                  std::vector<PredicateChain>{});
  }

  // (2) Collect chains of PredicateNodes on top of each StoredTableNode
  for (auto& [stored_table_node, predicate_chains] : predicate_chains_by_stored_table_node) {
    predicate_chains = find_predicate_chains_recursively(stored_table_node, stored_table_node);
  }

  // (3) Set pruned chunks for each StoredTableNode
  for (auto [stored_table_node, predicate_chains] : predicate_chains_by_stored_table_node) {
    if (predicate_chains.empty()) continue;

    // (3.1) Determine set of pruned chunks per predicate chain
    auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
    std::vector<std::set<ChunkID>> pruned_chunk_id_sets;
    for (auto& predicate_chain : predicate_chains) {
      auto exclusions = _compute_exclude_list(*table, predicate_chain, stored_table_node);
      pruned_chunk_id_sets.emplace_back(exclusions);
    }

    // (3.2) Calculate the intersect of pruned chunks across predicate chains
    std::set<ChunkID> pruned_chunk_ids = intersect_chunk_ids(pruned_chunk_id_sets);
    if (pruned_chunk_ids.empty()) continue;

    // (3.2) Set pruned chunk ids
    DebugAssert(stored_table_node->pruned_chunk_ids().empty(),
                "Did not expect a StoredTableNode with an already existing set of pruned chunk ids.");
    // Wanted side effect of using sets: pruned_chunk_ids vector is already sorted
    stored_table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
  }
}

std::set<ChunkID> ChunkPruningRule::_compute_exclude_list(
    const Table& table, const PredicateChain& predicate_chain,
    const std::shared_ptr<StoredTableNode>& stored_table_node) const {
  std::set<ChunkID> global_excluded_chunk_ids;
  for (auto predicate_node : predicate_chain) {
    /**
     * Determine the set of chunks that can be excluded for the given PredicateNode's predicate.
     */
    std::set<ChunkID> local_excluded_chunk_ids;

    auto excluded_chunk_ids_iter = _excluded_chunk_ids_by_predicate_node.find(predicate_node);
    if (excluded_chunk_ids_iter != _excluded_chunk_ids_by_predicate_node.end()) {
      // Shortcut: The given PredicateNode is part of multiple predicate chains and the set of excluded chunks
      //           has already been calculated.
      local_excluded_chunk_ids = excluded_chunk_ids_iter->second;
      global_excluded_chunk_ids.insert(local_excluded_chunk_ids.begin(), local_excluded_chunk_ids.end());
      continue;
    }

    auto& predicate = *predicate_node->predicate();

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

    for (const auto& operator_predicate : *operator_predicates) {
      // Cannot prune column-to-column predicates, at the moment. Column-to-placeholder predicates are never prunable.
      if (!is_variant(operator_predicate.value)) {
        continue;
      }

      const auto column_data_type =
          stored_table_node_without_column_pruning->output_expressions()[operator_predicate.column_id]->data_type();

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
      auto num_rows_pruned = size_t{0};
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = table.get_chunk(chunk_id);
        if (!chunk) continue;

        const auto pruning_statistics = chunk->pruning_statistics();
        if (!pruning_statistics) continue;

        const auto segment_statistics = (*pruning_statistics)[operator_predicate.column_id];
        if (_can_prune(*segment_statistics, condition, *value, value2)) {
          const auto& already_pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
          if (std::find(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), chunk_id) ==
              already_pruned_chunk_ids.end()) {
            // Chunk was not yet marked as pruned - update statistics
            num_rows_pruned += chunk->size();
          } else {
            // Chunk was already pruned. While we might prune on a different predicate this time, we must make sure that
            // we do not over-prune the statistics.
          }
          local_excluded_chunk_ids.insert(chunk_id);
        }
      }

      if (num_rows_pruned > size_t{0}) {
        const auto& old_statistics =
            stored_table_node->table_statistics ? stored_table_node->table_statistics : table.table_statistics();
        const auto pruned_statistics = _prune_table_statistics(*old_statistics, operator_predicate, num_rows_pruned);
        stored_table_node->table_statistics = pruned_statistics;
      }
    }

    // Cache result
    _excluded_chunk_ids_by_predicate_node.emplace(predicate_node, local_excluded_chunk_ids);
    // Add to global excluded list because we collect excluded chunks for the whole predicate chain
    global_excluded_chunk_ids.insert(local_excluded_chunk_ids.begin(), local_excluded_chunk_ids.end());
  }

  return global_excluded_chunk_ids;
}

bool ChunkPruningRule::_can_prune(const BaseAttributeStatistics& base_segment_statistics,
                                  const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                                  const std::optional<AllTypeVariant>& variant_value2) {
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
      // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
      DebugAssert(!segment_statistics.min_max_filter,
                  "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
    }

    if (segment_statistics.min_max_filter) {
      if (segment_statistics.min_max_filter->does_not_contain(predicate_condition, variant_value, variant_value2)) {
        can_prune = true;
      }
    }
  });

  return can_prune;
}

std::shared_ptr<TableStatistics> ChunkPruningRule::_prune_table_statistics(const TableStatistics& old_statistics,
                                                                           OperatorScanPredicate predicate,
                                                                           size_t num_rows_pruned) {
  // If a chunk is pruned, we update the table statistics. This is so that the selectivity of the predicate that was
  // used for pruning can be correctly estimated. Example: For a table that has sorted values from 1 to 100 and a chunk
  // size of 10, the predicate `x > 90` has a selectivity of 10%. However, if the ChunkPruningRule removes nine chunks
  // out of ten, the selectivity is now 100%. Updating the statistics is important so that the predicate ordering
  // can properly order the predicates.
  //
  // For the column that the predicate pruned on, we remove num_rows_pruned values that do not match the predicate
  // from the statistics. See the pruned() implementation of the different statistics types for details.
  // The other columns are simply scaled to reflect the reduced table size.
  //
  // For now, this does not take any sorting on a chunk- or table-level into account. In the future, this may be done
  // to further improve the accuracy of the statistics.

  const auto column_count = old_statistics.column_statistics.size();

  std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics(column_count);

  const auto scale = 1 - (static_cast<float>(num_rows_pruned) / old_statistics.row_count);
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    if (column_id == predicate.column_id) {
      column_statistics[column_id] = old_statistics.column_statistics[column_id]->pruned(
          num_rows_pruned, predicate.predicate_condition, boost::get<AllTypeVariant>(predicate.value),
          predicate.value2 ? std::optional<AllTypeVariant>{boost::get<AllTypeVariant>(*predicate.value2)}
                           : std::nullopt);
    } else {
      column_statistics[column_id] = old_statistics.column_statistics[column_id]->scaled(scale);
    }
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics),
                                           old_statistics.row_count - static_cast<float>(num_rows_pruned));
}

std::vector<PredicateChain> ChunkPruningRule::find_predicate_chains_recursively(
    const std::shared_ptr<StoredTableNode>& stored_table_node, const std::shared_ptr<AbstractLQPNode>& node,
    PredicateChain current_predicate_chain) {
  std::vector<PredicateChain> predicate_chains;

  visit_lqp_upwards(node, [&](const auto& current_node) {
    /**
     * In the following switch-statement, we
     *  (1) add PredicateNodes to the current predicate chain, if applicable, and
     *  (2) check whether the predicate chain continues or ends with current_node.
     */
    auto predicate_chain_continues = true;
    switch (current_node->type) {
      // clang-format off
      case LQPNodeType::Alias:        break;
      case LQPNodeType::Sort:         break;
      case LQPNodeType::StoredTable:  break;
      case LQPNodeType::Validate:     break;
      case LQPNodeType::Projection:   break;
      case LQPNodeType::Predicate: {
        const auto& predicate_node = std::static_pointer_cast<PredicateNode>(current_node);

        // PredicateNode might not belong to the current predicate chain,
        // e.g. when it follows a JoinNode and contains LQPColumnExpressions from other StoredTableNodes.
        auto belongs_to_predicate_chain = true;
        const auto& predicate_expression = predicate_node->predicate();
        visit_expression(predicate_expression, [&](const auto& expression) {
          if (expression->type != ExpressionType::LQPColumn) return ExpressionVisitation::VisitArguments;
          const auto& column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          if (column_expression->original_node.lock() != stored_table_node) {
            // PredicateNode filters stored_table_node. Therefore, we want to add it to the current predicate chain.
            belongs_to_predicate_chain = false;
          }
          return ExpressionVisitation::DoNotVisitArguments;
        });
        if (belongs_to_predicate_chain) current_predicate_chain.emplace_back(predicate_node);
      } break;
      case LQPNodeType::Join: {
        auto join_node = std::static_pointer_cast<JoinNode>(current_node);
        if (join_node->join_mode != JoinMode::Semi) break;

        // Check whether the predicate chain continues after the semi join
        predicate_chain_continues = false;
        for (const auto& expression : join_node->output_expressions()) {
          if (expression->type != ExpressionType::LQPColumn) continue;
          const auto column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          if (column_expression->original_node.lock() == stored_table_node) {
            // At least one column expression of stored_table_node survives the semi join.
            // Therefore, the predicate chain might continue.
            predicate_chain_continues = true;
            break;
          }
        }
      } break;
      default:
        // For all other types of nodes, we finalize the predicate chain.
        predicate_chain_continues = false;
        // clang-format on
    }

    if (!predicate_chain_continues) {
      // We do not have to go the LQP-tree up further because the predicate chain is complete.
      predicate_chains.emplace_back(current_predicate_chain);
      return LQPUpwardVisitation::DoNotVisitOutputs;
    }

    // Use recursion, in case the predicate chain branches.
    if (current_node->outputs().size() > 1) {
      for (auto& output_node : current_node->outputs()) {
        auto continued_predicate_chains =
            find_predicate_chains_recursively(stored_table_node, output_node, current_predicate_chain);

        predicate_chains.insert(predicate_chains.end(), continued_predicate_chains.begin(),
                                continued_predicate_chains.end());
      }
      return LQPUpwardVisitation::DoNotVisitOutputs;
    }

    // Continue without recursion
    return LQPUpwardVisitation::VisitOutputs;
  });

  return predicate_chains;
}

std::set<ChunkID> ChunkPruningRule::intersect_chunk_ids(const std::vector<std::set<ChunkID>>& chunk_id_sets) {
  if (chunk_id_sets.size() == 0 || chunk_id_sets.at(0).empty()) return {};
  if (chunk_id_sets.size() == 1) return chunk_id_sets.at(0);

  std::set<ChunkID> chunk_id_set = chunk_id_sets.at(0);
  for (size_t set_idx = 1; set_idx < chunk_id_sets.size(); ++set_idx) {
    auto current_chunk_id_set = chunk_id_sets.at(set_idx);
    if (current_chunk_id_set.empty()) return {};

    std::set<ChunkID> intersection;
    std::set_intersection(chunk_id_set.begin(), chunk_id_set.end(), current_chunk_id_set.begin(),
                          current_chunk_id_set.end(), std::inserter(intersection, intersection.end()));
    chunk_id_set = std::move(intersection);
  }

  return chunk_id_set;
}

}  // namespace opossum
