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

namespace {
using namespace hyrise;  // NOLINT
using PredicatePruningChain = std::vector<std::shared_ptr<PredicateNode>>;

/**
 * This function traverses the LQP upwards from @param next_node to find all predicates that filter
 * @param stored_table_node. If the LQP branches, recursion is used to continue @param current_predicate_pruning_chain
 * for all branches.
 * @param visited_predicate_nodes is for debugging purposes only.
 *
 * @returns all predicate pruning chain(s) that filter @param stored_table_node. Note, that these chains can overlap
 * in their first few nodes.
 */
std::vector<PredicatePruningChain> find_predicate_pruning_chains_by_stored_table_node_recursively(
    const std::shared_ptr<AbstractLQPNode>& next_node, PredicatePruningChain current_predicate_pruning_chain,
    const std::shared_ptr<StoredTableNode>& stored_table_node,
    std::unordered_set<std::shared_ptr<PredicateNode>>& visited_predicate_nodes) {
  std::vector<PredicatePruningChain> predicate_pruning_chains;

  visit_lqp_upwards(next_node, [&](const auto& current_node) {
    /**
     * In the following switch-statement, we
     *  (1) add PredicateNodes to the current predicate pruning chain, if applicable, and
     *  (2) check whether the predicate pruning chain continues or ends with current_node.
     */
    auto predicate_pruning_chain_continues = true;
    switch (current_node->type) {
      case LQPNodeType::Alias:
      case LQPNodeType::Sort:
      case LQPNodeType::StoredTable:
      case LQPNodeType::Validate:
      case LQPNodeType::Projection:
        break;
      case LQPNodeType::Predicate: {
        const auto& predicate_node = std::static_pointer_cast<PredicateNode>(current_node);
        /**
         * The recursion of this function should not lead to a repeated visitation of nodes. Due to the assumption
         * that predicate pruning chains do not merge after having branched out, predicate nodes are checked
         * for revisitation.
         */
        Assert(!visited_predicate_nodes.contains(predicate_node),
               "Predicate chains are not expected to merge after having branched.");
        visited_predicate_nodes.insert(predicate_node);

        // PredicateNode might not belong to the current predicate pruning chain,
        // e.g. when it follows a JoinNode and references LQPColumnExpressions from other StoredTableNodes.
        auto belongs_to_predicate_pruning_chain = true;
        const auto& predicate_expression = predicate_node->predicate();
        visit_expression(predicate_expression, [&](const auto& expression) {
          if (expression->type != ExpressionType::LQPColumn) {
            return ExpressionVisitation::VisitArguments;
          }
          const auto& column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          if (column_expression->original_node.lock() != stored_table_node) {
            // PredicateNode does not filter stored_table_node.
            // Therefore, it is not added to the current predicate pruning chain.
            belongs_to_predicate_pruning_chain = false;
          }
          return ExpressionVisitation::DoNotVisitArguments;
        });
        if (belongs_to_predicate_pruning_chain) {
          current_predicate_pruning_chain.emplace_back(predicate_node);
        }
      } break;
      case LQPNodeType::Join: {
        // Check whether the predicate pruning chain can continue after the join
        predicate_pruning_chain_continues = false;
        auto join_node = std::static_pointer_cast<JoinNode>(current_node);
        for (const auto& expression : join_node->output_expressions()) {
          if (expression->type != ExpressionType::LQPColumn) {
            continue;
          }
          const auto column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          if (column_expression->original_node.lock() == stored_table_node) {
            // At least one column expression of stored_table_node survives the join.
            predicate_pruning_chain_continues = true;
            break;
          }
        }
      } break;
      default:
        // For all other types of nodes, we cancel the predicate pruning chain.
        predicate_pruning_chain_continues = false;
    }

    if (!predicate_pruning_chain_continues) {
      // We do not have to go the LQP-tree up further because the predicate pruning chain is complete.
      predicate_pruning_chains.emplace_back(current_predicate_pruning_chain);
      return LQPUpwardVisitation::DoNotVisitOutputs;
    }

    /**
     * In case the predicate pruning chain branches, we use recursion to continue the predicate chain for each branch
     * individually.
     */
    if (current_node->outputs().size() > 1) {
      for (auto& output_node : current_node->outputs()) {
        auto continued_predicate_pruning_chains = find_predicate_pruning_chains_by_stored_table_node_recursively(
            output_node, current_predicate_pruning_chain, stored_table_node, visited_predicate_nodes);

        predicate_pruning_chains.insert(predicate_pruning_chains.end(), continued_predicate_pruning_chains.begin(),
                                        continued_predicate_pruning_chains.end());
      }
      return LQPUpwardVisitation::DoNotVisitOutputs;
    }

    // Continue without recursion
    return LQPUpwardVisitation::VisitOutputs;
  });

  return predicate_pruning_chains;
}

}  // namespace

namespace hyrise {

std::string ChunkPruningRule::name() const {
  static const auto name = std::string{"ChunkPruningRule"};
  return name;
}

void ChunkPruningRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  std::unordered_map<std::shared_ptr<StoredTableNode>, std::vector<PredicatePruningChain>>
      predicate_pruning_chains_by_stored_table_node;

  // (1) Collect all StoredTableNodes and find the chains of PredicateNodes that sit on top of them
  const auto nodes = lqp_find_nodes_by_type(lqp_root, LQPNodeType::StoredTable);
  for (const auto& node : nodes) {
    const auto& stored_table_node = std::static_pointer_cast<StoredTableNode>(node);
    predicate_pruning_chains_by_stored_table_node.emplace(
        stored_table_node, _find_predicate_pruning_chains_by_stored_table_node(stored_table_node));
  }
  // (2) Set pruned chunks for each StoredTableNode
  for (const auto& [stored_table_node, predicate_pruning_chains] : predicate_pruning_chains_by_stored_table_node) {
    if (predicate_pruning_chains.empty()) {
      continue;
    }

    // (2.1) Determine set of pruned chunks per predicate pruning chain
    std::vector<std::set<ChunkID>> pruned_chunk_id_sets;
    for (const auto& predicate_pruning_chain : predicate_pruning_chains) {
      auto exclusions = _compute_exclude_list(predicate_pruning_chain, stored_table_node);
      pruned_chunk_id_sets.emplace_back(std::move(exclusions));
    }

    // (2.2) Calculate the intersection of pruned chunks across all predicate pruning chains
    auto pruned_chunk_ids = _intersect_chunk_ids(pruned_chunk_id_sets);
    if (pruned_chunk_ids.empty()) {
      continue;
    }

    // (2.3) Set the pruned chunk ids of stored_table_node
    DebugAssert(stored_table_node->pruned_chunk_ids().empty(),
                "Did not expect a StoredTableNode with an already existing set of pruned chunk ids.");
    // Wanted side effect of using sets: pruned_chunk_ids vector is already sorted
    stored_table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
  }
}

/**
 * @returns chains of PredicateNodes that sit on top of the given @param stored_table_node.
 */
std::vector<PredicatePruningChain> ChunkPruningRule::_find_predicate_pruning_chains_by_stored_table_node(
    const std::shared_ptr<StoredTableNode>& stored_table_node) {
  /**
   * In the following, we use a recursive function to traverse the LQP upwards from stored_table_node. It returns all
   * predicate pruning chains that filter stored_table_node.
   * It takes the following four arguments:
   *   1. The first argument marks the starting point of the LQP upwards-traversal. Hence, we pass stored_table_node.
   *   2. The second argument refers to the current predicate pruning chain. Since the LQP traversal has not yet
   *      started, we pass an empty vector.
   *   3. The third argument refers to the StoredTableNode for which predicate pruning chains should be returned.
   *      Therefore, we have to pass stored_table_node again.
   *   4. The fourth argument is used for debugging purposes: The following function's recursion should not lead to a
   *      repeated visitation of nodes. Due to the assumption that predicate pruning chains do not merge after
   *      having branched out, visited predicate nodes are tracked and checked for revisitation in an Assert.
   */
  std::unordered_set<std::shared_ptr<PredicateNode>> visited_predicate_nodes;  // for debugging / Assert purposes
  return find_predicate_pruning_chains_by_stored_table_node_recursively(stored_table_node, {}, stored_table_node,
                                                                        visited_predicate_nodes);
}

std::set<ChunkID> ChunkPruningRule::_compute_exclude_list(
    const PredicatePruningChain& predicate_pruning_chain,
    const std::shared_ptr<StoredTableNode>& stored_table_node) const {
  std::set<ChunkID> excluded_chunk_ids;
  for (const auto& predicate_node : predicate_pruning_chain) {
    // Determine the set of chunks that can be excluded for the given PredicateNode's predicate.
    auto excluded_chunk_ids_iter =

        _excluded_chunk_ids_by_predicate_node_cache.find(std::make_pair(stored_table_node, predicate_node));
    if (excluded_chunk_ids_iter != _excluded_chunk_ids_by_predicate_node_cache.end()) {
      // Shortcut: The given PredicateNode is part of multiple predicate pruning chains and the set of excluded chunks
      //           has already been calculated.
      excluded_chunk_ids.insert(excluded_chunk_ids_iter->second.begin(), excluded_chunk_ids_iter->second.end());
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

    if (!operator_predicates) {
      return {};
    }

    std::set<ChunkID> current_excluded_chunk_ids;
    auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);

    const auto stored_table_node_output_expressions = stored_table_node_without_column_pruning->output_expressions();
    for (const auto& operator_predicate : *operator_predicates) {
      // Cannot prune column-to-column predicates, at the moment. Column-to-placeholder predicates are never prunable.
      if (!is_variant(operator_predicate.value)) {
        continue;
      }

      const auto column_data_type = stored_table_node_output_expressions[operator_predicate.column_id]->data_type();

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

      const auto chunk_count = table->chunk_count();
      auto num_rows_pruned = size_t{0};
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        if (!chunk) {
          continue;
        }

        const auto pruning_statistics = chunk->pruning_statistics();
        if (!pruning_statistics) {
          continue;
        }

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
          current_excluded_chunk_ids.insert(chunk_id);
        }
      }

      if (num_rows_pruned > size_t{0}) {
        const auto& old_statistics =
            stored_table_node->table_statistics ? stored_table_node->table_statistics : table->table_statistics();
        const auto pruned_statistics = _prune_table_statistics(*old_statistics, operator_predicate, num_rows_pruned);
        stored_table_node->table_statistics = pruned_statistics;
      }
    }

    // Cache result
    _excluded_chunk_ids_by_predicate_node_cache.emplace(std::make_pair(stored_table_node, predicate_node),
                                                        current_excluded_chunk_ids);
    // Add to global excluded list because we collect excluded chunks for the whole predicate pruning chain
    excluded_chunk_ids.insert(current_excluded_chunk_ids.begin(), current_excluded_chunk_ids.end());
  }

  return excluded_chunk_ids;
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

  Assert(old_statistics.row_count >= 0, "Did not expect a negative row count.");
  const auto column_count = old_statistics.column_statistics.size();

  std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics(column_count);

  auto scale = 1.0f;
  if (old_statistics.row_count > 0) {
    scale = 1 - (static_cast<float>(num_rows_pruned) / old_statistics.row_count);
  }
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

  return std::make_shared<TableStatistics>(
      std::move(column_statistics), std::max(0.0f, old_statistics.row_count - static_cast<float>(num_rows_pruned)));
}

std::set<ChunkID> ChunkPruningRule::_intersect_chunk_ids(const std::vector<std::set<ChunkID>>& chunk_id_sets) {
  if (chunk_id_sets.empty() || chunk_id_sets.at(0).empty()) {
    return {};
  }
  if (chunk_id_sets.size() == 1) {
    return chunk_id_sets.at(0);
  }

  std::set<ChunkID> chunk_id_set = chunk_id_sets.at(0);
  for (auto set_idx = size_t{1}; set_idx < chunk_id_sets.size(); ++set_idx) {
    const auto& current_chunk_id_set = chunk_id_sets.at(set_idx);
    if (current_chunk_id_set.empty()) {
      return {};
    }

    std::set<ChunkID> intersection;
    std::set_intersection(chunk_id_set.begin(), chunk_id_set.end(), current_chunk_id_set.begin(),
                          current_chunk_id_set.end(), std::inserter(intersection, intersection.end()));
    chunk_id_set = std::move(intersection);
  }

  return chunk_id_set;
}

}  // namespace hyrise
