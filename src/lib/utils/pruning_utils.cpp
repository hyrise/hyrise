#include "pruning_utils.hpp"

#include <cstdlib>
#include <optional>
#include <vector>

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "lossless_cast.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Check whether any of the statistics objects available for this Segment identify the predicate as prunable.
bool can_prune(const BaseAttributeStatistics& base_segment_statistics, const PredicateCondition predicate_condition,
               const AllTypeVariant& variant_value, const std::optional<AllTypeVariant>& variant_value2) {
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

template <typename T>
std::vector<T> pruned_items_mapping(const size_t initial_item_count, const std::vector<T>& pruned_item_ids) {
  // This function assume to be used solely for column and chunk pruning.
  static_assert(std::disjunction<std::is_same<T, ColumnID>, std::is_same<T, ChunkID>>(),
                "Unexpected template type passed.");

  auto INVALID_ID = T{0};
  if constexpr (std::is_same_v<T, ColumnID>) {
    INVALID_ID = INVALID_COLUMN_ID;
  } else {
    INVALID_ID = INVALID_CHUNK_ID;
  }

  DebugAssert(std::is_sorted(pruned_item_ids.begin(), pruned_item_ids.end()),
              "Expected a sorted vector of pruned chunk IDs.");
  DebugAssert(pruned_item_ids.empty() || pruned_item_ids.back() < initial_item_count,
              "Largest pruned chunk ID is too large.");
  DebugAssert(pruned_item_ids.size() <= initial_item_count,
              "List of pruned chunks longer than chunks in actual table.");

  auto id_mapping = std::vector<T>(initial_item_count, INVALID_ID);
  auto pruned_item_ids_iter = pruned_item_ids.begin();
  auto next_updated_id = T{0};
  for (auto item_index = T{0}; item_index < initial_item_count; ++item_index) {
    if (pruned_item_ids_iter != pruned_item_ids.end() && item_index == *pruned_item_ids_iter) {
      ++pruned_item_ids_iter;
      continue;
    }

    id_mapping[item_index] = next_updated_id;
    ++next_updated_id;
  }
  return id_mapping;
}

}  // namespace

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

std::set<ChunkID> compute_chunk_exclude_list(const PredicatePruningChain& predicate_pruning_chain,
                                             const std::shared_ptr<StoredTableNode>& stored_table_node) {
  auto pruned_chunk_ids_by_predicate_node_cache =
      std::unordered_map<StoredTableNodePredicateNodePair, std::set<ChunkID>,
                         boost::hash<StoredTableNodePredicateNodePair>>{};

  return compute_chunk_exclude_list(predicate_pruning_chain, stored_table_node,
                                    pruned_chunk_ids_by_predicate_node_cache);
}

std::set<ChunkID> compute_chunk_exclude_list(
    const PredicatePruningChain& predicate_pruning_chain, const std::shared_ptr<StoredTableNode>& stored_table_node,
    std::unordered_map<StoredTableNodePredicateNodePair, std::set<ChunkID>,
                       boost::hash<StoredTableNodePredicateNodePair>>& excluded_chunk_ids_by_predicate_node) {
  auto excluded_chunk_ids = std::set<ChunkID>{};
  for (const auto& predicate_node : predicate_pruning_chain) {
    // Determine the set of chunks that can be excluded for the given PredicateNode's predicate.
    auto excluded_chunk_ids_iter =
        excluded_chunk_ids_by_predicate_node.find(std::make_pair(stored_table_node, predicate_node));

    if (excluded_chunk_ids_iter != excluded_chunk_ids_by_predicate_node.end()) {
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

    // OperatorScanPredicate::from_expression cannot translate predicates that contain subqueries, even though they do
    // not influence other predicates. Thus, we replace subquery expressions by placeholders. Doing so, we can build a
    // predicate that will simply be skipped for pruning rather than abort and do not prune at all.
    for (auto& argument : predicate_without_column_pruning->arguments) {
      if (argument->type == ExpressionType::LQPSubquery) {
        argument = placeholder_(ParameterID{0});
      }
    }
    const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_without_column_pruning,
                                                                            *stored_table_node_without_column_pruning);
    // End of hacky.

    if (!operator_predicates) {
      return {};
    }

    auto current_excluded_chunk_ids = std::set<ChunkID>{};
    const auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);

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
        if (can_prune(*segment_statistics, condition, *value, value2)) {
          const auto& already_pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
          if (std::find(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), chunk_id) ==
              already_pruned_chunk_ids.end()) {
            // Chunk was not yet marked as pruned - update statistics.
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
        const auto pruned_statistics = prune_table_statistics(*old_statistics, operator_predicate, num_rows_pruned);
        stored_table_node->table_statistics = pruned_statistics;
      }
    }

    // Cache result.
    excluded_chunk_ids_by_predicate_node.emplace(std::make_pair(stored_table_node, predicate_node),
                                                 current_excluded_chunk_ids);
    // Add to global excluded list because we collect excluded chunks for the whole predicate pruning chain.
    excluded_chunk_ids.insert(current_excluded_chunk_ids.begin(), current_excluded_chunk_ids.end());
  }

  return excluded_chunk_ids;
}

std::shared_ptr<TableStatistics> prune_table_statistics(const TableStatistics& old_statistics,
                                                        OperatorScanPredicate predicate, size_t num_rows_pruned) {
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

  auto column_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>(column_count);

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

std::vector<ColumnID> pruned_column_id_mapping(const size_t original_table_column_count,
                                               const std::vector<ColumnID>& pruned_column_ids) {
  return pruned_items_mapping<ColumnID>(original_table_column_count, pruned_column_ids);
}

std::vector<ChunkID> pruned_chunk_id_mapping(const size_t original_table_chunk_count,
                                             const std::vector<ChunkID>& pruned_chunk_ids) {
  return pruned_items_mapping<ChunkID>(original_table_chunk_count, pruned_chunk_ids);
}

ColumnID column_id_before_pruning(const ColumnID column_id, const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");

  auto original_column_id = column_id;
  for (const auto& pruned_column_id : pruned_column_ids) {
    if (pruned_column_id > original_column_id) {
      return original_column_id;
    }
    ++original_column_id;
  }
  return original_column_id;
}

}  // namespace hyrise
