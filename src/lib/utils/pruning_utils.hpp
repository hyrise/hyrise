#pragma once

#include <set>

#include <boost/functional/hash.hpp>

#include "types.hpp"

namespace hyrise {

class StoredTableNode;
class TableStatistics;
struct OperatorScanPredicate;
class PredicateNode;

using PredicatePruningChain = std::vector<std::shared_ptr<PredicateNode>>;
using StoredTableNodePredicateNodePair = std::pair<std::shared_ptr<StoredTableNode>, std::shared_ptr<PredicateNode>>;

std::set<ChunkID> compute_chunk_exclude_list(
    const PredicatePruningChain& predicate_pruning_chain, const std::shared_ptr<StoredTableNode>& stored_table_node,
    std::unordered_map<StoredTableNodePredicateNodePair, std::set<ChunkID>,
                       boost::hash<StoredTableNodePredicateNodePair>>& excluded_chunk_ids_by_predicate);

// Convenience version when the result cache is not used by the caller for future calls, e.g., when there is only a
// single PredicatePruningChain.
std::set<ChunkID> compute_chunk_exclude_list(const PredicatePruningChain& predicate_pruning_chain,
                                             const std::shared_ptr<StoredTableNode>& stored_table_node);

std::shared_ptr<TableStatistics> prune_table_statistics(const TableStatistics& old_statistics,
                                                        OperatorScanPredicate predicate, size_t num_rows_pruned);

/**
 * Two convenience methods that do the same thing. Take a number (e.g., the number of columns) and a list of pruned
 * item IDs (e.g., pruned column IDs) and return a mapping that maps the item IDs of the initial table to the pruned
 * table. The returned vector stores the updated item IDs at the index of the original ID if the original item was not
 * pruned. If the original item with ID i was pruned, the mapping vector contains an invalid ID (either
 * INVALID_COLUMN_ID for column pruning, or INVALID_CHUNK_ID for chunk pruning) at index i.
 * Example: a table with 3 three chunks for which chunk #1 is pruned, the returned vector is:
 *      [ 0, INVALID_CHUNK_ID, 1 ]
 *
 * @param original_count the initial number of chunks or columns.
 * @param pruned_ids the IDs which have been pruned.
 * @return a vector of updated or invalid IDs.
 */
std::vector<ColumnID> pruned_column_id_mapping(const size_t original_table_column_count,
                                               const std::vector<ColumnID>& pruned_column_ids);

std::vector<ChunkID> pruned_chunk_id_mapping(const size_t original_table_chunk_count,
                                             const std::vector<ChunkID>& pruned_chunk_ids);

// For a given ColumnID column_id and a sequence of pruned ColumnIDs, this function calculates the value that column_id
// would have had before pruning.
ColumnID column_id_before_pruning(const ColumnID column_id, const std::vector<ColumnID>& pruned_column_ids);

}  // namespace hyrise
