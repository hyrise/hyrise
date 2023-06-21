#pragma once

#include "types.hpp"

namespace hyrise {

/**
 * Two convenience methods that do the same thing. Take a number (e.g., the number of columns) and a set of pruned
 * item ID (e.g., pruned column IDs) and return a mapping that maps the item IDs of the initial table to the pruned
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
