#pragma once

#include "types.hpp"

namespace opossum {

// Stores the updated ColumnID at the index of the original ID if the original column was not pruned.
// If the original column with ColumnID c was pruned, the id_mapping vector contains nullopt at index c.
std::vector<std::optional<ColumnID>> column_ids_after_pruning(const size_t original_table_column_count,
                                                              const std::vector<ColumnID>& pruned_column_ids);


// For a given ColumnID column_id and a sequence of pruned ColumnIDs, this function calculates the original ColumnID
// (before pruning) for column_id.
ColumnID column_id_before_pruning(ColumnID column_id, const std::vector<ColumnID>& pruned_column_ids);

}  // namespace opossum
