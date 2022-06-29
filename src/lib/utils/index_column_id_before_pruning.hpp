#pragma once

#include "types.hpp"

namespace opossum {

// If some columns have been pruned, the ColumnID to join on may differ from the original index ColumnID.
// In this function the original index ColumnID is calculated.
ColumnID index_column_id_before_pruning(ColumnID index_column_id, const std::vector<ColumnID>& pruned_column_ids);

}  // namespace opossum
