#pragma once

#include "types.hpp"

namespace opossum {

// For a given ColumnID column_id and a sequence of pruned ColumnIDs, this function calculates the original ColumnID
// (before pruning) for column_id.
ColumnID column_id_before_pruning(ColumnID column_id, const std::vector<ColumnID>& pruned_column_ids);

}  // namespace opossum
