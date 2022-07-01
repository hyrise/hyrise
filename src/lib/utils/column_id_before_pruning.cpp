#include "column_id_before_pruning.hpp"

#include <vector>

namespace opossum {

ColumnID column_id_before_pruning(ColumnID index_column_id, const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");

  for (const auto& pruned_column_id : pruned_column_ids) {
    if (pruned_column_id > index_column_id) {
      return index_column_id;
    }
    ++index_column_id;
  }
  return index_column_id;
}

}  // namespace opossum
