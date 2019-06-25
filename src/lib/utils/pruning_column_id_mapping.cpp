#include "pruning_column_id_mapping.hpp"

#include <stdlib.h>
#include <optional>
#include <vector>

namespace opossum {

const std::vector<std::optional<ColumnID>> column_ids_after_pruning(const size_t& original_table_column_count,
                                                                    const std::vector<ColumnID>& pruned_column_ids) {
  // Stored the updated ColumnID at the index of the original ID if the original column was not pruned.
  // If the original column with ColumnID c was pruned, the id_mapping vector contains nullopt at index c.
  std::vector<std::optional<ColumnID>> column_id_mapping;

  auto column_pruned_bitvector = std::vector<bool>();

  column_pruned_bitvector.resize(original_table_column_count);
  column_id_mapping.resize(original_table_column_count);

  // Fill the bitvector
  for (const auto& pruned_column_id : pruned_column_ids) {
    column_pruned_bitvector[pruned_column_id] = true;
  }

  // Calculate new colummn ids
  auto next_updated_column_id = ColumnID{0};
  for (auto column_index = ColumnID{0}; column_index < column_pruned_bitvector.size(); ++column_index) {
    if (!column_pruned_bitvector[column_index]) {
      column_id_mapping[column_index] = next_updated_column_id++;
    }
  }
  return column_id_mapping;
}

}  // namespace opossum
