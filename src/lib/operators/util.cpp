#include "util.hpp"

#include "../storage/chunk.hpp"
#include "../storage/reference_column.hpp"
#include "../storage/table.hpp"

namespace opossum {

bool chunk_references_only_one_table(const Chunk& chunk) {
  if (chunk.col_count() == 0) return false;

  auto first_column = std::dynamic_pointer_cast<ReferenceColumn>(chunk.get_column(0));
  auto first_referenced_table = first_column->referenced_table();
  auto first_pos_list = first_column->pos_list();

  for (auto i = 1u; i < chunk.col_count(); ++i) {
    const auto column = std::dynamic_pointer_cast<ReferenceColumn>(chunk.get_column(i));

    if (column == nullptr) return false;

    if (first_referenced_table != column->referenced_table()) return false;

    if (first_pos_list != column->pos_list()) return false;
  }

  return true;
}
}  // namespace opossum
