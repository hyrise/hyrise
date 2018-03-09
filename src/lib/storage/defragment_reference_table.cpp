#include "defragment_reference_table.hpp"

#include "table.hpp"
#include "chunk.hpp"
#include "base_column.hpp"
#include "reference_column.hpp"

namespace opossum {

std::shared_ptr<Table> merge_adjacent_chunks(const std::shared_ptr<Table>& input_table, const ChunkOffset min_chunk_size, const ChunkOffset max_chunk_size) {
  if (input_table->get_type() != TableType::References) return input_table;

  const auto output_table = Table::create_with_layout_from(input_table);

  for (auto chunk_id_a = ChunkID{0}; chunk_id_a + 1 < input_table->chunk_count(); ++chunk_id_a) {
    const auto chunk_a = input_table->get_chunk(chunk_id_a);

    if (!can_merge_chunks(chunk_a, input_table->get_chunk(chunk_id_a + 1))) continue;

    std::vector<std::shared_ptr<PosList>> merged_pos_lists;
    for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
      merged_pos_lists.emplace_back(std::make_shared<PosList>());
    }

    auto chunk_id_b = chunk_id_a + 1;
    for (; chunk_id_b < input_table->chunk_count(); ++chunk_id_b) {
      const auto chunk_b = input_table->get_chunk(chunk_id_b);

      if (!can_merge_chunks(chunk_a, chunk_b)) continue;


    }
    chunk_id_a = chunk_id_b;

    const auto chunk_b = input_table->get_chunk(chunk_id_a + 1);

    auto merge_chunks = true;

    if (chunk_a->size() >= min_chunk_size && chunk_b->size() >= min_chunk_size) {
      merge_chunks = false;
    }

    if (chunk_a->size() + chunk_b->size() > max_chunk_size)  {
      merge_chunks = false;
    }

    for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
      const auto reference_column_a = std::static_pointer_cast<ReferenceColumn>(chunk_a->get_column(column_id));
      const auto reference_column_b = std::static_pointer_cast<ReferenceColumn>(chunk_b->get_column(column_id));
    }
  }
}

}  // namespace opossum
