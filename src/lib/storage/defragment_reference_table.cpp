#include "defragment_reference_table.hpp"

#include "table.hpp"
#include "chunk.hpp"
#include "base_column.hpp"
#include "reference_column.hpp"

namespace opossum {

std::shared_ptr<Table> defragment_reference_table(const std::shared_ptr<const Table>& reference_table, const ChunkOffset min_chunk_size, const ChunkOffset max_chunk_size) {
  DebugAssert(reference_table->type() == TableType::References, "Can't handle non-reference tables");

  const auto output_table = std::make_shared<Table>(reference_table->column_definitions(), TableType::References, reference_table->max_chunk_size());

  for (auto chunk_id_begin = ChunkID{0}; chunk_id_begin < reference_table->chunk_count(); ) {
    const auto chunk_begin = reference_table->get_chunk(chunk_id_begin);

    auto accumulated_row_count = chunk_begin->size(); // Number of rows in the merged Chunk
    auto chunk_id_end = static_cast<ChunkID>(chunk_id_begin + 1); // ChunkID after the last Chunk to merge
    for (; chunk_id_end < reference_table->chunk_count(); ++chunk_id_end) {
      const auto chunk_end = reference_table->get_chunk(chunk_id_end);

      // If chunk_end is too big itself or merging it with the Chunks above would yield in a Chunk too big, don't
      // include it in the merging
      if (chunk_end->size() > min_chunk_size || accumulated_row_count + chunk_end->size() > max_chunk_size) {
        break;
      }

      // If the Chunk references different tables/columns, don't merge it.
      auto column_layout_matches = true;
      for (auto column_id = ColumnID{0}; column_id < reference_table->column_count(); ++column_id) {
        const auto reference_column_a = std::static_pointer_cast<const ReferenceColumn>(chunk_begin->get_column(column_id));
        const auto reference_column_b = std::static_pointer_cast<const ReferenceColumn>(chunk_end->get_column(column_id));

        if (reference_column_a->referenced_table() != reference_column_b->referenced_table() ||
        reference_column_a->referenced_column_id() != reference_column_b->referenced_column_id()) {
          column_layout_matches = false;
          break;
        }
      }

      if (!column_layout_matches) break;

      accumulated_row_count += chunk_end->size();
    }

    if (chunk_id_begin + 1 == chunk_id_end) {
      output_table->append_chunk(chunk_begin->columns(), chunk_begin->get_allocator(), chunk_begin->access_counter());
      ++chunk_id_begin;
      continue;
    }

    std::vector<std::shared_ptr<const PosList>> merged_pos_lists;
    merged_pos_lists.reserve(reference_table->column_count());

    std::map<std::vector<std::shared_ptr<const PosList>>, std::shared_ptr<const PosList>> merged_pos_list_by_pos_lists;

    for (auto column_id = ColumnID{0}; column_id < reference_table->column_count(); ++column_id) {
      std::vector<std::shared_ptr<const PosList>> column_pos_lists;
      for (auto chunk_id = chunk_id_begin; chunk_id < chunk_id_end; ++chunk_id) {
        const auto chunk = reference_table->get_chunk(chunk_id);
        const auto reference_column = std::static_pointer_cast<const ReferenceColumn>(chunk->get_column(column_id));
        column_pos_lists.emplace_back(reference_column->pos_list());
      }

      auto merged_pos_list_by_pos_lists_iter = merged_pos_list_by_pos_lists.find(column_pos_lists);
      if (merged_pos_list_by_pos_lists_iter == merged_pos_list_by_pos_lists.end()) {
        const auto merged_pos_list = std::make_shared<PosList>();
        merged_pos_list->reserve(accumulated_row_count);

        for (const auto& pos_list : column_pos_lists) {
          std::copy(pos_list->begin(), pos_list->end(), std::back_inserter(*merged_pos_list));
        }

        merged_pos_list_by_pos_lists_iter = merged_pos_list_by_pos_lists.emplace(column_pos_lists, merged_pos_list).first;
      }

      merged_pos_lists.emplace_back(merged_pos_list_by_pos_lists_iter->second);
    }

    ChunkColumns output_columns;

    for (auto column_id = ColumnID{0}; column_id < reference_table->column_count(); ++column_id) {
      const auto reference_column = std::static_pointer_cast<const ReferenceColumn>(chunk_begin->get_column(column_id));

      output_columns.emplace_back(std::make_shared<ReferenceColumn>(reference_column->referenced_table(), reference_column->referenced_column_id(), merged_pos_lists[column_id]));
    }

    output_table->append_chunk(output_columns);

    chunk_id_begin = chunk_id_end;
  }

  return output_table;
}

}  // namespace opossum
