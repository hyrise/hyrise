#pragma once

#include <type_traits>

#include "chunk_offset_mapping.hpp"

#include "resolve_type.hpp"

namespace opossum {

/**
 * @brief Resolves the column type of all columns referenced by a ReferenceColumn
 *
 * Partitions the reference column’s position list by chunk ID and
 * resolves then the type of each referenced column,
 * additionally passing on the vector of chunk offsets.
 *
 * The vector of chunk offsets can be passed to an IndexedIterator.
 *
 * This method is a replacement for ReferenceColumn::visit_dererenced and
 * should make the visitor pattern implementation for columns obsolete.
 * In combination with resolve_data_column_type
 *
 * @param func is a generic lambda or similar accepting a reference
 *             to a data column (value, dictionary, not reference)
 *             and a pointer to a ChunkOffsetList. It gets called
 *             multiple times.
 *
 *
 * Example
 *
 *   // Generic Implementation
 *   template <typename ColumnType>
 *   void consume(const ColumnType& column, const ChunkOffsetsList* mapped_chunk_offsets);
 *
 *   // Specialization for dictionary columns
 *   template <typename ColumnDataType>
 *   void consume(const DictionaryColumn<ColumnDataType>& column, const ChunkOffsetsList* mapped_chunk_offsets);
 *
 *   dereference<ColumnDataType>(ref_column, true,
 *                               [&](const auto& typed_data_column, auto mapped_chunk_offsets) {
 *     consume(typed_data_column, mapped_chunk_offsets);
 *   });
 */
template <typename ColumnDataType>
void dereference_column(const ReferenceColumn& ref_column, bool skip_null_row_ids, const Functor& func) {
  auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(*ref_column.pos_list(), skip_null_row_ids);

  // Visit each referenced column
  for (auto& [referenced_chunk_id, mapped_chunk_offsets] : chunk_offsets_by_chunk_id) {
    const auto& chunk = left_column.referenced_table()->get_chunk(referenced_chunk_id);
    auto referenced_column = chunk.get_column(left_column.referenced_column_id());

    resolve_data_column_type<ColumnDataType>(referenced_column, [&](const auto& typed_referenced_column) {
      func(typed_referenced_column, &mapped_chunk_offsets);
    });
  }
}

}  // namespace
