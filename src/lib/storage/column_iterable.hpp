#pragma once

#include <type_traits>

#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

enum class ColumnIteration { Continue, Break };

struct ColumnIterable {
  std::shared_ptr<const Table> table;
  const ColumnID column_id;

  ColumnIterable(const std::shared_ptr<const Table>& table, const ColumnID column_id);

  template <typename T, typename F>
  RowID for_each(const F& f, const RowID begin_row_id = RowID{ChunkID{0}, ChunkOffset{0}}) const {
    auto aborted = false;
    const auto chunk_count = table->chunk_count();
    auto chunk_offset = begin_row_id.chunk_offset;

    auto chunk_id = begin_row_id.chunk_id;
    for (; chunk_id < chunk_count;) {
      const auto chunk = table->get_chunk(chunk_id);
      const auto& segment = *chunk->get_segment(column_id);

      segment_with_iterators<T>(segment, [&](auto iter, auto end) {
        using SegmentPositionType = std::decay_t<decltype(*iter)>;

        if (chunk_id == begin_row_id.chunk_id) {
          iter.advance(begin_row_id.chunk_offset);
        }

        while (iter != end && !aborted) {
          if constexpr (std::is_same_v<std::invoke_result_t<F, SegmentPositionType, RowID>, void>) {
            f(*iter, RowID{chunk_id, chunk_offset});
          } else {
            if (f(*iter, RowID{chunk_id, chunk_offset}) == ColumnIteration::Break) {
              aborted = true;
            }
          }
          ++chunk_offset;
          ++iter;
        }
      });

      if (chunk_offset == chunk->size()) {
        ++chunk_id;
        chunk_offset = 0;
      }

      if (aborted) {
        break;
      }
    }

    return {chunk_id, chunk_offset};
  }
};

}  // namespace opossum
