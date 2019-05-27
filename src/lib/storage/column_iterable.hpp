#pragma once

#include <type_traits>

#include "types.hpp"
#include "storage/table.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

enum class ColumnIteration {
  Continue, Break
};

struct ColumnIterable {
  std::shared_ptr<const Table> table;
  const ColumnID column_id;

  ColumnIterable(const std::shared_ptr<const Table>& table,
                 const ColumnID column_id);

  template <typename T, typename F>
  void for_each(const F& f, const RowID begin_row_id = RowID{ChunkID{0}, ChunkOffset{0}}) const {
    auto aborted = false;
    const auto chunk_count = table->chunk_count();
    auto chunk_offset = begin_row_id.chunk_offset;

    for (auto chunk_id = begin_row_id.chunk_id; chunk_id < chunk_count; ++chunk_id) {
      const auto& segment = *table->get_chunk(chunk_id)->get_segment(column_id);

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

      if (aborted) {
        break;
      }

      chunk_offset = 0;
    }
  }
};

}  // namespace opossum
