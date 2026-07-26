#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod_utils/hyperloglog.hpp"
#include "operators/aggregate_dyod_utils/ticketing.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

// Cardinality estimation for sizing the fixed-capacity `ConcurrentTicketMap`. The map can grow if an estimate turns out
// to be too small, but only by migrating every entry into a larger table, so the estimates below aim to make that
// fallback unlikely rather than impossible.
//
// Two ingredients are combined:
//   * A hard upper bound that reads no data at all (`group_count_upper_bound`). For dictionary-encoded columns the sum
//     of the per-chunk dictionary sizes bounds a column's distinct values, and the product over the group-by columns
//     bounds the distinct combinations. On low-cardinality group-bys (`GROUP BY l_returnflag, l_linestatus` and the
//     like) this alone is small and exact enough that no data has to be touched.
//   * A HyperLogLog sketch over the key hashes that the grouping pass computes anyway. Unlike the throwaway hash table
//     it replaces, a sketch is 16 KiB regardless of cardinality and merges across threads by element-wise maximum, so
//     several chunks spread over the table can be sampled in parallel instead of only the first one. That matters for
//     tables clustered or sorted on a group-by column, where the first chunk sees a small slice of the domain.
//
// Note what a sketch does and does not buy: it counts what it is shown almost exactly, but extrapolating from sampled
// chunks to the whole table remains a guess. Where full coverage is affordable - a single dictionary-encoded column,
// where feeding the dictionaries costs a fraction of a row-wise pass - the extrapolation is skipped entirely.

// Precision 14: 16 KiB of registers per sketch, ~0.8 % relative standard error.
using CardinalitySketch = HyperLogLog<14>;

// At most this many chunks are read for an estimate, and never more than a `SAMPLE_CHUNK_DIVISOR`-th of the table, so
// estimation stays a small fraction of the grouping pass it precedes.
constexpr auto MAX_SAMPLE_CHUNKS = size_t{4};
constexpr auto SAMPLE_CHUNK_DIVISOR = size_t{8};

// Chunk IDs of an evenly spread sample of `input_table`, always including the first chunk.
std::vector<ChunkID> sample_chunk_ids(const std::shared_ptr<const Table>& input_table);

// Estimated number of distinct group-by keys for the multi-column path.
size_t estimate_group_count_multi_column(const RowFormat& format, const std::vector<ColumnID>& groupby_column_ids,
                                         const std::shared_ptr<const Table>& input_table, size_t max_chunk_size);

// Extrapolates `sampled_groups` distinct groups seen in `sampled_rows` rows to `row_count` rows, assuming the groups
// per row of the sample carry over to the rest of the table. Capped at `row_count`, which bounds the group count.
size_t extrapolate_group_count(size_t sampled_groups, size_t sampled_rows, size_t row_count);

// Estimated number of distinct values of a single non-string group-by column. When every chunk is dictionary-encoded
// and the summed dictionaries fit the scan budget, the sketch sees every value in the table and the result needs no
// extrapolation; otherwise a spread sample of chunks is scanned row-wise and extrapolated. Never returns less than
// one.

template <typename ColumnDataType>
size_t estimate_group_count_single_column(const ColumnID groupby_column_id,
                                          const std::shared_ptr<const Table>& input_table) {
  const auto row_count = input_table->row_count();
  if (row_count == 0) {
    return 1;
  }
  const auto hash_function = std::hash<ColumnDataType>{};

  auto sketch = CardinalitySketch{};
  auto sampled_rows = size_t{0};
  for (const auto chunk_id : sample_chunk_ids(input_table)) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    sampled_rows += chunk->size();
    segment_iterate<ColumnDataType>(*chunk->get_segment(groupby_column_id), [&](const auto& position) {
      if (!position.is_null()) {
        sketch.add(hash_function(position.value()));
      }
    });
  }

  return std::clamp(extrapolate_group_count(sketch.estimate_upper_bound(), sampled_rows, row_count), size_t{1},
                    size_t{row_count});
}

}  // namespace hyrise
