#include "chunk_offset_mapping.hpp"

#include <algorithm>
#include <iterator>

namespace opossum {

ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList& pos_list) {
  auto chunk_offsets_by_chunk_id = ChunkOffsetsByChunkID{};

  for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < pos_list.size(); ++chunk_offset) {
    const auto row_id = pos_list[chunk_offset];
    if (row_id.is_null()) continue;

    // Returns ChunkOffsetsList for row_id.chunk_id or creates new
    auto& mapped_chunk_offsets = chunk_offsets_by_chunk_id[row_id.chunk_id];

    mapped_chunk_offsets.push_back({chunk_offset, row_id.chunk_offset});
  }

  return chunk_offsets_by_chunk_id;
}

std::optional<std::pair<ChunkID, ChunkOffsetsList>> to_chunk_id_and_chunk_offsets_list(const PosList& pos_list) {
  auto chunk_offsets_list = ChunkOffsetsList{};
  chunk_offsets_list.reserve(pos_list.size());

  // Find first row ID that is not NULL
  const auto first_not_null_it = std::find_if(pos_list.cbegin(), pos_list.cend(),
                                              [](const auto& row_id) { return !row_id.is_null(); });

  // If none was found, return empty list
  if (first_not_null_it == pos_list.cend()) return std::nullopt;

  const auto chunk_id = first_not_null_it->chunk_id;

  // Skip any row ID that is NULL.
  const auto first_chunk_offset = static_cast<ChunkOffset>(std::distance(pos_list.cbegin(), first_not_null_it));
  for (auto chunk_offset = first_chunk_offset; chunk_offset < pos_list.size(); ++chunk_offset) {
    const auto row_id = pos_list[chunk_offset];

    // Skip any row ID that is NULL.
    if (row_id.is_null()) continue;

    // Fail if PosList references more than one chunk
    if (row_id.chunk_id != chunk_id) return std::nullopt;

    chunk_offsets_list.push_back({chunk_offset, row_id.chunk_offset});
  }

  return std::pair{chunk_id, chunk_offsets_list};
}

}  // namespace opossum
