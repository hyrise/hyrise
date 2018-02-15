#include "chunk_offset_mapping.hpp"

namespace opossum {

ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList& pos_list, bool skip_null_row_ids) {
  auto chunk_offsets_by_chunk_id = ChunkOffsetsByChunkID{};

  for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < pos_list.size(); ++chunk_offset) {
    const auto row_id = pos_list[chunk_offset];

    if (skip_null_row_ids && row_id == NULL_ROW_ID) continue;

    // Returns ChunkOffsetsList for row_id.chunk_id or creates new
    auto& mapped_chunk_offsets = chunk_offsets_by_chunk_id[row_id.chunk_id];

    mapped_chunk_offsets.push_back({chunk_offset, row_id.chunk_offset});
  }

  return chunk_offsets_by_chunk_id;
}

ChunkOffsetsList to_chunk_offsets_list(const PosList& pos_list, bool skip_null_row_ids) {
  auto chunk_offsets_list = ChunkOffsetsList(pos_list.size());

  [[maybe_unused]] const auto chunk_id = pos_list.front().chunk_id;

  for (ChunkOffset chunk_offset{0u}; chunk_offset < pos_list.size(); ++chunk_offset) {
    const auto row_id = pos_list[chunk_offset];
    DebugAssert(row_id.chunk_id == chunk_id, "Position list must only reference a single chunk.");

    chunk_offsets_list[chunk_offset] = ChunkOffsetMapping{chunk_offset, row_id.chunk_offset};
  }

  return chunk_offsets_list;
}

}  // namespace opossum
