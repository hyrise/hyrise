#include "chunk_offset_mapping.hpp"

#include <utility>

namespace opossum {

ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList& pos_list, PosListType pos_list_type,
                                                 bool skip_null_row_ids) {
  if (pos_list.empty()) return {};

  if (pos_list_type == PosListType::SingleChunk) {
    const auto row_id = pos_list.front();
    const auto chunk_id = row_id.chunk_id;
    Assert(!(row_id == NULL_ROW_ID), "Assumption: first element in pos list is not NULL.");

    return {{chunk_id, to_chunk_offsets_list(pos_list)}};
  }

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

ChunkOffsetsList to_chunk_offsets_list(const PosList& pos_list) {
  if (pos_list.empty()) return {};

  auto chunk_offsets_list = ChunkOffsetsList(pos_list.size());

  [[maybe_unused]] const auto first_chunk_id = pos_list.front().chunk_id;

  for (ChunkOffset chunk_offset{0u}; chunk_offset < pos_list.size(); ++chunk_offset) {
    const auto row_id = pos_list[chunk_offset];
    DebugAssert(row_id.chunk_id == first_chunk_id, "Position list must only reference a single chunk.");

    chunk_offsets_list[chunk_offset] = ChunkOffsetMapping{chunk_offset, row_id.chunk_offset};
  }

  return chunk_offsets_list;
}

}  // namespace opossum
