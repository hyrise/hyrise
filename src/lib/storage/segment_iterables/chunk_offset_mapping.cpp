#include "chunk_offset_mapping.hpp"

namespace opossum {

ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList& pos_list, const size_t number_of_chunks) {
  auto chunk_offsets_by_chunk_id = ChunkOffsetsByChunkID{number_of_chunks};
  for (auto& chunk_offsets : chunk_offsets_by_chunk_id) {
    chunk_offsets.reserve(pos_list.size() / number_of_chunks);
  }

  for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < pos_list.size(); ++chunk_offset) {
    const auto row_id = pos_list[chunk_offset];
    if (row_id.is_null()) continue;

    // Returns ChunkOffsetsList for row_id.chunk_id or creates new
    auto& mapped_chunk_offsets = chunk_offsets_by_chunk_id[row_id.chunk_id];

    mapped_chunk_offsets.emplace_back(chunk_offset, row_id.chunk_offset);
  }

  return chunk_offsets_by_chunk_id;
}

}  // namespace opossum
