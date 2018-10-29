#include "split_pos_list_by_chunk_id.hpp"

namespace opossum {

PosListsByChunkID split_pos_list_by_chunk_id(const std::shared_ptr<const PosList>& input_pos_list,
                                             const size_t number_of_chunks) {
  DebugAssert(!input_pos_list->references_single_chunk() || input_pos_list->empty(),
              "No need to split a reference segment that references a single chunk");

  // The input_pos_list references multiple chunks and we actually need to split it. Because we are supposed to return
  // shared_ptr<const PosList>, we first create regular PosLists, add the values to them, and then convert these.

  // Create PosLists and set them as `references_single_chunk`
  auto pos_lists_by_chunk_id = PosListsByChunkID{number_of_chunks};

  for (auto chunk_id = ChunkID{0}; chunk_id < number_of_chunks; ++chunk_id) {
    DebugAssert(chunk_id < number_of_chunks, "Inconsistent number_of_chunks passed");
    auto& mapping = pos_lists_by_chunk_id[chunk_id];
    mapping.row_ids = std::make_shared<PosList>();
    mapping.row_ids->guarantee_single_chunk();
    mapping.row_ids->reserve(input_pos_list->size() / number_of_chunks);
    mapping.original_positions.reserve(input_pos_list->size() / number_of_chunks);
  }

  // Iterate over the input_pos_list and split the entries by chunk_id
  auto original_position = ChunkOffset{0};
  for (const auto& row_id : *input_pos_list) {
    if (row_id.is_null()) {
      original_position++;
      continue;
    }

    auto& mapping = pos_lists_by_chunk_id[row_id.chunk_id];

    mapping.row_ids->emplace_back(row_id);
    mapping.original_positions.emplace_back(original_position++);
  }

  return pos_lists_by_chunk_id;
}

}  // namespace opossum
