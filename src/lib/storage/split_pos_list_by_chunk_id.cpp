#include "split_pos_list_by_chunk_id.hpp"

namespace opossum {

PosListsByChunkID split_pos_list_by_chunk_id(const std::shared_ptr<const PosList>& input_pos_list,
                                             const size_t number_of_chunks) {
  if (input_pos_list->references_single_chunk()) {
    // The easy case: We are already guaranteed that the input_pos_list has a single chunk, so we just need to build
    // a vector of the correct size, put the input_pos_list in, and return it.

    auto pos_lists_by_chunk_id = std::vector<std::shared_ptr<const PosList>>{number_of_chunks};

    const auto chunk_id = (*input_pos_list)[0].chunk_id;
    pos_lists_by_chunk_id[chunk_id] = input_pos_list;

    return pos_lists_by_chunk_id;
  } else {
    // The complicated case: The input_pos_list references multiple chunks and we actually need to split it. Because we
    // are supposed to return shared_ptr<const PosList>, we first create regular PosLists, add the values to them, and
    // then convert these.

    // Create PosLists and set them as `references_single_chunk`
    auto pos_lists_by_chunk_id = std::vector<std::shared_ptr<PosList>>{number_of_chunks};
    for (auto chunk_id = ChunkID{0}; chunk_id < number_of_chunks; ++chunk_id) {
      pos_lists_by_chunk_id[chunk_id] = std::make_shared<PosList>();
      pos_lists_by_chunk_id[chunk_id]->guarantee_single_chunk();
    }

    // Assume uniform distribution and allocate some memory
    for (auto& output_pos_list : pos_lists_by_chunk_id) {
      output_pos_list->reserve(input_pos_list->size() / number_of_chunks);
    }

    // Iterate over the input_pos_list and split the entries by chunk_id
    for (const auto& row_id : *input_pos_list) {
      if (row_id.is_null()) continue;

      auto& output_pos_list = pos_lists_by_chunk_id[row_id.chunk_id];

      output_pos_list->emplace_back(row_id);
    }

    // Convert the shared_ptr<PosList> to shared_ptr<const PosList> :/
    return PosListsByChunkID{pos_lists_by_chunk_id.begin(), pos_lists_by_chunk_id.end()};
  }
}

}  // namespace opossum
