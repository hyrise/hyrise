#pragma once

#include <unordered_map>
#include <vector>

#include "storage/pos_list.hpp"
#include "types.hpp"
#include "utils/uninitialized_vector.hpp"

namespace opossum {

struct SplittedPosList {
	std::shared_ptr<PosList> position_filter;
	std::vector<ChunkOffset> original_positions;
};

using PosListsByChunkID = std::vector<SplittedPosList>;

PosListsByChunkID split_pos_list_by_chunk_id(const std::shared_ptr<const PosList>& input_pos_list,
                                             const size_t number_of_chunks);

}  // namespace opossum
