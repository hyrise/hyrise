#pragma once

#include <unordered_map>
#include <vector>

#include "storage/pos_list.hpp"
#include "types.hpp"
#include "uninitialized_vector.hpp"

namespace opossum {

struct SplittedPosList {
  std::shared_ptr<PosList> position_filter;
  std::vector<ChunkOffset> original_positions;
};

using PosListsByChunkID = std::vector<SplittedPosList>;

// Splits a PosList that references multiple chunks into several PosLists that reference only one chunk each.
// The return struct contains one of those PosList as well as the position of an entry within the original PosList.
// For example, splitting [(1,3), (0,2), (1,2)] gives us two PosLists [(0,2)] and [(1,3), (1,2)] as well as the
// original positions [1] and [0, 2]. These original positions are needed to reassemble the result.

PosListsByChunkID split_pos_list_by_chunk_id(const std::shared_ptr<const PosList>& input_pos_list,
                                             const size_t number_of_chunks);

}  // namespace opossum
