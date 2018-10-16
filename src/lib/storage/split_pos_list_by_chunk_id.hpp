#pragma once

#include <unordered_map>
#include <vector>

#include "storage/pos_list.hpp"
#include "types.hpp"
#include "utils/uninitialized_vector.hpp"

namespace opossum {

using PosListsByChunkID = std::vector<std::shared_ptr<const PosList>>;

PosListsByChunkID split_pos_list_by_chunk_id(const std::shared_ptr<const PosList>& input_pos_list,
                                             const size_t number_of_chunks);

}  // namespace opossum
