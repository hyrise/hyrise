#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "uninitialized_vector.hpp"

#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

namespace hyrise {

// A SubPosList is a part of a PosList. In the case of split_pos_list_by_chunk_id, we have multiple SubPosLists, each
// of which references only a single chunk. For each entry in that SubPosList, we need to keep its position in the
// original PosList so that we can reassemble that PosList if needed.
struct SubPosList {
  std::shared_ptr<RowIDPosList> row_ids;
  std::vector<ChunkOffset> original_positions;
};

using PosListsByChunkID = std::vector<SubPosList>;

// Splits a PosList that references multiple chunks into several PosLists that reference only one chunk each.
// The returned structs contains one of those PosList as well as the position of an entry within the original PosList.
// For example, splitting [(1,3), (0,2), (1,2)] gives us two PosLists [(0,2)] and [(1,3), (1,2)] as well as the
// original positions [1] and [0, 2]. These original positions are needed to reassemble the result.
// The returned PosListsByChunkID has a guaranteed size of `number_of_chunks`, but the entries might be empty. The
// template parameter include_null_row_ids decides if NULL_ROW_IDs are skipped or if they are appended to an extra
// SubPosList. When include_null_row_ids is true, PosListsByChunkID has a guaranteed size of `number_of_chunks + 1`.

template <bool include_null_row_ids>
PosListsByChunkID split_pos_list_by_chunk_id(const std::shared_ptr<const AbstractPosList>& input_pos_list,
                                             size_t number_of_chunks);

}  // namespace hyrise
