#pragma once

#include <unordered_map>
#include <vector>

#include "types.hpp"

namespace opossum {

/**
 * Mapping between chunk offset into a reference column and
 * its dereferenced counter part, i.e., a reference into the
 * referenced value or dictionary column.
 */
struct ChunkOffsetMapping {
  const ChunkOffset into_referencing;  // chunk offset into reference column
  const ChunkOffset into_referenced;   // used to access values in the referenced data column
};

/**
 * @brief list of chunk offset mappings
 */
using ChunkOffsetsList = std::vector<ChunkOffsetMapping>;

using ChunkOffsetsIterator = ChunkOffsetsList::const_iterator;
using ChunkOffsetsByChunkID = std::unordered_map<ChunkID, ChunkOffsetsList>;

/**
 * If skip_null_row_ids is true, any occurrence of NULL_ROW_ID is removed from the result.
 * This is more often than not the desired behaviour. For example during a table scan,
 * any row that contains a null value is automatically excluded from the result because
 * a value compared to NULL is undefined.
 */
ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList &pos_list, bool skip_null_row_ids = true);

}  // namespace opossum
