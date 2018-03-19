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
  ChunkOffset into_referencing;  // chunk offset into reference column
  ChunkOffset into_referenced;   // used to access values in the referenced data column
};

/**
 * @brief list of chunk offset mappings
 */
using ChunkOffsetsList = std::vector<ChunkOffsetMapping>;

using ChunkOffsetsIterator = ChunkOffsetsList::const_iterator;
using ChunkOffsetsByChunkID = std::unordered_map<ChunkID, ChunkOffsetsList>;

ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList& pos_list);

}  // namespace opossum
