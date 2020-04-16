#include "entire_chunk_pos_list.hpp"

namespace opossum {

bool EntireChunkPosList::references_single_chunk() const { return true; }

ChunkID EntireChunkPosList::common_chunk_id() const { return _common_chunk_id; }

bool EntireChunkPosList::empty() const { return size() == 0; }

size_t EntireChunkPosList::size() const { return _common_chunk_size; }

size_t EntireChunkPosList::memory_usage(const MemoryUsageCalculationMode) const { return sizeof *this; }

AbstractPosList::PosListIterator<EntireChunkPosList, RowID> EntireChunkPosList::begin() const {
  return PosListIterator<EntireChunkPosList, RowID>(this, ChunkOffset{0});
}

AbstractPosList::PosListIterator<EntireChunkPosList, RowID> EntireChunkPosList::end() const {
  return PosListIterator<EntireChunkPosList, RowID>(this, static_cast<ChunkOffset>(size()));
}

AbstractPosList::PosListIterator<EntireChunkPosList, RowID> EntireChunkPosList::cbegin() const { return begin(); }

AbstractPosList::PosListIterator<EntireChunkPosList, RowID> EntireChunkPosList::cend() const { return end(); }

}  // namespace opossum
