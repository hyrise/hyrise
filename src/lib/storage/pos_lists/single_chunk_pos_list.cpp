#include "single_chunk_pos_list.hpp"

namespace opossum {

bool SingleChunkPosList::empty() const {
  return _offsets.empty();
}

size_t SingleChunkPosList::size() const {
  return _offsets.size();
}

size_t SingleChunkPosList::memory_usage(const MemoryUsageCalculationMode) const {
  return sizeof(this) + size() * sizeof(ChunkOffset);
}

bool SingleChunkPosList::references_single_chunk() const {
  return true;
}

ChunkID SingleChunkPosList::common_chunk_id() const {
  return _chunk_id;
}

std::vector<ChunkOffset>& SingleChunkPosList::get_offsets() {
  return _offsets;
}

AbstractPosList::PosListIterator<SingleChunkPosList, RowID> SingleChunkPosList::begin() const {
  return AbstractPosList::PosListIterator<SingleChunkPosList, RowID>(this, ChunkOffset{0});
}

AbstractPosList::PosListIterator<SingleChunkPosList, RowID> SingleChunkPosList::end() const {
  return AbstractPosList::PosListIterator<SingleChunkPosList, RowID>(this, static_cast<ChunkOffset>(size()));
}

AbstractPosList::PosListIterator<SingleChunkPosList, RowID> SingleChunkPosList::cbegin() const {
  return begin();
}

AbstractPosList::PosListIterator<SingleChunkPosList, RowID> SingleChunkPosList::cend() const {
  return end();
}

}
