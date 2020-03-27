#include "matches_all_pos_list.hpp"

namespace opossum {

bool MatchesAllPosList::references_single_chunk() const { return true; }

ChunkID MatchesAllPosList::common_chunk_id() const {
  DebugAssert(_common_chunk_id != INVALID_CHUNK_ID, "common_chunk_id called on invalid chunk id");
  return _common_chunk_id;
}

bool MatchesAllPosList::empty() const { return size() == 0; }

size_t MatchesAllPosList::size() const { return _common_chunk_size_on_creation; }

size_t MatchesAllPosList::memory_usage(const MemoryUsageCalculationMode) const { return sizeof *this; }

AbstractPosList::PosListIterator<const MatchesAllPosList*, RowID> MatchesAllPosList::begin() const {
  return PosListIterator<const MatchesAllPosList*, RowID>(this, ChunkOffset{0});
}

AbstractPosList::PosListIterator<const MatchesAllPosList*, RowID> MatchesAllPosList::end() const {
  return PosListIterator<const MatchesAllPosList*, RowID>(this, static_cast<ChunkOffset>(size()));
}

AbstractPosList::PosListIterator<const MatchesAllPosList*, RowID> MatchesAllPosList::cbegin() const { return begin(); }

AbstractPosList::PosListIterator<const MatchesAllPosList*, RowID> MatchesAllPosList::cend() const { return end(); }

}  // namespace opossum
