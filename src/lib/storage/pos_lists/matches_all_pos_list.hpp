#pragma once

#include "abstract_pos_list.hpp"
#include "storage/chunk.hpp"

namespace opossum {

class MatchesAllPosList : public AbstractPosList {
 public:
  explicit MatchesAllPosList(std::shared_ptr<const Chunk> common_chunk, const ChunkID common_chunk_id)
      : _common_chunk(common_chunk),
        _common_chunk_id(common_chunk_id),
        _common_chunk_size_on_creation(common_chunk->size()) {}

  MatchesAllPosList& operator=(MatchesAllPosList&& other) = default;

  bool references_single_chunk() const final;
  ChunkID common_chunk_id() const final;

  // implemented here to assure compiler optimization without LTO (PosListIterator uses this a lot)
  RowID operator[](size_t n) const final {
    DebugAssert(_common_chunk_id != INVALID_CHUNK_ID, "operator[] called on invalid chunk id");
    return RowID{_common_chunk_id, static_cast<ChunkOffset>(n)};
  }

  bool empty() const final;
  size_t size() const final;
  size_t memory_usage(const MemoryUsageCalculationMode) const final;

  PosListIterator<const MatchesAllPosList*, RowID> begin() const;
  PosListIterator<const MatchesAllPosList*, RowID> end() const;
  PosListIterator<const MatchesAllPosList*, RowID> cbegin() const;
  PosListIterator<const MatchesAllPosList*, RowID> cend() const;

 private:
  std::shared_ptr<const Chunk> _common_chunk;
  ChunkID _common_chunk_id;

  // If tuples are added to the chunk _after_ we create the pos list, we do not want to automatically contain these
  // (MVCC correctness).  To do that, we store the size of the chunk when constructing an object. The end() methods
  // can then use this to give a correct end iterator, even if new values were added to the chunk in between.
  ChunkOffset _common_chunk_size_on_creation;
};

}  // namespace opossum
