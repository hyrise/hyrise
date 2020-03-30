#pragma once

#include "abstract_pos_list.hpp"
#include "storage/chunk.hpp"

namespace opossum {

class EntireChunkPosList : public AbstractPosList {
 public:
  // The EntireChunkPosList will match the first common_chunk_size elements in the chunk.
  // If you create an EntireChunkPosList for a mutable chunk, make sure to determine that size
  // _before_ doing the checks that all elements should match. This way, you prevent the race
  // condition of an element being added in between doing the checks and getting the size.
  explicit EntireChunkPosList(const ChunkID common_chunk_id, const ChunkOffset common_chunk_size)
      : _common_chunk_id(common_chunk_id), _common_chunk_size(common_chunk_size) {}

  EntireChunkPosList& operator=(EntireChunkPosList&& other) = default;

  bool references_single_chunk() const final;
  ChunkID common_chunk_id() const final;

  // implemented here to assure compiler optimization without LTO (PosListIterator uses this a lot)
  RowID operator[](const size_t index) const final {
    DebugAssert(_common_chunk_id != INVALID_CHUNK_ID, "operator[] called on invalid chunk id");
    return RowID{_common_chunk_id, static_cast<ChunkOffset>(index)};
  }

  bool empty() const final;
  size_t size() const final;
  size_t memory_usage(const MemoryUsageCalculationMode) const final;

  PosListIterator<EntireChunkPosList, RowID> begin() const;
  PosListIterator<EntireChunkPosList, RowID> end() const;
  PosListIterator<EntireChunkPosList, RowID> cbegin() const;
  PosListIterator<EntireChunkPosList, RowID> cend() const;

 private:
  ChunkID _common_chunk_id;

  // If tuples are added to the chunk _after_ we create the pos list, we do not want to automatically contain these
  // (MVCC correctness).  To do that, we store the size of the chunk when constructing an object. The end() methods
  // can then use this to give a correct end iterator, even if new values were added to the chunk in between.
  ChunkOffset _common_chunk_size;
};

}  // namespace opossum
