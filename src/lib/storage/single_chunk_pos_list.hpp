#pragma once

#include "pos_lists/abstract_pos_list.hpp"

namespace opossum {

class SingleChunkPosList final : public AbstractPosList {
  public:

  SingleChunkPosList() = delete;

  SingleChunkPosList(ChunkID chunkID) : _chunk_id(chunkID) {}

  virtual bool empty() const override final {
    return _offsets.empty();
  }
  virtual size_t size() const override final {
    return _offsets.size();
  }
  
  virtual size_t memory_usage(const MemoryUsageCalculationMode) const override final {
    return sizeof(this) + size() * sizeof(ChunkOffset);
  }

  virtual bool references_single_chunk() const override final {
    return true;
  }

  virtual ChunkID common_chunk_id() const override final {
    return _chunk_id;
  }

  virtual RowID operator[](size_t n) const override final {
    return RowID{_chunk_id, _offsets[n]};
  }

  std::vector<ChunkOffset>& get_offsets() {
      return _offsets;
  }

  PosListIterator<const SingleChunkPosList*, RowID> begin() const {
    return PosListIterator<const SingleChunkPosList*, RowID>(this, ChunkOffset{0});
  }

  PosListIterator<const SingleChunkPosList*, RowID> end() const {
    return PosListIterator<const SingleChunkPosList*, RowID>(this, static_cast<ChunkOffset>(size()));
  }

  PosListIterator<const SingleChunkPosList*, RowID> cbegin() const {
    return begin();
  }

  PosListIterator<const SingleChunkPosList*, RowID> cend() const {
    return end();
  }

  private:
    std::vector<ChunkOffset> _offsets;
    ChunkID _chunk_id = INVALID_CHUNK_ID;
};

}
