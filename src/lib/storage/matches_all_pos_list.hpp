#pragma once

#include "abstract_pos_list.hpp"
#include "./chunk.hpp"

namespace opossum {

class MatchesAllPosList : public AbstractPosList {
 public:
  explicit MatchesAllPosList(std::shared_ptr<const Chunk> common_chunk, const ChunkID common_chunk_id)
      : _common_chunk(common_chunk), _common_chunk_id(common_chunk_id) { }

  MatchesAllPosList& operator=(MatchesAllPosList&& other) = default;

  MatchesAllPosList() = delete;

  virtual bool references_single_chunk() const override {
    return true;
  }

  virtual ChunkID common_chunk_id() const override {
    DebugAssert(_common_chunk_id != INVALID_CHUNK_ID, "common_chunk_id called on invalid chunk id");
    return _common_chunk_id;
  }

  virtual RowID operator[](size_t n) const override {
    DebugAssert(_common_chunk_id != INVALID_CHUNK_ID, "operator[] called on invalid chunk id");
    return RowID{_common_chunk_id, static_cast<ChunkOffset>(n)};
  }

  virtual bool empty() const override {
    return size() == 0;
  }

  virtual size_t size() const override {
    return _common_chunk->size();
  }

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const override {
    return sizeof *this;
  }

  virtual bool operator==(const MatchesAllPosList* other) const {
    return _common_chunk == other->_common_chunk;
  }

  virtual bool operator==(const AbstractPosList* other) const override {
    // TODO
    return false;
  }

  // template <typename Functor>
  // void for_each(const Functor& functor) const;

 private:
  std::shared_ptr<const Chunk> _common_chunk = nullptr;
  ChunkID _common_chunk_id = INVALID_CHUNK_ID;
};


}  // namespace opossum
