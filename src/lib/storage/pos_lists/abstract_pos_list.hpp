#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class RowIDPosList;

class AbstractPosList : private Noncopyable {
 public:
  template <typename PosListType = AbstractPosList, typename DereferenceReturnType = RowID>
  class PosListIterator : public boost::iterator_facade<PosListIterator<PosListType, DereferenceReturnType>, RowID,
                                                        boost::random_access_traversal_tag, DereferenceReturnType> {
   public:
    PosListIterator(const PosListType* pos_list, const ChunkOffset offset)
        : _pos_list(pos_list), _chunk_offset(offset) {}

    void increment() { ++_chunk_offset; }

    void decrement() { --_chunk_offset; }

    void advance(std::ptrdiff_t n) { _chunk_offset += n; }

    bool equal(const PosListIterator& other) const {
      DebugAssert(_pos_list == other._pos_list, "PosListIterator compared to iterator on different PosList instance");
      return other._chunk_offset == _chunk_offset;
    }

    std::ptrdiff_t distance_to(const PosListIterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    DereferenceReturnType dereference() const {
      DebugAssert(_chunk_offset < _pos_list->size(), "past-the-end PosListIterator dereferenced");
      return (*_pos_list)[_chunk_offset];
    }

    const PosListType* _pos_list;
    ChunkOffset _chunk_offset;
  };

  virtual ~AbstractPosList() = default;

  AbstractPosList& operator=(AbstractPosList&& other) = default;

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual RowID operator[](size_t n) const = 0;

  PosListIterator<> begin() const;
  PosListIterator<> end() const;
  PosListIterator<> cbegin() const;
  PosListIterator<> cend() const;

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  friend bool operator==(const AbstractPosList& lhs, const AbstractPosList& rhs);
};

inline bool operator==(const AbstractPosList& lhs, const AbstractPosList& rhs) {
  PerformanceWarning("Using slow PosList comparison.");
  return std::equal(lhs.cbegin(), lhs.cend(), rhs.cbegin(), rhs.cend());
}

}  // namespace opossum
