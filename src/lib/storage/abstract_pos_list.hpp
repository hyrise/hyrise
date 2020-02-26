#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class PosList;

class AbstractPosList {
 public:
  template <typename PosListType = const AbstractPosList*, typename DereferenceReturnType = RowID>
  class PosListIterator : public boost::iterator_facade<PosListIterator<PosListType, DereferenceReturnType>, RowID,
                                                        boost::random_access_traversal_tag, DereferenceReturnType> {
   public:
    PosListIterator(PosListType pl_pointer, ChunkOffset pos, ChunkOffset max_size) {
      _pl_pointer = pl_pointer;
      _chunk_offset = pos;
      _max_size = max_size;
    }

    // boost will not use the random_access_iterator_tag if reference_type is not a c++ reference (which it isn't here)
    // we still want to use random access (for binary search, distance, ...)
    typedef std::random_access_iterator_tag iterator_category;

    PosListIterator() = delete;

    void increment() { ++_chunk_offset; }

    void decrement() { --_chunk_offset; }

    void advance(std::ptrdiff_t n) { _chunk_offset += n; }

    bool equal(const PosListIterator& other) const {
      DebugAssert(_pl_pointer == other._pl_pointer, "You trusted me and I failed you.");
      return other._chunk_offset == _chunk_offset;
    }

    std::ptrdiff_t distance_to(const PosListIterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    DereferenceReturnType dereference() const {
      DebugAssert(_chunk_offset < _max_size, "You foool");
      return (*_pl_pointer)[_chunk_offset];
    }

    PosListType _pl_pointer;
    ChunkOffset _chunk_offset;
    ChunkOffset _max_size;
  };

  // public:
  virtual ~AbstractPosList() = default;

  AbstractPosList& operator=(AbstractPosList&& other) = default;

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual RowID operator[](size_t n) const = 0;

  PosListIterator<> begin() const {
    PerformanceWarning("AbstractPosList::begin() called - dereferencing this iterator will be slow.");
    return PosListIterator<>(this, ChunkOffset{0}, static_cast<ChunkOffset>(size()));
  }

  PosListIterator<> end() const {
    PerformanceWarning("AbstractPosList::end() called - dereferencing this iterator will be slow.");
    return PosListIterator<>(this, static_cast<ChunkOffset>(size()), static_cast<ChunkOffset>(size()));
  }

  PosListIterator<> cbegin() const { return begin(); }

  PosListIterator<> cend() const { return end(); }

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  friend bool operator==(const AbstractPosList& lhs, const AbstractPosList& rhs);
};

inline bool operator==(const AbstractPosList& lhs, const AbstractPosList& rhs) {
  PerformanceWarning("Using slow PosList comparison.");

  if (lhs.size() != rhs.size()) {
    return false;
  }

  auto lhsIt = lhs.cbegin();
  auto lhsEnd = lhs.cend();
  auto rhsIt = rhs.cbegin();

  while (lhsIt != lhsEnd) {
    if (!(*lhsIt == *rhsIt)) {
      return false;
    }

    lhsIt++;
    rhsIt++;
  }

  return true;
}

}  // namespace opossum
