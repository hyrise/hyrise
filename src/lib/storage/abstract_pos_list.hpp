#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class PosList;

class AbstractPosList {
public:
  template <bool modifiable = false>
  class PosListIterator : public boost::iterator_facade<PosListIterator<modifiable>, RowID, boost::random_access_traversal_tag, std::conditional_t<modifiable, RowID&, RowID>> {
public:
  typedef typename std::conditional<modifiable, AbstractPosList*, const AbstractPosList*>::type Typ;
  typedef typename std::conditional<modifiable, RowID&, RowID>::type DereferenceReturnType;
  PosListIterator(Typ pl_pointer, ChunkOffset pos, ChunkOffset max_size) {
    _pl_pointer = pl_pointer;
    _chunk_offset = pos;
    _max_size = max_size;
  }

  void increment() { ++_chunk_offset; }

  void decrement() { --_chunk_offset; }

  void advance(std::ptrdiff_t n) { _chunk_offset += n; }


  bool equal(const PosListIterator other) const {
    // assert + chunk offset compare
    // DebugAssert()
    return (other._chunk_offset == _chunk_offset && other._pl_pointer == _pl_pointer);
  }

  bool equal(const PosListIterator* other) const {
    // assert + chunk offset compare
    // DebugAssert()
    return (other->_chunk_offset == _chunk_offset && other->_pl_pointer == _pl_pointer);
  }

  std::ptrdiff_t distance_to(const PosListIterator other) const {
    return (other._chunk_offset - _chunk_offset);
  }

  std::ptrdiff_t distance_to(const PosListIterator* other) const {
    return (other->_chunk_offset - _chunk_offset);
  }

  DereferenceReturnType dereference() const;

  Typ _pl_pointer;
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

  PosListIterator<false> begin() const {
    return PosListIterator<false>(this, ChunkOffset{0}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  PosListIterator<true> begin();
  PosListIterator<true> end();

  PosListIterator<false> end() const {
    return PosListIterator<false>(this, ChunkOffset{static_cast<ChunkOffset>(size())}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  PosListIterator<false> cbegin() const {
    return PosListIterator<false>(this, ChunkOffset{0}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  PosListIterator<false> cend() const {
    return PosListIterator<false>(this, ChunkOffset{static_cast<ChunkOffset>(size())}, ChunkOffset{static_cast<ChunkOffset>(size())});
  }

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  virtual bool operator==(const AbstractPosList* other) const = 0;

  // template <typename Functor>
  // void for_each(const Functor& functor) const;
};


}  // namespace opossum
