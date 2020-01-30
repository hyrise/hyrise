#pragma once

#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// class AbstractPosList {
// // public:
// //   virtual ~AbstractPosList() = default;
// // virtual RowID operator[](size_t n) const = 0; 
// }

// template<typename Functor>
// void resolve_pos_list_type(const AbstractPosList& untyped_pos_list, const Functor& func);



class AbstractPosList {

  class PosListIterator : public boost::iterator_facade<PosListIterator, RowID, boost::random_access_traversal_tag> {
public:
  PosListIterator(const AbstractPosList* pl_pointer, ChunkOffset pos) {
    _pl_pointer = pl_pointer;
    chunk_offset = pos;
  }

  void increment() { ++chunk_offset; }

  void decrement() { --chunk_offset; }

  void advance(std::ptrdiff_t n) { chunk_offset += n; }

  /**
   * Although `other` could have a different type, it is practically impossible,
   * since AnySegmentIterator is only used within AnySegmentIterable.
   */
  bool equal(const PosListIterator* other) const {
    // assert + chunk offset compare
    // DebugAssert()
    return false;
    // const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    // return _iterator == casted_other->_iterator;
  }

  std::ptrdiff_t distance_to(const PosListIterator* other) const {
    // const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    // return casted_other->_iterator - _iterator;
    return -1;
  }

  RowID dereference() const {
    return (*_pl_pointer)[chunk_offset];
    // const auto value = *_iterator;
    // return {value.value(), value.is_null(), value.chunk_offset()};

  }


  const AbstractPosList* _pl_pointer;
  ChunkOffset chunk_offset;
};

 public:
  virtual ~AbstractPosList() = default;

  AbstractPosList& operator=(AbstractPosList&& other) = default;

  // Returns whether it is guaranteed that the PosList references a single ChunkID.
  // However, it may be false even if this is the case.
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  virtual RowID operator[](size_t n) const = 0;

  PosListIterator begin() const {
    return PosListIterator(this, ChunkOffset{0});
  }

  PosListIterator end() const {
    return PosListIterator(this, ChunkOffset{4});
  }

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual size_t memory_usage(const MemoryUsageCalculationMode) const = 0;

  virtual bool operator==(const AbstractPosList& other) const = 0;

  // template <typename Functor>
  // void for_each(const Functor& functor) const;
};

template <typename PosListType>
extern typename PosListType::const_iterator make_pos_list_begin_iterator(PosListType& pos_list);

template <typename PosListType>
extern typename PosListType::const_iterator make_pos_list_end_iterator(PosListType& pos_list);

template <typename PosListType>
extern typename PosListType::iterator make_pos_list_begin_iterator_nc(PosListType& pos_list);

template <typename PosListType>
extern typename PosListType::iterator make_pos_list_end_iterator_nc(PosListType& pos_list);

}  // namespace opossum
