#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include <utility>
#include <vector>

#include "chunk_offset_mapping.hpp"
#include "column_value.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief base class of all iterators used by iterables
 *
 * Why is boost::iterator_core_access a friend class?
 *
 * Boost’s iterator facade needs access to the three methods listed in the example.
 * They could be made public, but that’s not really desirable because they are
 * part of an internal interface and should not be accessed by the user of iterator.
 * This friend class gives the iterator facade access to the methods.
 *
 *
 * Example Usage
 *
 * class Iterator : public BaseIterator<Iterator, Value> {
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   void increment() { ... }
 *   bool equal(const Iterator& other) const { return false; }
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
using BaseIterator = boost::iterator_facade<Derived, Value, boost::forward_traversal_tag, Value>;

/**
 * @brief base class of all referenced iterators used by iterables
 *
 * This iterator should be used whenever a reference column is “dereferenced”,
 * i.e., its underlying value or dictionary column is iterated over.
 * index_into_referenced is the index into the underlying data structure.
 * index_of_referencing is the current index in the reference column.
 *
 *
 * Example Usage
 *
 * class Iterator : public BaseIndexedIterator<Iterator, Value> {
 *  public:
 *   Iterator(const ChunkOffsetIterator& chunk_offset_it)
 *       : BaseIndexedIterator<Iterator, Value>{chunk_offset_it} {}
 *
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   // don’t forget to check if index_into_referenced() == INVALID_CHUNK_OFFSET (i.e. NULL)
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class BaseIndexedIterator : public BaseIterator<Derived, Value> {
 public:
  explicit BaseIndexedIterator(const ChunkOffsetsIterator& chunk_offsets_it) : _chunk_offsets_it{chunk_offsets_it} {}

 protected:
  const ChunkOffsetMapping& chunk_offsets() const { return *_chunk_offsets_it; }

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { ++_chunk_offsets_it; }
  bool equal(const BaseIndexedIterator& other) const { return (_chunk_offsets_it == other._chunk_offsets_it); }

 protected:
  ChunkOffsetsIterator _chunk_offsets_it;
};

}  // namespace opossum
