#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/column_iterables/column_iterator_values.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief template-free base class of all iterators used by iterables
 *
 * The class allows the JitOperator to keep pointers to differently specialized versions
 * of the iterators in a common data structure.
 */
class JitBaseColumnIterator {};

/**
 * @brief base class of all iterators used by iterables
 *
 * Instantiations of this template are part of the column iterable
 * interface (see column_iterables.hpp) and are implemented
 * in sub-classes of ColumnIterable (see e.g. value_column_iterable.hpp)
 *
 * Value must be a sub-class of AbstractColumnIteratorValue<T>.
 *
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
 * class Iterator : public BaseColumnIterator<Iterator, Value> {
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   void increment() { ... }
 *   bool equal(const Iterator& other) const { return false; }
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class BaseColumnIterator : public boost::iterator_facade<Derived, Value, boost::forward_traversal_tag, Value>,
                           public JitBaseColumnIterator {};

/**
 * @brief base class of all point-access iterators used by iterables
 *
 * This iterator should be used whenever a reference column is “dereferenced”,
 * i.e., its underlying value or dictionary column is iterated over.
 * index_into_referenced is the index into the underlying data structure.
 * index_of_referencing is the current index in the reference column.
 *
 *
 * Example Usage
 *
 * class Iterator : public BasePointAccessColumnIterator<Iterator, Value> {
 *  public:
 *   Iterator(const ChunkOffsetIterator& chunk_offset_it)
 *       : BasePointAccessColumnIterator<Iterator, Value>{chunk_offset_it} {}
 *
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class BasePointAccessColumnIterator : public BaseColumnIterator<Derived, Value> {
 public:
  explicit BasePointAccessColumnIterator(const ChunkOffsetsIterator& chunk_offsets_it)
      : _chunk_offsets_it{chunk_offsets_it} {}

 protected:
  const ChunkOffsetMapping& chunk_offsets() const {
    DebugAssert(_chunk_offsets_it->into_referenced != INVALID_CHUNK_OFFSET,
                "Invalid ChunkOffset, calling code should handle null values");
    return *_chunk_offsets_it;
  }

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { ++_chunk_offsets_it; }
  bool equal(const BasePointAccessColumnIterator& other) const {
    return (_chunk_offsets_it == other._chunk_offsets_it);
  }

 private:
  ChunkOffsetsIterator _chunk_offsets_it;
};

}  // namespace opossum
