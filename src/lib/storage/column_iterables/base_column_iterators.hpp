#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/column_iterables/column_iterator_values.hpp"
#include "types.hpp"

namespace opossum {

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
using BaseColumnIterator = boost::iterator_facade<Derived, Value, boost::forward_traversal_tag, Value>;

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
 *   // don’t forget to check if chunk_offsets().index_into_referenced == INVALID_CHUNK_OFFSET (i.e. NULL)
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class BasePointAccessColumnIterator : public BaseColumnIterator<Derived, Value> {
 public:
  explicit BasePointAccessColumnIterator(PosListIterator pos_list_it, ChunkOffset chunk_offset)
      : _pos_list_it{std::move(pos_list_it)}, _chunk_offset{std::move(chunk_offset)} {}

 protected:
  ChunkOffsetMapping chunk_offsets() const { return {_chunk_offset, _pos_list_it->chunk_offset}; }

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_pos_list_it;
    ++_chunk_offset;
  }

  bool equal(const BasePointAccessColumnIterator& other) const { return (_pos_list_it == other._pos_list_it); }

 private:
  PosListIterator _pos_list_it;
  ChunkOffset _chunk_offset;
};

}  // namespace opossum
