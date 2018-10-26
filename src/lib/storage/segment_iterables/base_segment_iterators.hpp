#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include "storage/pos_list.hpp"
#include "storage/segment_iterables/segment_iterator_values.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief template-free base class of all iterators used by iterables
 *
 * The class allows the JitOperatorWrapper to keep pointers to differently specialized versions
 * of the iterators in a common data structure.
 */
class JitBaseSegmentIterator {};

/**
 * @brief base class of all iterators used by iterables
 *
 * Instantiations of this template are part of the segment iterable
 * interface (see segment_iterables/.hpp) and are implemented
 * in sub-classes of SegmentIterable (see e.g. value_segment_iterable.hpp)
 *
 * Value must be a sub-class of AbstractSegmentIteratorValue<T>.
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
 * class Iterator : public BaseSegmentIterator<Iterator, Value> {
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   void increment() { ... }
 *   bool equal(const Iterator& other) const { return false; }
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class BaseSegmentIterator : public boost::iterator_facade<Derived, Value, boost::forward_traversal_tag, Value>,
                            public JitBaseSegmentIterator {};

/**
 * Mapping between chunk offset into a reference segment and
 * its dereferenced counter part, i.e., a reference into the
 * referenced value or dictionary segment.
 */
struct ChunkOffsetMapping {
  ChunkOffset offset_in_poslist;           // chunk offset in reference segment
  ChunkOffset offset_in_referenced_chunk;  // chunk offset in referenced segment
};

/**
 * @brief base class of all point-access iterators used by iterables
 *
 * This iterator should be used whenever a reference segment is “dereferenced”,
 * i.e., its underlying value or dictionary segment is iterated over.
 * The passed position_filter is used to select which of the iterable's values
 * are returned.
 */

template <typename Derived, typename Value>
class BasePointAccessSegmentIterator : public BaseSegmentIterator<Derived, Value> {
 public:
  explicit BasePointAccessSegmentIterator(const PosList::const_iterator position_filter_begin,
                                          PosList::const_iterator position_filter_it)
      : _position_filter_begin{std::move(position_filter_begin)}, _position_filter_it{std::move(position_filter_it)} {}

 protected:
  const ChunkOffsetMapping chunk_offsets() const {
    DebugAssert(_position_filter_it->chunk_offset != INVALID_CHUNK_OFFSET,
                "Invalid ChunkOffset, calling code should handle null values");
    return {static_cast<ChunkOffset>(std::distance(_position_filter_begin, _position_filter_it)),
            _position_filter_it->chunk_offset};
  }

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { ++_position_filter_it; }
  bool equal(const BasePointAccessSegmentIterator& other) const {
    return (_position_filter_it == other._position_filter_it);
  }

 private:
  const PosList::const_iterator _position_filter_begin;
  PosList::const_iterator _position_filter_it;
};

}  // namespace opossum
