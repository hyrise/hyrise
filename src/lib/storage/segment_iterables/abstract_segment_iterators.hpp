#pragma once

#include <utility>

#include <boost/iterator/iterator_facade.hpp>

#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/segment_iterables/segment_positions.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * @brief base class of all iterators used by iterables
 *
 * Instantiations of this template are part of the segment iterable
 * interface (see segment_iterables/.hpp) and are implemented
 * in sub-classes of SegmentIterable (see e.g. value_segment_iterable.hpp)
 *
 * Value must be a sub-class of AbstractSegmentPosition<T>.
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
 * class Iterator : public AbstractSegmentIterator<Iterator, Value> {
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   void increment() { ... }
 *   bool equal(const Iterator& other) const { return false; }
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class AbstractSegmentIterator
    : public boost::iterator_facade<Derived, Value, boost::random_access_traversal_tag, Value, std::ptrdiff_t> {};

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

template <typename Derived, typename Value, typename PosListIteratorType>
class AbstractPointAccessSegmentIterator : public AbstractSegmentIterator<Derived, Value> {
 public:
  explicit AbstractPointAccessSegmentIterator(PosListIteratorType position_filter_begin,
                                              PosListIteratorType position_filter_it)
      : _position_filter_begin{std::move(position_filter_begin)}, _position_filter_it{std::move(position_filter_it)} {}

 protected:
  const ChunkOffsetMapping chunk_offsets() const {
    DebugAssert(_position_filter_it->chunk_offset != INVALID_CHUNK_OFFSET,
                "Invalid ChunkOffset, calling code should handle null values.");
    return {static_cast<ChunkOffset>(_position_filter_it - _position_filter_begin), _position_filter_it->chunk_offset};
  }

  template <typename F>
  void prefetch(const ChunkOffset offset, const size_t position_filter_size, F functor) const {
    constexpr auto PREFETCH_LENGTH = uint16_t{32};
    if (position_filter_size < PREFETCH_LENGTH) {
      return;
    }

    const auto start_position = std::distance(_position_filter_begin, _position_filter_it);
    if (offset == 0) {
      auto position_filter_it = _position_filter_it + PREFETCH_LENGTH / 2;
      const auto items_to_prefetch = std::min(size_t{PREFETCH_LENGTH / 2}, position_filter_size - start_position);

      for (auto offset_increase = size_t{0}; offset_increase < items_to_prefetch && (start_position + offset_increase) < position_filter_size; ++offset_increase) {
        functor(position_filter_it->chunk_offset);
        ++position_filter_it;
      }
    }

    if (static_cast<size_t>(start_position + PREFETCH_LENGTH) < position_filter_size) {
      const auto position_filter_it = _position_filter_it + PREFETCH_LENGTH;
      DebugAssert(static_cast<size_t>(std::distance(_position_filter_begin, position_filter_it)) < position_filter_size,
                  "Prefetching offset too large.");
      functor(position_filter_it->chunk_offset);
    }
  }

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_position_filter_it;
  }

  void decrement() {
    --_position_filter_it;
  }

  void advance(std::ptrdiff_t n) {
    _position_filter_it += n;
  }

  bool equal(const AbstractPointAccessSegmentIterator& other) const {
    return (_position_filter_it == other._position_filter_it);
  }

  std::ptrdiff_t distance_to(const AbstractPointAccessSegmentIterator& other) const {
    return other._position_filter_it - _position_filter_it;
  }


 private:
  PosListIteratorType _position_filter_begin;
  PosListIteratorType _position_filter_it;
};

}  // namespace hyrise
