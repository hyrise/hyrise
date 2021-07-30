#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/fsst_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class FSSTSegmentIterable : public PointAccessibleSegmentIterable<FSSTSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit FSSTSegmentIterable(const FSSTSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();

    auto begin = Iterator{_segment, ChunkOffset{0}};
    auto end = Iterator{_segment, static_cast<ChunkOffset>(_segment.size())};

    functor(begin, end);
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {

    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = decltype(position_filter->cbegin());
    auto begin = PointAccessIterator<PosListIteratorType>{_segment, position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator<PosListIteratorType>{_segment, position_filter->cbegin(), position_filter->cend()};
    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const FSSTSegment<T>& _segment;

 private:
  class Iterator : public AbstractSegmentIterator<Iterator, SegmentPosition<T>> {
   public:
    explicit Iterator(const FSSTSegment<T>& segment, ChunkOffset chunk_offset)
        : _segment{segment}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
    }

    void decrement() {
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const {
      return _chunk_offset == other._chunk_offset;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const {
      std::optional<T> value = _segment.get_typed_value(_chunk_offset);
      bool has_value = value.has_value();
      return SegmentPosition<T>(has_value ? value.value() : T{}, !has_value, _chunk_offset);
    }

   private:
    const FSSTSegment<T>& _segment;
    ChunkOffset _chunk_offset;
  };

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                        SegmentPosition<T>, PosListIteratorType> {
   public:
    PointAccessIterator(const FSSTSegment<T>& segment,
                        PosListIteratorType position_filter_begin, PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>, SegmentPosition<T>,
                                             PosListIteratorType>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _segment{segment} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      std::optional<T> value = _segment.get_typed_value(chunk_offsets.offset_in_referenced_chunk);
      bool has_value = value.has_value();
      return SegmentPosition<T>(has_value ? value.value() : T{}, !has_value, chunk_offsets.offset_in_poslist);
    }

   private:
    const FSSTSegment<T>& _segment;
  };
};

}  // namespace opossum
