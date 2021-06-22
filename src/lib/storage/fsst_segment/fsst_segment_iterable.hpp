#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/fsst_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

//TODO (anyone): check if we need type T for FSST
template <typename T>
class FSSTSegmentIterable : public PointAccessibleSegmentIterable<FSSTSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit FSSTSegmentIterable(const FSSTSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    // TODO (anyone): Should we increment only when dereferencing?
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();

    auto begin = Iterator{_segment, ChunkOffset{0}};
    auto end = Iterator{_segment, static_cast<ChunkOffset>(_segment.size())};

    //    auto begin = Iterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
    //                                       _segment.end_positions()->cbegin(), ChunkOffset{0}};
    //    auto end = Iterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
    //                        _segment.end_positions()->cend(), static_cast<ChunkOffset>(_segment.size())};

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
    //    using ValueType = T;
    //    using IterableType = FSSTSegmentIterable<T>;
    //    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    //   public:
    // Begin and End Iterator
    //    explicit Iterator(ValueIterator data_it, std::optional<NullValueIterator> null_value_it, ChunkOffset chunk_offset)
    //        : _chunk_offset{chunk_offset}, _data_it{std::move(data_it)}, _null_value_it{std::move(null_value_it)} {}

    explicit Iterator(const FSSTSegment<T>& segment, ChunkOffset chunk_offset)
        : _segment{segment}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
      // TODO(anyone): check if we want to have null values check here
      //      ++_data_it;
      //      if (_null_value_it) ++(*_null_value_it);
    }

    void decrement() {
      --_chunk_offset;
      //      --_data_it;
      //      if (_null_value_it) --(*_null_value_it);
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
      //      _data_it += n;
      //      if (_null_value_it) *_null_value_it += n;
    }

    bool equal(const Iterator& other) const {
      return _chunk_offset == other._chunk_offset;
    }  //TODO (anyone): should we check more than the offset? What about comparing different segments?

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const {
      std::optional<T> value = _segment.get_typed_value(_chunk_offset);
      bool has_value = value.has_value();
      return SegmentPosition<T>(has_value ? value.value() : T{}, !has_value, _chunk_offset);
      //      return SegmentPosition<T>{*_data_it, _null_value_it ? **_null_value_it : false, _chunk_offset};
    }

   private:
    const FSSTSegment<T>& _segment;
    ChunkOffset _chunk_offset;
    //    ValueIterator _data_it;
    //    std::optional<NullValueIterator> _null_value_it;
  };

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                        SegmentPosition<T>, PosListIteratorType> {
   public:
//    using ValueType = T;
//    using IterableType = FSSTSegmentIterable<T>;
//    using DataIteratorType = typename std::vector<T>::const_iterator;
//    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    // Begin Iterator
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
      return SegmentPosition<T>(has_value ? value.value() : T{}, !has_value, chunk_offsets.offset_in_poslist); // Why offset_in_poslist?
//      const auto& value = *(_data_it + chunk_offsets.offset_in_poslist);
//      const auto is_null = _null_value_it && *(*_null_value_it + chunk_offsets.offset_in_referenced_chunk);
//      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const FSSTSegment<T>& _segment;
  };
};

}  // namespace opossum
