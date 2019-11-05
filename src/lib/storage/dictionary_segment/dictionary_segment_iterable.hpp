#pragma once

#include <type_traits>

#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T, typename Dictionary>
class DictionarySegmentIterable : public PointAccessibleSegmentIterable<DictionarySegmentIterable<T, Dictionary>> {
 public:
  using ValueType = T;

  explicit DictionarySegmentIterable(const DictionarySegment<T>& segment)
      : _segment{segment}, _dictionary(segment.dictionary()) {}

  explicit DictionarySegmentIterable(const FixedStringDictionarySegment<pmr_string>& segment)
      : _segment{segment}, _dictionary(segment.fixed_string_dictionary()) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_statistics().on_iterator_create(_segment.size());
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using ZsIteratorType = decltype(vector.cbegin());
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      auto begin = Iterator<ZsIteratorType, DictionaryIteratorType>{_dictionary->cbegin(), _segment.null_value_id(),
                                                                    vector.cbegin(), ChunkOffset{0u}, &_segment};
      auto end = Iterator<ZsIteratorType, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.cend(), static_cast<ChunkOffset>(_segment.size()),
          &_segment};

      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    _segment.access_statistics().on_iterator_create_with_pos_list(position_filter->size());
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      auto decompressor = vector.create_decompressor();
      using ZsDecompressorType = std::decay_t<decltype(*decompressor)>;
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      auto begin = PointAccessIterator<ZsDecompressorType, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), std::move(decompressor), position_filter->cbegin(),
          position_filter->cbegin(), &_segment};
      auto end = PointAccessIterator<ZsDecompressorType, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), nullptr, position_filter->cbegin(), position_filter->cend(),
          &_segment};
      functor(begin, end);
    });
  }

  size_t _on_size() const { return _segment.size(); }

  const BaseDictionarySegment& segment() const { return _segment; }

 private:
  template <typename ZsIteratorType, typename DictionaryIteratorType>
  class Iterator : public BaseSegmentIterator<Iterator<ZsIteratorType, DictionaryIteratorType>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    Iterator(DictionaryIteratorType dictionary_begin_it, ValueID null_value_id, ZsIteratorType attribute_it,
             ChunkOffset chunk_offset, const BaseSegment* segment)
        : _dictionary_begin_it{dictionary_begin_it},
          _null_value_id{null_value_id},
          _attribute_it{attribute_it},
          _chunk_offset{chunk_offset},
          _segment{segment} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_attribute_it;
      ++_chunk_offset;
    }

    void decrement() {
      --_attribute_it;
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _attribute_it += n;
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _attribute_it == other._attribute_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._attribute_it - _attribute_it; }

    SegmentPosition<T> dereference() const {
      const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, _chunk_offset};

      _segment->access_statistics().on_iterator_dereference(1);
      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, _chunk_offset};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    ZsIteratorType _attribute_it;
    ChunkOffset _chunk_offset;
    const BaseSegment* _segment;
  };

  template <typename ZsDecompressorType, typename DictionaryIteratorType>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<ZsDecompressorType, DictionaryIteratorType>,
                                              SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    PointAccessIterator(DictionaryIteratorType dictionary_begin_it, const ValueID null_value_id,
                        const std::shared_ptr<ZsDecompressorType>& attribute_decompressor,
                        PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it,
                        const BaseSegment* segment)
        : BasePointAccessSegmentIterator<PointAccessIterator<ZsDecompressorType, DictionaryIteratorType>,
                                         SegmentPosition<T>>{std::move(position_filter_begin),
                                                             std::move(position_filter_it)},
          _dictionary_begin_it{dictionary_begin_it},
          _null_value_id{null_value_id},
          _attribute_decompressor{attribute_decompressor},
          _segment{segment}{}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decompressor->get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};

      _segment->access_statistics().on_iterator_dereference_using_pos_list(1);
      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, chunk_offsets.offset_in_poslist};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    std::shared_ptr<ZsDecompressorType> _attribute_decompressor;
    const BaseSegment* _segment;
  };

 private:
  const BaseDictionarySegment& _segment;
  std::shared_ptr<const Dictionary> _dictionary;
};

template <typename T>
struct is_dictionary_segment_iterable {
  static constexpr auto value = false;
};

template <template <typename T, typename Dictionary> typename Iterable, typename T, typename Dictionary>
struct is_dictionary_segment_iterable<Iterable<T, Dictionary>> {
  static constexpr auto value = std::is_same_v<DictionarySegmentIterable<T, Dictionary>, Iterable<T, Dictionary>>;
};

template <typename T>
inline constexpr bool is_dictionary_segment_iterable_v = is_dictionary_segment_iterable<T>::value;

}  // namespace opossum
