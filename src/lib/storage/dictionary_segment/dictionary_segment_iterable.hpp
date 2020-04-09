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
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using ZsIteratorType = decltype(vector.cbegin());
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      auto begin = Iterator<ZsIteratorType, DictionaryIteratorType>{_dictionary->cbegin(), _segment.null_value_id(),
                                                                    vector.cbegin(), ChunkOffset{0u}};
      auto end = Iterator<ZsIteratorType, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.cend(), static_cast<ChunkOffset>(_segment.size())};

      functor(begin, end);
    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using ZsDecompressorType = std::decay_t<decltype(vector.create_decompressor())>;
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      using PosListIteratorType = decltype(position_filter->cbegin());
      auto begin = PointAccessIterator<ZsDecompressorType, DictionaryIteratorType, PosListIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cbegin()};
      auto end = PointAccessIterator<ZsDecompressorType, DictionaryIteratorType, PosListIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cend()};
      functor(begin, end);
    });
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  template <typename ZsIteratorType, typename DictionaryIteratorType>
  class Iterator : public BaseSegmentIterator<Iterator<ZsIteratorType, DictionaryIteratorType>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    Iterator(DictionaryIteratorType dictionary_begin_it, ValueID null_value_id, ZsIteratorType attribute_it,
             ChunkOffset chunk_offset)
        : _dictionary_begin_it{std::move(dictionary_begin_it)},
          _null_value_id{null_value_id},
          _attribute_it{std::move(attribute_it)},
          _chunk_offset{chunk_offset} {}

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

      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, _chunk_offset};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    ZsIteratorType _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename ZsDecompressorType, typename DictionaryIteratorType, typename PosListIteratorType>
  class PointAccessIterator : public BasePointAccessSegmentIterator<
                                  PointAccessIterator<ZsDecompressorType, DictionaryIteratorType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    PointAccessIterator(DictionaryIteratorType dictionary_begin_it, const ValueID null_value_id,
                        ZsDecompressorType attribute_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : BasePointAccessSegmentIterator<
              PointAccessIterator<ZsDecompressorType, DictionaryIteratorType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _dictionary_begin_it{std::move(dictionary_begin_it)},
          _null_value_id{null_value_id},
          _attribute_decompressor{std::move(attribute_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};

      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, chunk_offsets.offset_in_poslist};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    mutable ZsDecompressorType _attribute_decompressor;
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
