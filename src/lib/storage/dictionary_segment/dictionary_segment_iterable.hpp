#pragma once

#include <type_traits>

#include "storage/abstract_segment.hpp"
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
      : _segment{segment}, _dictionary(*segment.dictionary()), _null_value_id(_segment.null_value_id()) {}

  explicit DictionarySegmentIterable(const FixedStringDictionarySegment<pmr_string>& segment)
      : _segment{segment}, _dictionary(*segment.fixed_string_dictionary()), _null_value_id(_segment.null_value_id()) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& attribute_vector) {
      using CompressedVectorIterator = decltype(attribute_vector.cbegin());

      if (_segment.is_nullable()) {
        auto begin = Iterator<CompressedVectorIterator, true>(&_dictionary, _null_value_id,
                                                  attribute_vector.cbegin(), ChunkOffset{0u});
        auto end = Iterator<CompressedVectorIterator, true>(&_dictionary, _null_value_id,
                                                attribute_vector.cend(),
                                                static_cast<ChunkOffset>(_segment.size()));
        functor(begin, end);
      } else {
        auto begin = Iterator<CompressedVectorIterator, false>(&_dictionary, _null_value_id,
                                                  attribute_vector.cbegin(), ChunkOffset{0u});
        auto end = Iterator<CompressedVectorIterator, false>(&_dictionary, _null_value_id,
                                                attribute_vector.cend(),
                                                static_cast<ChunkOffset>(_segment.size()));
        functor(begin, end);
      }
      // TODO: can we use a templated lambda to better capsulate the duplicated code?

    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& attribute_vector) {
      using Decompressor = std::decay_t<decltype(attribute_vector.create_decompressor())>;
      using PosListIteratorType = decltype(position_filter->cbegin());

      if (_segment.is_nullable()) {
        auto begin = PointAccessIterator<Decompressor, PosListIteratorType, true>(
            &_dictionary, _null_value_id, attribute_vector.create_decompressor(), position_filter->cbegin(),
            position_filter->cbegin());
        auto end = PointAccessIterator<Decompressor, PosListIteratorType, true>(
            &_dictionary, _null_value_id, attribute_vector.create_decompressor(), position_filter->cbegin(),
            position_filter->cend());
        functor(begin, end);
      } else {
        auto begin = PointAccessIterator<Decompressor, PosListIteratorType, false>(
            &_dictionary, _null_value_id, attribute_vector.create_decompressor(), position_filter->cbegin(),
            position_filter->cbegin());
        auto end = PointAccessIterator<Decompressor, PosListIteratorType, false>(
            &_dictionary, _null_value_id, attribute_vector.create_decompressor(), position_filter->cbegin(),
            position_filter->cend());
        functor(begin, end);
      }
    });
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  template <typename CompressedVectorIterator, bool IsNullable>
  class Iterator : public AbstractSegmentIterator<Iterator<CompressedVectorIterator, IsNullable>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    Iterator(const Dictionary* dictionary, const ValueID null_value_id, CompressedVectorIterator attribute_it,
             ChunkOffset chunk_offset)
        : _dictionary{dictionary},
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

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    SegmentPosition<T> dereference() const {
      const auto value_id = static_cast<ValueID>(*_attribute_it);

      if constexpr (IsNullable) {
        if (value_id == _null_value_id) {
          return SegmentPosition<T>{T{}, true, _chunk_offset};
        }
      }

      if constexpr (std::is_same_v<Dictionary, FixedStringVector>) {
        return SegmentPosition<T>{_dictionary->get_string_at(value_id), false, _chunk_offset};
      } else {
        return SegmentPosition<T>{T{(*_dictionary)[value_id]}, false, _chunk_offset};
      }
    }

   private:
    const Dictionary* _dictionary;
    ValueID _null_value_id;
    CompressedVectorIterator _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename Decompressor, typename PosListIteratorType, bool IsNullable>
  class PointAccessIterator
      : public AbstractPointAccessSegmentIterator<PointAccessIterator<Decompressor, PosListIteratorType, IsNullable>,
                                              SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    PointAccessIterator(const Dictionary* dictionary, const ValueID null_value_id,
                        Decompressor attribute_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<Decompressor, PosListIteratorType, IsNullable>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _dictionary{dictionary},
          _null_value_id(null_value_id),
          _attribute_decompressor{std::move(attribute_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);

      if constexpr (IsNullable) {
        if (value_id == _null_value_id) {
          return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
        }
      }

      if constexpr (std::is_same_v<Dictionary, FixedStringVector>) {
        return SegmentPosition<T>{_dictionary->get_string_at(value_id), false, chunk_offsets.offset_in_poslist};
      } else {
        return SegmentPosition<T>{T{(*_dictionary)[value_id]}, false, chunk_offsets.offset_in_poslist};
      }
    }

   private:
    const Dictionary* _dictionary;
    ValueID _null_value_id;
    mutable Decompressor _attribute_decompressor;
  };

 private:
  const BaseDictionarySegment& _segment;
  const Dictionary& _dictionary;
  const ValueID _null_value_id;
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
