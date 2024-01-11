#pragma once

#include <type_traits>

#include "storage/abstract_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/variable_string_dictionary_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace hyrise {

template <typename T>
class VariableStringDictionarySegmentIterable
    : public PointAccessibleSegmentIterable<VariableStringDictionarySegmentIterable<T>> {
 public:
  using ValueType = T;
  using Dictionary = pmr_vector<char>;

  explicit VariableStringDictionarySegmentIterable(const VariableStringDictionarySegment<T>& segment)
      : _segment{segment}, _dictionary(segment.dictionary()) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    _segment.access_counter[SegmentAccessCounter::AccessType::Dictionary] += _segment.size();

    resolve_compressed_vector_type(*_segment.attribute_vector(), [this, &functor](const auto& vector) {
      using CompressedVectorIterator = decltype(vector.cbegin());
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      const auto& offset_vector = _segment.offset_vector();

      auto begin =
          Iterator<CompressedVectorIterator, DictionaryIteratorType>{_dictionary->cbegin(), _segment.null_value_id(),
                                                                     vector.cbegin(),       ChunkOffset{0u},
                                                                     offset_vector,         _dictionary};
      auto end = Iterator<CompressedVectorIterator, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(),
          vector.cend(),         static_cast<ChunkOffset>(_segment.size()),
          offset_vector,         _dictionary};

      functor(begin, end);
    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();
    _segment.access_counter[SegmentAccessCounter::AccessType::Dictionary] += position_filter->size();

    resolve_compressed_vector_type(*_segment.attribute_vector(), [this, &functor,
                                                                  &position_filter](const auto& vector) {
      using Decompressor = std::decay_t<decltype(vector.create_decompressor())>;
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      using PosListIteratorType = decltype(position_filter->cbegin());
      auto begin =
          PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{_dictionary->cbegin(),
                                                                                         _segment.null_value_id(),
                                                                                         vector.create_decompressor(),
                                                                                         position_filter->cbegin(),
                                                                                         position_filter->cbegin(),
                                                                                         _segment.offset_vector(),
                                                                                         _dictionary};
      auto end =
          PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{_dictionary->cbegin(),
                                                                                         _segment.null_value_id(),
                                                                                         vector.create_decompressor(),
                                                                                         position_filter->cbegin(),
                                                                                         position_filter->cend(),
                                                                                         _segment.offset_vector(),
                                                                                         _dictionary};
      functor(begin, end);
    });
  }

  size_t _on_size() const {
    return _segment.size();
  }

 private:
  template <typename CompressedVectorIterator, typename DictionaryIteratorType>
  class Iterator
      : public AbstractSegmentIterator<Iterator<CompressedVectorIterator, DictionaryIteratorType>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = VariableStringDictionarySegmentIterable<T>;

    Iterator(DictionaryIteratorType dictionary_begin_it, ValueID null_value_id, CompressedVectorIterator attribute_it,
             ChunkOffset chunk_offset, const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector,
             const std::shared_ptr<const pmr_vector<char>>& dictionary)
        : _dictionary_begin_it{std::move(dictionary_begin_it)},
          _null_value_id{null_value_id},
          _attribute_it{std::move(attribute_it)},
          _chunk_offset{chunk_offset},
          _offset_vector{offset_vector},
          _dictionary{dictionary} {}

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

    bool equal(const Iterator& other) const {
      return _attribute_it == other._attribute_it;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return other._attribute_it - _attribute_it;
    }

    SegmentPosition<T> dereference() const {
      const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) {
        return SegmentPosition<T>{T{}, true, _chunk_offset};
      }

      return SegmentPosition<T>{
          T{VariableStringDictionarySegment<T>::get_string(*_offset_vector, *_dictionary, value_id)}, false,
          _chunk_offset};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    CompressedVectorIterator _attribute_it;
    ChunkOffset _chunk_offset;
    std::shared_ptr<const pmr_vector<uint32_t>> _offset_vector;
    std::shared_ptr<const Dictionary> _dictionary;
  };

  template <typename Decompressor, typename DictionaryIteratorType, typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = VariableStringDictionarySegmentIterable<T>;

    PointAccessIterator(DictionaryIteratorType dictionary_begin_it, const ValueID null_value_id,
                        Decompressor attribute_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it,
                        const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector,
                        const std::shared_ptr<const pmr_vector<char>>& dictionary)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _dictionary_begin_it{std::move(dictionary_begin_it)},
          _null_value_id{null_value_id},
          _attribute_decompressor{std::move(attribute_decompressor)},
          _offset_vector{offset_vector},
          _dictionary{dictionary} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) {
        return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
      }

      return SegmentPosition<T>{
          T{VariableStringDictionarySegment<T>::get_string(*_offset_vector, *_dictionary, ValueID{value_id})}, false,
          chunk_offsets.offset_in_poslist};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    mutable Decompressor _attribute_decompressor;
    std::shared_ptr<const pmr_vector<uint32_t>> _offset_vector;
    std::shared_ptr<const Dictionary> _dictionary;
  };

 private:
  const VariableStringDictionarySegment<pmr_string>& _segment;
  std::shared_ptr<const Dictionary> _dictionary;
};

}  // namespace hyrise
