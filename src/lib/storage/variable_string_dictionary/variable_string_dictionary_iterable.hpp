#pragma once

#include <type_traits>

#include "storage/abstract_segment.hpp"
#include "storage/variable_string_dictionary_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace hyrise {

template <typename T>
class VariableStringDictionarySegmentIterable : public PointAccessibleSegmentIterable<VariableStringDictionarySegmentIterable<T>> {
 public:
  using ValueType = T;
  using Dictionary = pmr_vector<char>;

  explicit VariableStringDictionarySegmentIterable(const VariableStringDictionarySegment<T>& segment)
      : _segment{segment}, _klotz(segment.klotz()) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    _segment.access_counter[SegmentAccessCounter::AccessType::Dictionary] += _segment.size();

    resolve_compressed_vector_type(*_segment.attribute_vector(), [this, &functor](const auto& vector) {
      using CompressedVectorIterator = decltype(vector.cbegin());
      using DictionaryIteratorType = decltype(_klotz->cbegin());


      auto begin = Iterator<CompressedVectorIterator, DictionaryIteratorType>{
          _klotz->cbegin(), _klotz->size(), vector.cbegin(), ChunkOffset{0u}};
      auto end = Iterator<CompressedVectorIterator, DictionaryIteratorType>{
          _klotz->cbegin(), _klotz->size(), vector.cend(), static_cast<ChunkOffset>(_segment.size())};

      functor(begin, end);
    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();
    _segment.access_counter[SegmentAccessCounter::AccessType::Dictionary] += position_filter->size();

    resolve_compressed_vector_type(*_segment.attribute_vector(), [this, &functor, &position_filter](const auto& vector) {
      using Decompressor = std::decay_t<decltype(vector.create_decompressor())>;
      using DictionaryIteratorType = decltype(_klotz->cbegin());

      using PosListIteratorType = decltype(position_filter->cbegin());
      auto begin = PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{
          _klotz->cbegin(), _klotz->size(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cbegin()};
      auto end = PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{
          _klotz->cbegin(), _klotz->size(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cend()};
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

    Iterator(DictionaryIteratorType dictionary_begin_it, uint32_t null_offset, CompressedVectorIterator attribute_it,
             ChunkOffset chunk_offset)
        : _klotz_begin_it{std::move(dictionary_begin_it)},
          _null_offset{null_offset},
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

    bool equal(const Iterator& other) const {
      return _attribute_it == other._attribute_it;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return other._attribute_it - _attribute_it;
    }

    SegmentPosition<T> dereference() const {
      const auto offset = static_cast<ValueID>(*_attribute_it);
      const auto is_null = (offset == _null_offset);

      if (is_null) {
        return SegmentPosition<T>{T{}, true, _chunk_offset};
      }

      // TODO: Remove &* Hack to get pointer to iterator's data
      return SegmentPosition<T>{T{&*(_klotz_begin_it + offset)}, false, _chunk_offset};
    }

   private:
    DictionaryIteratorType _klotz_begin_it;
    uint32_t _null_offset;
    CompressedVectorIterator _attribute_it;
    ChunkOffset _chunk_offset;
  };

  // TODO: Add offset_vector also here.
  template <typename Decompressor, typename DictionaryIteratorType, typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = VariableStringDictionarySegmentIterable<T>;

    PointAccessIterator(DictionaryIteratorType dictionary_begin_it, const uint32_t null_offset,
                        Decompressor attribute_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _klotz_begin_it{std::move(dictionary_begin_it)},
          _null_offset{null_offset},
          _attribute_decompressor{std::move(attribute_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto offset = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (offset == _null_offset);

      if (is_null) {
        return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
      }

      return SegmentPosition<T>{T{&*(_klotz_begin_it + offset)}, false, chunk_offsets.offset_in_poslist};
    }

   private:
    DictionaryIteratorType _klotz_begin_it;
    uint32_t _null_offset;
    mutable Decompressor _attribute_decompressor;
  };

 private:
  const VariableStringDictionarySegment<pmr_string>& _segment;
  std::shared_ptr<const Dictionary> _klotz;
};

}  // namespace hyrise
