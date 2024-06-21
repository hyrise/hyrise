#pragma once

#include <memory>
#include <type_traits>
#include <utility>

#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace hyrise {

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
    _segment.access_counter[SegmentAccessCounter::AccessType::Dictionary] += _segment.size();

    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using CompressedVectorIterator = decltype(vector.cbegin());
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      auto begin = Iterator<CompressedVectorIterator, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.cbegin(), ChunkOffset{0u}};
      auto end = Iterator<CompressedVectorIterator, DictionaryIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.cend(), static_cast<ChunkOffset>(_segment.size())};

      functor(begin, end);
    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    const auto position_filter_size = position_filter->size();
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter_size;
    _segment.access_counter[SegmentAccessCounter::AccessType::Dictionary] += position_filter_size;

    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using Decompressor = std::decay_t<decltype(vector.create_decompressor())>;
      using DictionaryIteratorType = decltype(_dictionary->cbegin());

      using PosListIteratorType = decltype(position_filter->cbegin());
      auto begin = PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cbegin(), position_filter_size};
      auto end = PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>{
          _dictionary->cbegin(), _segment.null_value_id(), vector.create_decompressor(), position_filter->cbegin(),
          position_filter->cend(), position_filter_size};
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
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    Iterator(DictionaryIteratorType dictionary_begin_it, ValueID null_value_id, CompressedVectorIterator attribute_it,
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

      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, _chunk_offset};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    CompressedVectorIterator _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename Decompressor, typename DictionaryIteratorType, typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<
                                  PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>,
                                  SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    PointAccessIterator(DictionaryIteratorType dictionary_begin_it, const ValueID null_value_id,
                        Decompressor attribute_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it, size_t position_filter_size)
        : AbstractPointAccessSegmentIterator<
              PointAccessIterator<Decompressor, DictionaryIteratorType, PosListIteratorType>, SegmentPosition<T>,
              PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
          _dictionary_begin_it{std::move(dictionary_begin_it)},
          _null_value_id{null_value_id},
          _attribute_decompressor{std::move(attribute_decompressor)},
          _position_filter_size{position_filter_size} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      // TODO(Martin): Find a nice way to check for FixedWidth ...
      if constexpr (std::is_same_v<Decompressor, FixedWidthIntegerDecompressor<uint8_t>> ||
                    std::is_same_v<Decompressor, FixedWidthIntegerDecompressor<uint16_t>> ||
                    std::is_same_v<Decompressor, FixedWidthIntegerDecompressor<uint32_t>>) {
        // For now, we only support prefetching for FixedWidthInteger compression. For bit-packed vectors, getting the
        // actual address is a bit more tricky.
        this->prefetch(chunk_offsets.offset_in_poslist, _position_filter_size, [&, this](const ChunkOffset offset) {
          // std::cerr << "Prefetching address " << _attribute_decompressor.address_of(offset) << "\n";
          __builtin_prefetch(static_cast<const void*>(_attribute_decompressor.address_of(offset)), /* read */ 0, /* locality hint */ 0);
          DebugAssert(static_cast<uint32_t>(*_attribute_decompressor.address_of(offset)) == _attribute_decompressor.get(offset), "Mismatch");
          // std::cerr << "Values: " << static_cast<uint32_t>(*_attribute_decompressor.address_of(offset)) << " & " << _attribute_decompressor.get(offset) << std::endl;
        });

        // std::cerr << "Accessing address " << _attribute_decompressor.address_of(chunk_offsets.offset_in_referenced_chunk) << "\n";
      }

      const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) {
        return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};
      }

      return SegmentPosition<T>{T{*(_dictionary_begin_it + value_id)}, false, chunk_offsets.offset_in_poslist};
    }

   private:
    DictionaryIteratorType _dictionary_begin_it;
    ValueID _null_value_id;
    mutable Decompressor _attribute_decompressor;
    size_t _position_filter_size;
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

}  // namespace hyrise
