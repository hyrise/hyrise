#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"

#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T, typename Dictionary>
class DictionarySegmentIterable : public PointAccessibleSegmentIterable<DictionarySegmentIterable<T, Dictionary>> {
 public:
  using ValueType = T;

  explicit DictionarySegmentIterable(const DictionarySegment<T>& segment)
      : _segment{segment}, _dictionary(segment.dictionary()) {}

  explicit DictionarySegmentIterable(const FixedStringDictionarySegment<std::string>& segment)
      : _segment{segment}, _dictionary(segment.fixed_string_dictionary()) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      using ZsIteratorType = decltype(vector.cbegin());

      auto begin = Iterator<ZsIteratorType>{*_dictionary, _segment.null_value_id(), vector.cbegin(), ChunkOffset{0u}};
      auto end = Iterator<ZsIteratorType>{*_dictionary, _segment.null_value_id(), vector.cend(),
                                          static_cast<ChunkOffset>(_segment.size())};
      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    resolve_compressed_vector_type(*_segment.attribute_vector(), [&](const auto& vector) {
      auto decompressor = vector.create_decompressor();
      using ZsDecompressorType = std::decay_t<decltype(*decompressor)>;

      auto begin = PointAccessIterator<ZsDecompressorType>{*_dictionary, _segment.null_value_id(), *decompressor,
                                                           position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator<ZsDecompressorType>{*_dictionary, _segment.null_value_id(), *decompressor,
                                                         position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    });
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const BaseDictionarySegment& _segment;
  std::shared_ptr<const Dictionary> _dictionary;

 private:
  template <typename ZsIteratorType>
  class Iterator : public BaseSegmentIterator<Iterator<ZsIteratorType>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    explicit Iterator(const Dictionary& dictionary, const ValueID null_value_id, const ZsIteratorType attribute_it,
                      ChunkOffset chunk_offset)
        : _dictionary{dictionary},
          _null_value_id{null_value_id},
          _attribute_it{attribute_it},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_attribute_it;
      ++_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      DebugAssert(n >= 0, "Rewinding iterators is not implemented");
      _attribute_it += n;
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _attribute_it == other._attribute_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._attribute_it - _attribute_it; }

    SegmentPosition<T> dereference() const {
      const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, _chunk_offset};

      if constexpr (std::is_same_v<Dictionary, FixedStringVector>) {
        return SegmentPosition<T>{_dictionary.get_string_at(value_id), false, _chunk_offset};
      } else {
        return SegmentPosition<T>{_dictionary[value_id], false, _chunk_offset};
      }
    }

   private:
    const Dictionary& _dictionary;
    const ValueID _null_value_id;
    ZsIteratorType _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename ZsDecompressorType>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<ZsDecompressorType>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = DictionarySegmentIterable<T, Dictionary>;

    PointAccessIterator(const Dictionary& dictionary, const ValueID null_value_id,
                        ZsDecompressorType& attribute_decompressor, const PosList::const_iterator position_filter_begin,
                        PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<ZsDecompressorType>,
                                         SegmentPosition<T>>{std::move(position_filter_begin),
                                                             std::move(position_filter_it)},
          _dictionary{dictionary},
          _null_value_id{null_value_id},
          _attribute_decompressor{attribute_decompressor} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return SegmentPosition<T>{T{}, true, chunk_offsets.offset_in_poslist};

      if constexpr (std::is_same_v<Dictionary, FixedStringVector>) {
        return SegmentPosition<T>{_dictionary.get_string_at(value_id), false, chunk_offsets.offset_in_poslist};
      } else {
        return SegmentPosition<T>{_dictionary[value_id], false, chunk_offsets.offset_in_poslist};
      }
    }

   private:
    const Dictionary& _dictionary;
    const ValueID _null_value_id;
    ZsDecompressorType& _attribute_decompressor;
  };
};

}  // namespace opossum
