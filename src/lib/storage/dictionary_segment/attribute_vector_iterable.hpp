#pragma once

#include <utility>

#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

class AttributeVectorIterable : public PointAccessibleSegmentIterable<AttributeVectorIterable> {
 public:
  using ValueType = ValueID;

  explicit AttributeVectorIterable(const BaseCompressedVector& attribute_vector, const ValueID null_value_id)
      : _attribute_vector{attribute_vector}, _null_value_id{null_value_id} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(_attribute_vector, [&](const auto& vector) {
      using ZsIteratorType = decltype(vector.cbegin());

      auto begin = Iterator<ZsIteratorType>{_null_value_id, vector.cbegin(), ChunkOffset{0u}};
      auto end =
          Iterator<ZsIteratorType>{_null_value_id, vector.cend(), static_cast<ChunkOffset>(_attribute_vector.size())};
      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    resolve_compressed_vector_type(_attribute_vector, [&](const auto& vector) {
      auto decompressor = vector.create_decompressor();
      using ZsDecompressorType = std::decay_t<decltype(*decompressor)>;

      auto begin = PointAccessIterator<ZsDecompressorType>{_null_value_id, *decompressor, position_filter->cbegin(),
                                                           position_filter->cbegin()};
      auto end = PointAccessIterator<ZsDecompressorType>{_null_value_id, *decompressor, position_filter->cbegin(),
                                                         position_filter->cend()};
      functor(begin, end);
    });
  }

  size_t _on_size() const { return _attribute_vector.size(); }

 private:
  const BaseCompressedVector& _attribute_vector;
  const ValueID _null_value_id;

 private:
  template <typename ZsIteratorType>
  class Iterator : public BaseSegmentIterator<Iterator<ZsIteratorType>, SegmentPosition<ValueID>> {
   public:
    explicit Iterator(const ValueID null_value_id, ZsIteratorType attribute_it, ChunkOffset chunk_offset)
        : _null_value_id{null_value_id}, _attribute_it{attribute_it}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_attribute_it;
      ++_chunk_offset;
    }

    bool equal(const Iterator& other) const { return _attribute_it == other._attribute_it; }

    void advance(std::ptrdiff_t n) {
      DebugAssert(n >= 0, "Rewinding iterators is not implemented");
      _attribute_it += n;
      _chunk_offset += n;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._attribute_it - _attribute_it; }

    SegmentPosition<ValueID> dereference() const {
      const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto is_null = (value_id == _null_value_id);

      return {value_id, is_null, _chunk_offset};
    }

   private:
    const ValueID _null_value_id;
    ZsIteratorType _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename ZsDecompressorType>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<ZsDecompressorType>, SegmentPosition<ValueID>> {
   public:
    PointAccessIterator(const ValueID null_value_id, ZsDecompressorType& attribute_decompressor,
                        const PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<ZsDecompressorType>,
                                         SegmentPosition<ValueID>>{std::move(position_filter_begin),
                                                                   std::move(position_filter_it)},
          _null_value_id{null_value_id},
          _attribute_decompressor{attribute_decompressor} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<ValueID> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = static_cast<ValueID>(_attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk));
      const auto is_null = (value_id == _null_value_id);

      return {value_id, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const ValueID _null_value_id;
    ZsDecompressorType& _attribute_decompressor;
  };
};

}  // namespace opossum
