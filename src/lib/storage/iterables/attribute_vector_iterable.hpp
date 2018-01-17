#pragma once

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>

#include <utility>

#include "iterables.hpp"
#include "storage/zero_suppression/resolve_zs_vector_type.hpp"

namespace opossum {

class AttributeVectorIterable : public IndexableIterable<AttributeVectorIterable> {
 public:
  explicit AttributeVectorIterable(const BaseZeroSuppressionVector& attribute_vector, const ValueID null_value_id)
      : _attribute_vector{attribute_vector}, _null_value_id{null_value_id} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_zs_vector_type(_attribute_vector, [&](const auto& vector) {
      auto begin = create_iterator(vector.cbegin(), ChunkOffset{0u});
      auto end = create_iterator(vector.cend(), static_cast<ChunkOffset>(vector.size()));
      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    resolve_zs_vector_type(_attribute_vector, [&](const auto& vector) {
      auto decoder = vector.create_decoder();

      auto begin = create_indexed_iterator(mapped_chunk_offsets.cbegin(), *decoder);
      auto end = create_indexed_iterator(mapped_chunk_offsets.cend(), *decoder);
      functor(begin, end);
    });
  }

 private:
  const BaseZeroSuppressionVector& _attribute_vector;
  const ValueID _null_value_id;

 private:
  class IteratorLookup {
   public:
    IteratorLookup() = default;
    explicit IteratorLookup(ValueID null_value_id) : _null_value_id{null_value_id} {}

    NullableColumnValue<ValueID> operator()(const boost::tuple<uint32_t, ChunkOffset>& tuple) const {
      ValueID value_id{};
      ChunkOffset chunk_offset{};
      boost::tie(value_id, chunk_offset) = tuple;

      const auto is_null = (value_id == _null_value_id);
      if (is_null) return {value_id, true, chunk_offset};

      return {value_id, false, chunk_offset};
    }

   private:
    const ValueID _null_value_id;
  };

  template <typename ZsIteratorType>
  using Iterator = boost::transform_iterator<
      IteratorLookup, boost::zip_iterator<boost::tuple<ZsIteratorType, boost::counting_iterator<ChunkOffset>>>>;

  template <typename ZsIteratorType>
  Iterator<ZsIteratorType> create_iterator(ZsIteratorType ns_iterator, ChunkOffset chunk_offset) const {
    const auto lookup = IteratorLookup{_null_value_id};
    return Iterator<ZsIteratorType>(
        boost::make_tuple(std::move(ns_iterator), boost::make_counting_iterator(chunk_offset)), lookup);
  }

  template <typename ZsDecoderType>
  class IndexedIteratorLookup {
   public:
    IndexedIteratorLookup(ValueID null_value_id, ZsDecoderType& ns_decoder)
        : _null_value_id{null_value_id}, _ns_decoder{ns_decoder} {}

    NullableColumnValue<ValueID> operator()(const ChunkOffsetMapping& chunk_offsets) const {
      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return {ValueID{}, true, chunk_offsets.into_referencing};

      const auto value_id = ValueID{_ns_decoder.get(chunk_offsets.into_referenced)};
      const auto is_null = (value_id == _null_value_id);
      if (is_null) return {value_id, true, chunk_offsets.into_referencing};

      return {value_id, false, chunk_offsets.into_referencing};
    }

   private:
    const ValueID _null_value_id;
    ZsDecoderType& _ns_decoder;
  };

  template <typename ZsDecoderType>
  using IndexedIterator = boost::transform_iterator<IndexedIteratorLookup<ZsDecoderType>, ChunkOffsetsIterator>;

  template <typename ZsDecoderType>
  IndexedIterator<ZsDecoderType> create_indexed_iterator(ChunkOffsetsIterator chunk_offsets_it,
                                                         ZsDecoderType& decoder) const {
    const auto lookup = IndexedIteratorLookup<ZsDecoderType>{_null_value_id, decoder};
    return IndexedIterator<ZsDecoderType>{chunk_offsets_it, lookup};
  }
};

}  // namespace opossum
