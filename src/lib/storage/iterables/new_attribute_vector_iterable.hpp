#pragma once

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/tuple/tuple.hpp>

#include <utility>
#include <vector>

#include "iterables.hpp"
#include "storage/null_suppression/base_ns_vector.hpp"
#include "storage/null_suppression/ns_utils.hpp"

namespace opossum {

class AttributeVectorIterable : public IndexableIterable<AttributeVectorIterable> {
 public:
  explicit AttributeVectorIterable(const BaseNsVector& attribute_vector, ValueID null_value_id)
      : _attribute_vector{attribute_vector}, _null_value_id{null_value_id} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& f) const {
    with_ns_decoder(_attribute_vector, [&](auto decoder) {
      auto begin = create_iterator(decoder.cbegin(), ChunkOffset{0u});
      auto end = create_iterator(decoder.cend(), static_cast<ChunkOffset>(decoder.size()));
      f(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& f) const {
    with_ns_decoder(_attribute_vector, [&](auto decoder) {
      auto begin = create_indexed_iterator(mapped_chunk_offsets.cbegin(), decoder);
      auto end = create_indexed_iterator(mapped_chunk_offsets.cend(), decoder);
      f(begin, end);
    });
  }

 private:
  const BaseNsVector& _attribute_vector;
  const ValueID _null_value_id;

 private:
  class IteratorLookup {
   public:
    explicit IteratorLookup(ValueID null_value_id) : _null_value_id{null_value_id} {}

    NullableColumnValue<ValueID> operator()(const boost::tuple<uint32_t, ChunkOffset>& tuple) const {
      ValueID value_id{};
      ChunkOffset chunk_offset{};
      boost::tie(value_id, chunk_offset) = tuple;

      const auto is_null = (value_id == _null_value_id);

      return {ValueID{value_id}, is_null, chunk_offset};
    }

   private:
    const ValueID _null_value_id;
  };

  template <typename NsIteratorType>
  using Iterator = boost::transform_iterator<
      IteratorLookup, boost::zip_iterator<boost::tuple<NsIteratorType, boost::counting_iterator<ChunkOffset>>>>;

  template <typename NsIteratorType>
  Iterator<NsIteratorType> create_iterator(NsIteratorType ns_iterator, ChunkOffset chunk_offset) const {
    const auto lookup = IteratorLookup{_null_value_id};
    return Iterator<NsIteratorType>(
        boost::make_tuple(std::move(ns_iterator), boost::make_counting_iterator(chunk_offset)), lookup);
  }

  template <typename NsDecoderType>
  class IndexedIteratorLookup {
   public:
    IndexedIteratorLookup(ValueID null_value_id, NsDecoderType& ns_decoder)
        : _null_value_id{null_value_id}, _ns_decoder{ns_decoder} {}

    NullableColumnValue<ValueID> operator()(const ChunkOffsetMapping& chunk_offsets) const {
      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return {NULL_VALUE_ID, true, chunk_offsets.into_referencing};

      const auto value_id = _ns_decoder.get(chunk_offsets.into_referenced);
      const auto is_null = (value_id == _null_value_id);
      return {ValueID{value_id}, is_null, chunk_offsets.into_referencing};
    }

   private:
    const ValueID _null_value_id;
    NsDecoderType& _ns_decoder;
  };

  template <typename NsDecoderType>
  using IndexedIterator = boost::transform_iterator<IndexedIteratorLookup<NsDecoderType>, ChunkOffsetsIterator>;

  template <typename NsDecoderType>
  IndexedIterator<NsDecoderType> create_indexed_iterator(ChunkOffsetsIterator chunk_offsets_it,
                                                         NsDecoderType& decoder) const {
    const auto lookup = IndexedIteratorLookup{_null_value_id, decoder};
    return IndexedIterator<NsDecoderType>{chunk_offsets_it, lookup};
  }
};

}  // namespace opossum
