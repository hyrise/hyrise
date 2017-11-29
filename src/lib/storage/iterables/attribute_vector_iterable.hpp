#pragma once

#include <iterator>
#include <utility>
#include <vector>

#include "iterables.hpp"
#include "storage/base_attribute_vector.hpp"

namespace opossum {

class AttributeVectorIterable : public IndexableIterable<AttributeVectorIterable> {
 public:
  explicit AttributeVectorIterable(const BaseAttributeVector& attribute_vector) : _attribute_vector{attribute_vector} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& f) const {
    auto begin = Iterator{_attribute_vector, 0u};
    auto end = Iterator{_attribute_vector, static_cast<ChunkOffset>(_attribute_vector.size())};
    f(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& f) const {
    auto begin = IndexedIterator{_attribute_vector, mapped_chunk_offsets.cbegin()};
    auto end = IndexedIterator{_attribute_vector, mapped_chunk_offsets.cend()};
    f(begin, end);
  }

 private:
  const BaseAttributeVector& _attribute_vector;

 private:
  class Iterator : public BaseIterator<Iterator, NullableColumnValue<ValueID>> {
   public:
    explicit Iterator(const BaseAttributeVector& attribute_vector, ChunkOffset chunk_offset)
        : _attribute_vector{attribute_vector}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }
    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    NullableColumnValue<ValueID> dereference() const {
      const auto value_id = _attribute_vector.get(_chunk_offset);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return NullableColumnValue<ValueID>{value_id, is_null, _chunk_offset};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    ChunkOffset _chunk_offset;
  };

  class IndexedIterator : public BaseIndexedIterator<IndexedIterator, NullableColumnValue<ValueID>> {
   public:
    explicit IndexedIterator(const BaseAttributeVector& attribute_vector, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<IndexedIterator, NullableColumnValue<ValueID>>{chunk_offsets_it},
          _attribute_vector{attribute_vector} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    NullableColumnValue<ValueID> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return NullableColumnValue<ValueID>{NULL_VALUE_ID, true, chunk_offsets.into_referencing};

      const auto value_id = _attribute_vector.get(chunk_offsets.into_referenced);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return NullableColumnValue<ValueID>{value_id, is_null, chunk_offsets.into_referencing};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
  };
};

}  // namespace opossum
