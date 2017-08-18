#pragma once

#include <iterator>
#include <utility>
#include <vector>

#include "base_iterables.hpp"
#include "storage/base_attribute_vector.hpp"

namespace opossum {

class AttributeVectorIterable : public BaseIndexableIterable<AttributeVectorIterable> {
 public:
  explicit AttributeVectorIterable(const BaseAttributeVector& attribute_vector,
                                   const ChunkOffsetsList* mapped_chunk_offsets = nullptr)
      : BaseIndexableIterable<AttributeVectorIterable>{mapped_chunk_offsets},
        _attribute_vector{attribute_vector} {}

  template <typename Functor>
  void _on_get_iterators_with_indices(const Functor& f) const {
    auto begin = IndexedIterator{_attribute_vector, _mapped_chunk_offsets->cbegin()};
    auto end = IndexedIterator{_attribute_vector, _mapped_chunk_offsets->cend()};
    f(begin, end);
  }

  template <typename Functor>
  void _on_get_iterators_without_indices(const Functor& f) const {
    auto begin = Iterator{_attribute_vector, 0u};
    auto end = Iterator{_attribute_vector, static_cast<ChunkOffset>(_attribute_vector.size())};
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
    friend class boost::iterator_core_access;

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
    friend class boost::iterator_core_access;

    NullableColumnValue<ValueID> dereference() const {
      const auto value_id = _attribute_vector.get(this->index_into_referenced());
      const auto is_null = (value_id == NULL_VALUE_ID);

      return NullableColumnValue<ValueID>{value_id, is_null, this->index_of_referencing()};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
  };
};

}  // namespace opossum
