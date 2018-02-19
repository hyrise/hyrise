#pragma once

#include <iterator>
#include <utility>
#include <vector>

#include "storage/column_iterables.hpp"

#include "base_attribute_vector.hpp"

namespace opossum {

class DeprecatedAttributeVectorIterable : public PointAccessibleColumnIterable<DeprecatedAttributeVectorIterable> {
 public:
  explicit DeprecatedAttributeVectorIterable(const BaseAttributeVector& attribute_vector)
      : _attribute_vector{attribute_vector} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& f) const {
    auto begin = Iterator{_attribute_vector, 0u};
    auto end = Iterator{_attribute_vector, static_cast<ChunkOffset>(_attribute_vector.size())};
    f(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ColumnPointAccessPlan& plan, const Functor& f) const {
    auto begin = PointAccessIterator{_attribute_vector, plan.begin, plan.begin_chunk_offset};
    auto end = PointAccessIterator{_attribute_vector, plan.end, ChunkOffset{}};
    f(begin, end);
  }

 private:
  const BaseAttributeVector& _attribute_vector;

 private:
  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<ValueID>> {
   public:
    explicit Iterator(const BaseAttributeVector& attribute_vector, ChunkOffset chunk_offset)
        : _attribute_vector{attribute_vector}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }
    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    ColumnIteratorValue<ValueID> dereference() const {
      const auto value_id = _attribute_vector.get(_chunk_offset);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return ColumnIteratorValue<ValueID>{value_id, is_null, _chunk_offset};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    ChunkOffset _chunk_offset;
  };

  class PointAccessIterator : public BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<ValueID>> {
   public:
    explicit PointAccessIterator(const BaseAttributeVector& attribute_vector, PosListIterator pos_list_it,
                                 ChunkOffset chunk_offset)
        : BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<ValueID>>{pos_list_it, chunk_offset},
          _attribute_vector{attribute_vector} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<ValueID> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnIteratorValue<ValueID>{NULL_VALUE_ID, true, chunk_offsets.into_referencing};

      const auto value_id = _attribute_vector.get(chunk_offsets.into_referenced);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return ColumnIteratorValue<ValueID>{value_id, is_null, chunk_offsets.into_referencing};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
  };
};

}  // namespace opossum
