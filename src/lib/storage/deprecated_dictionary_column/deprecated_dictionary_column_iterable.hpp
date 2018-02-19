#pragma once

#include <utility>
#include <vector>

#include "storage/column_iterables.hpp"
#include "storage/deprecated_dictionary_column.hpp"

#include "base_attribute_vector.hpp"

namespace opossum {

template <typename T>
class DeprecatedDictionaryColumnIterable : public PointAccessibleColumnIterable<DeprecatedDictionaryColumnIterable<T>> {
 public:
  explicit DeprecatedDictionaryColumnIterable(const DeprecatedDictionaryColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin = Iterator{*_column.dictionary(), *_column.attribute_vector(), 0u};
    auto end = Iterator{*_column.dictionary(), *_column.attribute_vector(), static_cast<ChunkOffset>(_column.size())};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ColumnPointAccessPlan& plan, const Functor& functor) const {
    auto begin =
        PointAccessIterator{*_column.dictionary(), *_column.attribute_vector(), plan.begin, plan.begin_chunk_offset};
    auto end = PointAccessIterator{*_column.dictionary(), *_column.attribute_vector(), plan.end, ChunkOffset{}};
    functor(begin, end);
  }

 private:
  const DeprecatedDictionaryColumn<T>& _column;

 private:
  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
   public:
    using Dictionary = pmr_vector<T>;

   public:
    explicit Iterator(const Dictionary& dictionary, const BaseAttributeVector& attribute_vector,
                      ChunkOffset chunk_offset)
        : _dictionary{dictionary}, _attribute_vector{attribute_vector}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }
    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    ColumnIteratorValue<T> dereference() const {
      const auto value_id = _attribute_vector.get(_chunk_offset);
      const auto is_null = (value_id == NULL_VALUE_ID);

      if (is_null) return ColumnIteratorValue<T>{T{}, true, _chunk_offset};

      return ColumnIteratorValue<T>{_dictionary[value_id], false, _chunk_offset};
    }

   private:
    const Dictionary& _dictionary;
    const BaseAttributeVector& _attribute_vector;
    ChunkOffset _chunk_offset;
  };

  class PointAccessIterator : public BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<T>> {
   public:
    using Dictionary = pmr_vector<T>;

   public:
    explicit PointAccessIterator(const Dictionary& dictionary, const BaseAttributeVector& attribute_vector,
                                 PosListIterator pos_list_it, ChunkOffset chunk_offset)
        : BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<T>>{pos_list_it, chunk_offset},
          _dictionary{dictionary},
          _attribute_vector{attribute_vector} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnIteratorValue<T>{T{}, true, chunk_offsets.into_referencing};

      const auto value_id = _attribute_vector.get(chunk_offsets.into_referenced);
      const auto is_null = (value_id == NULL_VALUE_ID);

      if (is_null) return ColumnIteratorValue<T>{T{}, true, chunk_offsets.into_referencing};

      return ColumnIteratorValue<T>{_dictionary[value_id], false, chunk_offsets.into_referencing};
    }

   private:
    const Dictionary& _dictionary;
    const BaseAttributeVector& _attribute_vector;
  };
};

}  // namespace opossum
