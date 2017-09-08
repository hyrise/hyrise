#pragma once

#include <iterator>
#include <utility>

#include "iterables.hpp"

#include "types.hpp"

namespace opossum {

/**
 * This is an iterable for the null value vector of a value column.
 * It is used for example in the IS NULL implementation of the table scan.
 */
class NullValueVectorIterable : public IndexableIterable<NullValueVectorIterable> {
 public:
  explicit NullValueVectorIterable(const pmr_concurrent_vector<bool>& null_values) : _null_values{null_values} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& f) const {
    auto begin = Iterator{_null_values.cbegin(), _null_values.cbegin()};
    auto end = Iterator{_null_values.cbegin(), _null_values.cend()};
    f(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& f) const {
    auto begin = IndexedIterator{_null_values, mapped_chunk_offsets.cbegin()};
    auto end = IndexedIterator{_null_values, mapped_chunk_offsets.cend()};
    f(begin, end);
  }

 private:
  const pmr_concurrent_vector<bool>& _null_values;

 private:
  class Iterator : public BaseIterator<Iterator, ColumnNullValue> {
   public:
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_null_value_it; }
    bool equal(const Iterator& other) const { return _null_value_it == other._null_value_it; }

    ColumnNullValue dereference() const {
      return ColumnNullValue{*_null_value_it,
                             static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
  };

  class IndexedIterator : public BaseIndexedIterator<IndexedIterator, ColumnNullValue> {
   public:
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit IndexedIterator(const NullValueVector& null_values, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<IndexedIterator, ColumnNullValue>{chunk_offsets_it}, _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnNullValue dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnNullValue{true, chunk_offsets.into_referencing};

      return ColumnNullValue{_null_values[chunk_offsets.into_referenced], chunk_offsets.into_referencing};
    }

   private:
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
