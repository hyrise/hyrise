#pragma once

#include <iterator>
#include <utility>

#include "storage/column_iterables.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This is an iterable for the null value vector of a value column.
 * It is used for example in the IS NULL implementation of the table scan.
 */
class NullValueVectorIterable : public PointAccessibleColumnIterable<NullValueVectorIterable> {
 public:
  explicit NullValueVectorIterable(const pmr_concurrent_vector<bool>& null_values) : _null_values{null_values} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin = Iterator{_null_values.cbegin(), _null_values.cbegin()};
    auto end = Iterator{_null_values.cbegin(), _null_values.cend()};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ColumnPointAccessPlan& plan, const Functor& functor) const {
    auto begin = PointAccessIterator{_null_values, plan.begin, plan.begin_chunk_offset};
    auto end = PointAccessIterator{_null_values, plan.end, ChunkOffset{}};
    functor(begin, end);
  }

 private:
  const pmr_concurrent_vector<bool>& _null_values;

 private:
  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorNullValue> {
   public:
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_null_value_it; }
    bool equal(const Iterator& other) const { return _null_value_it == other._null_value_it; }

    ColumnIteratorNullValue dereference() const {
      return ColumnIteratorNullValue{*_null_value_it,
                                     static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
  };

  class PointAccessIterator : public BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorNullValue> {
   public:
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit PointAccessIterator(const NullValueVector& null_values, PosListIterator pos_list_it,
                                 ChunkOffset chunk_offset)
        : BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorNullValue>{pos_list_it, chunk_offset},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorNullValue dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnIteratorNullValue{true, chunk_offsets.into_referencing};

      return ColumnIteratorNullValue{_null_values[chunk_offsets.into_referenced], chunk_offsets.into_referencing};
    }

   private:
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
