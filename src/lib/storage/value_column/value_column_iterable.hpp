#pragma once

#include <utility>
#include <vector>

#include "storage/column_iterables.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
class ValueColumnIterable : public PointAccessibleColumnIterable<ValueColumnIterable<T>> {
 public:
  explicit ValueColumnIterable(const ValueColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    if (_column.is_nullable()) {
      auto begin = Iterator{_column.values().cbegin(), _column.values().cbegin(), _column.null_values().cbegin()};
      auto end = Iterator{_column.values().cbegin(), _column.values().cend(), _column.null_values().cend()};
      functor(begin, end);
      return;
    }

    auto begin = NonNullIterator{_column.values().cbegin(), _column.values().cbegin()};
    auto end = NonNullIterator{_column.values().cend(), _column.values().cend()};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ColumnPointAccessPlan& plan, const Functor& functor) const {
    if (_column.is_nullable()) {
      auto begin = PointAccessIterator{_column.values(), _column.null_values(), plan.begin, plan.begin_chunk_offset};
      auto end = PointAccessIterator{_column.values(), _column.null_values(), plan.end, ChunkOffset{}};
      functor(begin, end);
    } else {
      auto begin = NonNullPointAccessIterator{_column.values(), plan.begin, plan.begin_chunk_offset};
      auto end = NonNullPointAccessIterator{_column.values(), plan.end, ChunkOffset{}};
      functor(begin, end);
    }
  }

 private:
  const ValueColumn<T>& _column;

 private:
  class NonNullIterator : public BaseColumnIterator<NonNullIterator, NonNullColumnIteratorValue<T>> {
   public:
    using ValueIterator = typename pmr_concurrent_vector<T>::const_iterator;

   public:
    explicit NonNullIterator(const ValueIterator begin_value_it, const ValueIterator value_it)
        : _value_it{value_it}, _chunk_offset{static_cast<ChunkOffset>(std::distance(begin_value_it, value_it))} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_value_it;
      ++_chunk_offset;
    }
    bool equal(const NonNullIterator& other) const { return _value_it == other._value_it; }

    NonNullColumnIteratorValue<T> dereference() const {
      return NonNullColumnIteratorValue<T>{*_value_it, _chunk_offset};
    }

   private:
    ValueIterator _value_it;
    ChunkOffset _chunk_offset;
  };

  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
   public:
    using ValueIterator = typename pmr_concurrent_vector<T>::const_iterator;
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const ValueIterator begin_value_it, const ValueIterator value_it,
                      const NullValueIterator null_value_it)
        : _value_it(value_it),
          _null_value_it{null_value_it},
          _chunk_offset{static_cast<ChunkOffset>(std::distance(begin_value_it, value_it))} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_value_it;
      ++_null_value_it;
      ++_chunk_offset;
    }

    bool equal(const Iterator& other) const { return _value_it == other._value_it; }

    ColumnIteratorValue<T> dereference() const {
      return ColumnIteratorValue<T>{*_value_it, *_null_value_it, _chunk_offset};
    }

   private:
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
    ChunkOffset _chunk_offset;
  };

  class NonNullPointAccessIterator
      : public BasePointAccessColumnIterator<NonNullPointAccessIterator, ColumnIteratorValue<T>> {
   public:
    using ValueVector = pmr_concurrent_vector<T>;

   public:
    explicit NonNullPointAccessIterator(const ValueVector& values, PosListIterator pos_list_it,
                                        ChunkOffset chunk_offset)
        : BasePointAccessColumnIterator<NonNullPointAccessIterator, ColumnIteratorValue<T>>{pos_list_it, chunk_offset},
          _values{values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnIteratorValue<T>{T{}, true, chunk_offsets.into_referencing};

      return ColumnIteratorValue<T>{_values[chunk_offsets.into_referenced], false, chunk_offsets.into_referencing};
    }

   private:
    const ValueVector& _values;
  };

  class PointAccessIterator : public BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<T>> {
   public:
    using ValueVector = pmr_concurrent_vector<T>;
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit PointAccessIterator(const ValueVector& values, const NullValueVector& null_values,
                                 PosListIterator pos_list_it, ChunkOffset chunk_offset)
        : BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<T>>{pos_list_it, chunk_offset},
          _values{values},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnIteratorValue<T>{T{}, true, chunk_offsets.into_referencing};

      return ColumnIteratorValue<T>{_values[chunk_offsets.into_referenced], _null_values[chunk_offsets.into_referenced],
                                    chunk_offsets.into_referencing};
    }

   private:
    const ValueVector& _values;
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
