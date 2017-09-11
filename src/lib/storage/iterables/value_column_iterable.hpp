#pragma once

#include <utility>
#include <vector>

#include "iterables.hpp"

#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
class ValueColumnIterable : public IndexableIterable<ValueColumnIterable<T>> {
 public:
  explicit ValueColumnIterable(const ValueColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& f) const {
    if (_column.is_nullable()) {
      auto begin =
          NullableIterator{_column.values().cbegin(), _column.values().cbegin(), _column.null_values().cbegin()};
      auto end = NullableIterator{_column.values().cbegin(), _column.values().cend(), _column.null_values().cend()};
      f(begin, end);
      return;
    }

    auto begin = Iterator{_column.values().cbegin(), _column.values().cbegin()};
    auto end = Iterator{_column.values().cend(), _column.values().cend()};
    f(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& f) const {
    if (_column.is_nullable()) {
      auto begin = NullableIndexedIterator{_column.values(), _column.null_values(), mapped_chunk_offsets.cbegin()};
      auto end = NullableIndexedIterator{_column.values(), _column.null_values(), mapped_chunk_offsets.cend()};
      f(begin, end);
    } else {
      auto begin = IndexedIterator{_column.values(), mapped_chunk_offsets.cbegin()};
      auto end = IndexedIterator{_column.values(), mapped_chunk_offsets.cend()};
      f(begin, end);
    }
  }

 private:
  const ValueColumn<T>& _column;

 private:
  class Iterator : public BaseIterator<Iterator, ColumnValue<T>> {
   public:
    using ValueIterator = typename pmr_concurrent_vector<T>::const_iterator;

   public:
    explicit Iterator(const ValueIterator& begin_value_it, const ValueIterator& value_it)
        : _begin_value_it{begin_value_it}, _value_it(value_it) {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_value_it; }
    bool equal(const Iterator& other) const { return _value_it == other._value_it; }

    ColumnValue<T> dereference() const {
      return ColumnValue<T>{*_value_it, static_cast<ChunkOffset>(std::distance(_begin_value_it, _value_it))};
    }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
  };

  class NullableIterator : public BaseIterator<NullableIterator, NullableColumnValue<T>> {
   public:
    using ValueIterator = typename pmr_concurrent_vector<T>::const_iterator;
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit NullableIterator(const ValueIterator& begin_value_it, const ValueIterator& value_it,
                              const NullValueIterator& null_value_it)
        : _begin_value_it{begin_value_it}, _value_it(value_it), _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_value_it;
      ++_null_value_it;
    }

    bool equal(const NullableIterator& other) const { return _value_it == other._value_it; }

    NullableColumnValue<T> dereference() const {
      return NullableColumnValue<T>{*_value_it, *_null_value_it,
                                    static_cast<ChunkOffset>(std::distance(_begin_value_it, _value_it))};
    }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
  };

  class IndexedIterator : public BaseIndexedIterator<IndexedIterator, ColumnValue<T>> {
   public:
    using ValueVector = pmr_concurrent_vector<T>;

   public:
    explicit IndexedIterator(const ValueVector& values, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<IndexedIterator, ColumnValue<T>>{chunk_offsets_it}, _values{values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnValue<T>{T{}, chunk_offsets.into_referencing};

      return ColumnValue<T>{_values[chunk_offsets.into_referenced], chunk_offsets.into_referencing};
    }

   private:
    const ValueVector& _values;
  };

  class NullableIndexedIterator : public BaseIndexedIterator<NullableIndexedIterator, NullableColumnValue<T>> {
   public:
    using ValueVector = pmr_concurrent_vector<T>;
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit NullableIndexedIterator(const ValueVector& values, const NullValueVector& null_values,
                                     const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<NullableIndexedIterator, NullableColumnValue<T>>{chunk_offsets_it},
          _values{values},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    NullableColumnValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return NullableColumnValue<T>{T{}, true, chunk_offsets.into_referencing};

      return NullableColumnValue<T>{_values[chunk_offsets.into_referenced], _null_values[chunk_offsets.into_referenced],
                                    chunk_offsets.into_referencing};
    }

   private:
    const ValueVector& _values;
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
