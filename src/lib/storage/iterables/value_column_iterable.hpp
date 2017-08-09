#pragma once

#include "tbb/concurrent_vector.h"

#include <iterator>
#include <utility>

#include "storage/value_column.hpp"
#include "column_value.hpp"


namespace opossum {

enum class ValueColumnIterableType {
  NullableReferenced,
  Referenced,
  Nullable,
  Simple
};

template <typename T>
class ValueColumnIterable
{
 public:
  using Type = ValueColumnIterableType;

 public:
  class Iterator : public std::iterator<std::input_iterator_tag, ColumnValue<T>, std::ptrdiff_t, ColumnValue<T> *, ColumnValue<T>> {
   public:
    using ValueIterator = typename tbb::concurrent_vector<T>::const_iterator;

   public:
    explicit Iterator(const ValueIterator & begin_value_it, const ValueIterator & value_it)
        : _begin_value_it{begin_value_it}, _value_it(value_it) {}

    Iterator& operator++() { ++_value_it; return *this;}
    Iterator operator++(int) { auto retval = *this; ++(*this); return retval; }
    bool operator==(Iterator other) const { return _value_it == other._value_it; }
    bool operator!=(Iterator other) const { return !(*this == other); }
    auto operator*() const { return ColumnValue<T>{*_value_it, std::distance(_begin_value_it, _value_it)}; }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
  };

  class NullableIterator : public std::iterator<std::input_iterator_tag, NullableColumnValue<T>, std::ptrdiff_t, NullableColumnValue<T> *, NullableColumnValue<T>> {
   public:
    using ValueIterator = typename tbb::concurrent_vector<T>::const_iterator;
    using NullValueIterator = tbb::concurrent_vector<bool>::const_iterator;

   public:
    explicit NullableIterator(const ValueIterator & begin_value_it, const ValueIterator & value_it, const NullValueIterator & null_value_it)
        : _begin_value_it{begin_value_it},
          _value_it(value_it),
          _null_value_it{null_value_it} {}

    NullableIterator& operator++() { ++_value_it; ++_null_value_it; return *this;}
    NullableIterator operator++(int) { auto retval = *this; ++(*this); return retval; }
    bool operator==(NullableIterator other) const { return _value_it == other._value_it; }
    bool operator!=(NullableIterator other) const { return !(*this == other); }

    auto operator*() const {
      return NullableColumnValue<T>{*_value_it, *_null_value_it, std::distance(_begin_value_it, _value_it)};
    }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
  };

  class ReferencedIterator : public std::iterator<std::input_iterator_tag, ColumnValue<T>, std::ptrdiff_t, ColumnValue<T> *, ColumnValue<T>> {
   public:
    using ValueVector = tbb::concurrent_vector<T>;
    using ChunkOffsetsIterator = std::vector<std::pair<ChunkOffset, ChunkOffset>>::const_iterator;

   public:
    explicit ReferencedIterator(const ValueVector & values, const ChunkOffsetsIterator & chunk_offsets_it)
        : _values{values}, _chunk_offsets_it{chunk_offsets_it} {}

    ReferencedIterator& operator++() { ++_chunk_offsets_it; return *this;}
    ReferencedIterator operator++(int) { auto retval = *this; ++(*this); return retval; }

    bool operator==(ReferencedIterator other) const {
      return (_chunk_offsets_it == other._chunk_offsets_it) && (&_values == &other._values);
    }

    bool operator!=(ReferencedIterator other) const { return !(*this == other); }

    auto operator*() const {
      return ColumnValue<T>{_values[_chunk_offsets_it->second], _chunk_offsets_it->first};
    }

   private:
    const ValueVector & _values;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  class NullableReferencedIterator : public std::iterator<std::input_iterator_tag, NullableColumnValue<T>, std::ptrdiff_t, NullableColumnValue<T> *, NullableColumnValue<T>> {
   public:
    using ValueVector = tbb::concurrent_vector<T>;
    using NullValueVector = tbb::concurrent_vector<bool>;
    using ChunkOffsetsIterator = std::vector<std::pair<ChunkOffset, ChunkOffset>>::const_iterator;

   public:
    explicit NullableReferencedIterator(const ValueVector & values, const NullValueVector & null_values, const ChunkOffsetsIterator & chunk_offsets_it)
        : _values{values}, _null_values{null_values}, _chunk_offsets_it(chunk_offsets_it) {}

    NullableReferencedIterator& operator++() { ++_chunk_offsets_it; return *this;}
    NullableReferencedIterator operator++(int) { auto retval = *this; ++(*this); return retval; }

    bool operator==(NullableReferencedIterator other) const {
      return (_chunk_offsets_it == other._chunk_offsets_it) && (&_values == &other._values);
    }

    bool operator!=(NullableReferencedIterator other) const { return !(*this == other); }

    auto operator*() const {
      return NullableColumnValue<T>{_values[_chunk_offsets_it->second], _null_values[_chunk_offsets_it->second], _chunk_offsets_it->first};
    }

   private:
    const ValueVector & _values;
    const NullValueVector & _null_values;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  ValueColumnIterable(const ValueColumn<T> & column,
                      const std::vector<std::pair<ChunkOffset, ChunkOffset>> * mapped_chunk_offsets = nullptr)
      : _column{column}, _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  auto execute_for_all(const Functor & func) const {
    if (_column.is_nullable() && _mapped_chunk_offsets != nullptr) {
      auto begin = NullableReferencedIterator(_column.values(), _column.null_values(), _mapped_chunk_offsets->cbegin());
      auto end = NullableReferencedIterator(_column.values(), _column.null_values(), _mapped_chunk_offsets->cend());
      return func(begin, end);
    }

    if (_mapped_chunk_offsets != nullptr) {
      auto begin = ReferencedIterator(_column.values(), _mapped_chunk_offsets->cbegin());
      auto end = ReferencedIterator(_column.values(), _mapped_chunk_offsets->cend());
      return func(begin, end);
    }

    if (_column.is_nullable()) {
      auto begin = NullableIterator(_column.values().cbegin(), _column.values().cbegin(), _column.null_values().cbegin());
      auto end = NullableIterator(_column.values().cbegin(), _column.values().cend(), _column.null_values().cend());
      return func(begin, end);
    }

    auto begin = Iterator(_column.values().cbegin(), _column.values().cbegin());
    auto end = Iterator(_column.values().cend(), _column.values().cend());
    return func(begin, end);
  }

  template <typename Functor>
  auto execute_for_all_no_mapping(const Functor & func) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    if (_column.is_nullable()) {
      auto begin = NullableIterator(_column.values().cbegin(), _column.values().cbegin(), _column.null_values().cbegin());
      auto end = NullableIterator(_column.values().cbegin(), _column.values().cend(), _column.null_values().cend());
      return func(begin, end);
    }

    auto begin = Iterator(_column.values().cbegin(), _column.values().cbegin());
    auto end = Iterator(_column.values().cend(), _column.values().cend());
    return func(begin, end);
  }

  Type type() const {
    if (_column.is_nullable() && _mapped_chunk_offsets != nullptr) {
      return Type::NullableReferenced;
    }

    if (_mapped_chunk_offsets != nullptr) {
      return Type::Referenced;
    }

    if (_column.is_nullable()) {
      return Type::Nullable;
    }

    return Type::Simple;
  }

 private:
  const ValueColumn<T> & _column;
  const std::vector<std::pair<ChunkOffset, ChunkOffset>> * _mapped_chunk_offsets;
};

}  // namespace opossum
