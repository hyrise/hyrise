#pragma once

#include "tbb/concurrent_vector.h"

#include <iterator>

#include "value_column.hpp"


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
  class ColumnValue {
   public:
    ColumnValue(const T & value, const ChunkOffset & chunk_offset) : _value{value}, _chunk_offset{chunk_offset} {}

    const T & value() const { return _value; }
    bool is_null() const { return false; }
    ChunkOffset chunk_offset() const { return _chunk_offset; }

   private:
    const T & _value;
    const ChunkOffset & _chunk_offset;
  };

  class NullableColumnValue {
   public:
    NullableColumnValue(const T & value, const bool null_value, const ChunkOffset & chunk_offset)
        : _value{value},
          _null_value{null_value},
          _chunk_offset{chunk_offset} {}

    const T & value() const { return _value; }
    bool is_null() const { return _null_value; }
    ChunkOffset chunk_offset() { return _chunk_offset; }

   private:
    const T & _value;
    const bool _null_value;
    const ChunkOffset & _chunk_offset;
  };

  class Iterator : public std::iterator<std::input_iterator_tag, ColumnValue, std::ptrdiff_t, ColumnValue *, ColumnValue> {
   public:
    using ValueIterator = typename tbb::concurrent_vector<T>::const_iterator;

   public:
    explicit Iterator(const ValueIterator & begin_value_it, const ValueIterator & value_it)
        : _begin_value_it{begin_value_it}, _value_it(value_it) {}

    Iterator& operator++() { ++_value_it; return *this;}
    Iterator operator++(int) { auto retval = *this; ++(*this); return retval; }
    bool operator==(Iterator other) const { return _value_it == other._value_it; }
    bool operator!=(Iterator other) const { return !(*this == other); }
    auto operator*() const { return ColumnValue{*_value_it, std::distance(_begin_value_it, _value_it)}; }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
  };

  class NullableIterator : public std::iterator<std::input_iterator_tag, NullableColumnValue, std::ptrdiff_t, NullableColumnValue *, NullableColumnValue> {
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
      return NullableColumnValue{*_value_it, *_null_value_it, std::distance(_begin_value_it, _value_it)};
    }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
  };

  class ReferencedIterator : public std::iterator<std::input_iterator_tag, ColumnValue, std::ptrdiff_t, ColumnValue *, ColumnValue> {
   public:
    using ValueVector = tbb::concurrent_vector<T>;
    using ChunkOffsetIterator = std::vector<ChunkOffset>::const_iterator;

   public:
    explicit ReferencedIterator(const ValueVector & values, const ChunkOffsetIterator & chunk_offset_it)
        : _values{values}, _chunk_offset_it(chunk_offset_it) {}

    ReferencedIterator& operator++() { ++_chunk_offset_it; return *this;}
    ReferencedIterator operator++(int) { auto retval = *this; ++(*this); return retval; }

    bool operator==(ReferencedIterator other) const {
      return (_chunk_offset_it == other._chunk_offset_it) && (&_values == &other._values);
    }

    bool operator!=(ReferencedIterator other) const { return !(*this == other); }

    auto operator*() const {
      return ColumnValue{_values[*_chunk_offset_it], *_chunk_offset_it};
    }

   private:
    const ValueVector & _values;
    ChunkOffsetIterator _chunk_offset_it;
  };

  class NullableReferencedIterator : public std::iterator<std::input_iterator_tag, NullableColumnValue, std::ptrdiff_t, NullableColumnValue *, NullableColumnValue> {
   public:
    using ValueVector = tbb::concurrent_vector<T>;
    using NullValueVector = tbb::concurrent_vector<bool>;
    using ChunkOffsetIterator = std::vector<ChunkOffset>::const_iterator;

   public:
    explicit NullableReferencedIterator(const ValueVector & values, const NullValueVector & null_values, const ChunkOffsetIterator & chunk_offset_it)
        : _values{values}, _null_values{null_values}, _chunk_offset_it(chunk_offset_it) {}

    NullableReferencedIterator& operator++() { ++_chunk_offset_it; return *this;}
    NullableReferencedIterator operator++(int) { auto retval = *this; ++(*this); return retval; }

    bool operator==(NullableReferencedIterator other) const {
      return (_chunk_offset_it == other._chunk_offset_it) && (&_values == &other._values);
    }

    bool operator!=(NullableReferencedIterator other) const { return !(*this == other); }

    auto operator*() const {
      return NullableColumnValue{_values[*_chunk_offset_it], _null_values[*_chunk_offset_it], *_chunk_offset_it};
    }

   private:
    const ValueVector & _values;
    const NullValueVector & _null_values;
    ChunkOffsetIterator _chunk_offset_it;
  };

  ValueColumnIterable(const ValueColumn<T> & column,
                      std::shared_ptr<const std::vector<ChunkOffset>> chunk_offsets = nullptr)
      : _column{column}, _chunk_offsets{chunk_offsets} {}

  template <typename Functor>
  auto execute_for_all(const Functor & func) {
    if (_column.is_nullable() && _chunk_offsets != nullptr) {
      auto begin = NullableReferencedIterator(_column.values(), _column.null_values(), _chunk_offsets->cbegin());
      auto end = NullableReferencedIterator(_column.values(), _column.null_values(), _chunk_offsets->cend());
      return func(begin, end);
    }

    if (_chunk_offsets != nullptr) {
      auto begin = ReferencedIterator(_column.values(), _chunk_offsets->cbegin());
      auto end = ReferencedIterator(_column.values(), _chunk_offsets->cend());
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

  Type type() const {
    if (_column.is_nullable() && _chunk_offsets != nullptr) {
      return Type::NullableReferenced;
    }

    if (_chunk_offsets != nullptr) {
      return Type::Referenced;
    }

    if (_column.is_nullable()) {
      return Type::Nullable;
    }

    return Type::Simple;
  }

 private:
  const ValueColumn<T> & _column;
  const std::shared_ptr<const std::vector<ChunkOffset>> _chunk_offsets;
};

}  // namespace opossum
