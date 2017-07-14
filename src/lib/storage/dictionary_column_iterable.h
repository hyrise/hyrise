#pragma once

#include "tbb/concurrent_vector.h"

#include <iterator>

#include "dictionary_column.h"


template <typename T>
class DictionaryColumnIterable
{
 public:
  class NullableColumnValue {
   public:
    ColumnValue(const T & value, const bool null_value, const ChunkOffset & chunk_offset)
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

  class Iterator : std::iterator<std::input_iterator_tag, NullableColumnValue, std::ptrdiff_t, NullableColumnValue *, NullableColumnValue> {
   public:
    using Dictionary = std::vector<T>;

   public:
    explicit Iterator(const Dictionary & dictionary, const BaseAttributeVector * attribute_vector, size_t index)
        : _dictionary{dictionary}, _attribute_vector(attribute_vector), _index{index} {}

    Iterator& operator++() { ++_index; return *this;}
    Iterator operator++(int) { auto retval = *this; ++(*this); return retval; }
    bool operator==(Iterator other) const { return _index == other._index; }
    bool operator!=(Iterator other) const { return !(*this == other); }

    reference operator*() const {
      const auto value_id = _attribute_vector->get(_index);
      const auto is_null = (value_id == NULL_VALUE_ID);

      if (is_null)
        return NullableColumnValue{T{}, is_null, _index};

      return NullColumnValue{_dictionary[value_id], is_null, _index};
    }

   private:
    const Dictionary & _dictionary;
    const BaseAttributeVector * _attribute_vector;
    size_t _index;
  };

  class ReferencedIterator : std::iterator<std::input_iterator_tag, NullableColumnValue, std::ptrdiff_t, NullableColumnValue *, NullableColumnValue> {
   public:
    using Dictionary = std::vector<T>;
    using ChunkOffsetIterator = std::vector<ChunkOffset>::const_iterator;

   public:
    explicit ReferencedIterator(const Dictionary & dictionary, const BaseAttributeVector * attribute_vector, const ChunkOffsetIterator & chunk_offset_it)
        : _dictionary{dictionary}, _attribute_vector(attribute_vector), _chunk_offset_it(chunk_offset_it) {}

    Iterator& operator++() { ++_chunk_offset_it; return *this;}
    Iterator operator++(int) { auto retval = *this; ++(*this); return retval; }

    bool operator==(Iterator other) const {
      return (_chunk_offset_it == other._chunk_offset_it);
    }

    bool operator!=(Iterator other) const { return !(*this == other); }

    reference operator*() const {
      const auto value_id = _attribute_vector->get(*_chunk_offset_it);
      const auto is_null = (value_id == NULL_VALUE_ID);

      if (is_null)
        return NullableColumnValue{T{}, is_null, *_chunk_offset_it};

      return NullableColumnValue{_dictionary[value_id], is_null, *_chunk_offset_it};
    }

   private:
    const Dictionary & _dictionary;
    const BaseAttributeVector * _attribute_vector;
    ChunkOffsetIterator _chunk_offset_it;
  };

  DictionaryColumnIterable(std::shared_ptr<const Dictionary<T>> column,
                           std::shared_ptr<const std::vector<ChunkOffset>> chunk_offsets)
      : _column{column}, _chunk_offsets{chunk_offsets} {}

  template <typename Functor>
  void execute_for_all(const Functor & func) {
    if (_chunk_offsets != nullptr) {
      auto begin = ReferencedIterator(*_column->dictionary(), *_column->attribute_vector(), _chunk_offsets->cbegin());
      auto end = ReferencedIterator(*_column->dictionary(), *_column->attribute_vector(), _chunk_offsets->cend());
      return func(begin, end);
    }

    auto begin = Iterator(*_column->dictionary(), *_column->attribute_vector(), 0u);
    auto end = Iterator(*_column->dictionary(), *_column->attribute_vector(), _column->size());
    return func(begin, end);
  }

 private:
  const std::shared_ptr<const DictionaryColumn<T>> _column;
  const std::shared_ptr<const std::vector<ChunkOffset>> _chunk_offsets;
};
