#pragma once

#include <iterator>

#include "tbb/concurrent_vector.h"

#include "storage/base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

class ColumnNullValue {
 public:
  ColumnNullValue(const bool null_value, const ChunkOffset& chunk_offset)
      : _null_value{null_value}, _chunk_offset{chunk_offset} {}

  bool is_null() const { return false; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

class NullValueDictionaryIterable {
 public:
  class Iterator : public std::iterator<std::input_iterator_tag, ColumnNullValue, std::ptrdiff_t, ColumnNullValue*,
                                        ColumnNullValue> {
   public:
    explicit Iterator(const BaseAttributeVector& attribute_vector, size_t index)
        : _attribute_vector{attribute_vector}, _index{index} {}

    Iterator& operator++() {
      ++_index;
      return *this;
    }

    Iterator operator++(int) {
      auto retval = *this;
      ++(*this);
      return retval;
    }

    bool operator==(Iterator other) const { return _index == other._index; }
    bool operator!=(Iterator other) const { return !(*this == other); }

    auto operator*() const {
      const auto value_id = _attribute_vector.get(_index);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return ColumnNullValue{is_null, _index};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    size_t _index;
  };

  class ReferencedIterator : public std::iterator<std::input_iterator_tag, ColumnNullValue, std::ptrdiff_t,
                                                  ColumnNullValue*, ColumnNullValue> {
   public:
    using ChunkOffsetsIterator = std::vector<std::pair<ChunkOffset, ChunkOffset>>::const_iterator;

   public:
    explicit ReferencedIterator(const BaseAttributeVector& attribute_vector,
                                const ChunkOffsetsIterator& chunk_offsets_it)
        : _attribute_vector{attribute_vector}, _chunk_offsets_it{chunk_offsets_it} {}

    ReferencedIterator& operator++() {
      ++_chunk_offsets_it;
      return *this;
    }

    ReferencedIterator operator++(int) {
      auto retval = *this;
      ++(*this);
      return retval;
    }

    bool operator==(ReferencedIterator other) const { return (_chunk_offsets_it == other._chunk_offsets_it); }
    bool operator!=(ReferencedIterator other) const { return !(*this == other); }

    auto operator*() const {
      if (_chunk_offsets_it->second == INVALID_CHUNK_OFFSET) return ColumnNullValue{true, _chunk_offsets_it->first};

      const auto value_id = _attribute_vector.get(_chunk_offsets_it->second);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return ColumnNullValue{is_null, _chunk_offsets_it->first};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  NullValueDictionaryIterable(const BaseAttributeVector& attribute_vector,
                              const std::vector<std::pair<ChunkOffset, ChunkOffset>>* mapped_chunk_offsets = nullptr)
      : _attribute_vector{attribute_vector}, _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  auto execute_for_all(const Functor& func) const {
    if (_mapped_chunk_offsets != nullptr) {
      auto begin = ReferencedIterator(_attribute_vector, _mapped_chunk_offsets->cbegin());
      auto end = ReferencedIterator(_attribute_vector, _mapped_chunk_offsets->cend());
      return func(begin, end);
    }

    auto begin = Iterator(_attribute_vector, 0u);
    auto end = Iterator(_attribute_vector, _attribute_vector.size());
    return func(begin, end);
  }

  template <typename Functor>
  auto execute_for_all_no_mapping(const Functor& func) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    auto begin = Iterator(_attribute_vector, 0u);
    auto end = Iterator(_attribute_vector, _attribute_vector.size());
    return func(begin, end);
  }

 private:
  const BaseAttributeVector& _attribute_vector;
  const std::vector<std::pair<ChunkOffset, ChunkOffset>>* _mapped_chunk_offsets;
};

class NullValueValueColumnIterable {
 public:
  class Iterator : public std::iterator<std::input_iterator_tag, ColumnNullValue, std::ptrdiff_t, ColumnNullValue*,
                                        ColumnNullValue> {
   public:
    using NullValueIterator = tbb::concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it} {}

    Iterator& operator++() {
      ++_null_value_it;
      return *this;
    }

    Iterator operator++(int) {
      auto retval = *this;
      ++(*this);
      return retval;
    }

    bool operator==(Iterator other) const { return _null_value_it == other._null_value_it; }
    bool operator!=(Iterator other) const { return !(*this == other); }

    auto operator*() const {
      return ColumnNullValue{*_null_value_it, std::distance(_begin_null_value_it, _null_value_it)};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
  };

  class ReferencedIterator : public std::iterator<std::input_iterator_tag, ColumnNullValue, std::ptrdiff_t,
                                                  ColumnNullValue*, ColumnNullValue> {
   public:
    using NullValueVector = tbb::concurrent_vector<bool>;
    using ChunkOffsetsIterator = std::vector<std::pair<ChunkOffset, ChunkOffset>>::const_iterator;

   public:
    explicit ReferencedIterator(const NullValueVector& null_values, const ChunkOffsetsIterator& chunk_offsets_it)
        : _null_values{null_values}, _chunk_offsets_it(chunk_offsets_it) {}

    ReferencedIterator& operator++() {
      ++_chunk_offsets_it;
      return *this;
    }
    ReferencedIterator operator++(int) {
      auto retval = *this;
      ++(*this);
      return retval;
    }

    bool operator==(ReferencedIterator other) const {
      return (_chunk_offsets_it == other._chunk_offsets_it) && (&_null_values == &other._null_values);
    }

    bool operator!=(ReferencedIterator other) const { return !(*this == other); }

    auto operator*() const {
      if (_chunk_offsets_it->second == INVALID_CHUNK_OFFSET) return ColumnNullValue{true, _chunk_offsets_it->first};

      return ColumnNullValue{_null_values[_chunk_offsets_it->second], _chunk_offsets_it->first};
    }

   private:
    const NullValueVector& _null_values;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  NullValueValueColumnIterable(const tbb::concurrent_vector<bool>& null_values,
                               const std::vector<std::pair<ChunkOffset, ChunkOffset>>* mapped_chunk_offsets = nullptr)
      : _null_values{null_values}, _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  auto execute_for_all(const Functor& func) const {
    if (_mapped_chunk_offsets != nullptr) {
      auto begin = ReferencedIterator(_null_values, _mapped_chunk_offsets->cbegin());
      auto end = ReferencedIterator(_null_values, _mapped_chunk_offsets->cend());
      return func(begin, end);
    }

    auto begin = Iterator(_null_values.cbegin(), _null_values.cbegin());
    auto end = Iterator(_null_values.cbegin(), _null_values.cend());
    return func(begin, end);
  }

  template <typename Functor>
  auto execute_for_all_no_mapping(const Functor& func) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    auto begin = Iterator(_null_values.cbegin(), _null_values.cbegin());
    auto end = Iterator(_null_values.cbegin(), _null_values.cend());
    return func(begin, end);
  }

 private:
  const tbb::concurrent_vector<bool>& _null_values;
  const std::vector<std::pair<ChunkOffset, ChunkOffset>>* _mapped_chunk_offsets;
};

}  // namespace opossum
