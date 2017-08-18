#pragma once

#include <iterator>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "iterator_utils.hpp"
#include "storage/base_attribute_vector.hpp"

#include "types.hpp"

namespace opossum {

class ColumnNullValue {
 public:
  ColumnNullValue(const bool null_value, const ChunkOffset& chunk_offset)
      : _null_value{null_value}, _chunk_offset{chunk_offset} {}

  bool is_null() const { return _null_value; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

class NullValueDictionaryIterable {
 public:
  class Iterator : public BaseIterator<Iterator, ColumnNullValue> {
   public:
    explicit Iterator(const BaseAttributeVector& attribute_vector, ChunkOffset chunk_offset)
        : _attribute_vector{attribute_vector}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;

    void increment() { ++_chunk_offset; }
    bool equal(const Iterator &other) const { return _chunk_offset == other._chunk_offset; }

    ColumnNullValue dereference() const {
      const auto value_id = _attribute_vector.get(_chunk_offset);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return ColumnNullValue{is_null, _chunk_offset};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    ChunkOffset _chunk_offset;
  };

  class ReferencedIterator : public BaseReferencedIterator<ReferencedIterator, ColumnNullValue> {
   public:
    explicit ReferencedIterator(const BaseAttributeVector& attribute_vector,
                                const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseReferencedIterator<ReferencedIterator, ColumnNullValue>{chunk_offsets_it},
          _attribute_vector{attribute_vector} {}

   private:
    friend class boost::iterator_core_access;

    ColumnNullValue dereference() const {
      if (this->index_into_referenced() == INVALID_CHUNK_OFFSET) return ColumnNullValue{true, this->index_of_referencing()};

      const auto value_id = _attribute_vector.get(this->index_into_referenced());
      const auto is_null = (value_id == NULL_VALUE_ID);

      return ColumnNullValue{is_null, this->index_of_referencing()};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  NullValueDictionaryIterable(const BaseAttributeVector& attribute_vector,
                              const std::vector<std::pair<ChunkOffset, ChunkOffset>>* mapped_chunk_offsets = nullptr)
      : _attribute_vector{attribute_vector}, _mapped_chunk_offsets{mapped_chunk_offsets} {}


  template <typename Functor>
  void execute_for_all_no_mapping(const Functor& func) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    auto begin = Iterator{_attribute_vector, 0u};
    auto end = Iterator{_attribute_vector, static_cast<ChunkOffset>(_attribute_vector.size())};
    func(begin, end);
  }

  template <typename Functor>
  void execute_for_all(const Functor& func) const {
    if (_mapped_chunk_offsets == nullptr) {
      execute_for_all_no_mapping(func);
      return;
    }

    auto begin = ReferencedIterator{_attribute_vector, _mapped_chunk_offsets->cbegin()};
    auto end = ReferencedIterator{_attribute_vector, _mapped_chunk_offsets->cend()};
    func(begin, end);
    return;
  }

 private:
  const BaseAttributeVector& _attribute_vector;
  const std::vector<std::pair<ChunkOffset, ChunkOffset>>* _mapped_chunk_offsets;
};

class NullValueValueColumnIterable {
 public:
  class Iterator : public BaseIterator<Iterator, ColumnNullValue> {
   public:
    using NullValueIterator = tbb::concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;

    void increment() { ++_null_value_it; }
    bool equal(const Iterator &other) const { return _null_value_it == other._null_value_it; }

    ColumnNullValue dereference() const {
      return ColumnNullValue{*_null_value_it,
                             static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
  };

  class ReferencedIterator : public BaseReferencedIterator<ReferencedIterator, ColumnNullValue> {
   public:
    using NullValueVector = tbb::concurrent_vector<bool>;

   public:
    explicit ReferencedIterator(const NullValueVector& null_values, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseReferencedIterator<ReferencedIterator, ColumnNullValue>{chunk_offsets_it},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;

    ColumnNullValue dereference() const {
      if (this->index_into_referenced() == INVALID_CHUNK_OFFSET) return ColumnNullValue{true, this->index_of_referencing()};

      return ColumnNullValue{_null_values[this->index_into_referenced()], this->index_of_referencing()};
    }

   private:
    const NullValueVector& _null_values;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  NullValueValueColumnIterable(const tbb::concurrent_vector<bool>& null_values,
                               const std::vector<std::pair<ChunkOffset, ChunkOffset>>* mapped_chunk_offsets = nullptr)
      : _null_values{null_values}, _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  void execute_for_all_no_mapping(const Functor& func) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    auto begin = Iterator{_null_values.cbegin(), _null_values.cbegin()};
    auto end = Iterator{_null_values.cbegin(), _null_values.cend()};
    func(begin, end);
  }

  template <typename Functor>
  void execute_for_all(const Functor& func) const {
    if (_mapped_chunk_offsets == nullptr) {
      execute_for_all_no_mapping(func);
      return;
    }

    auto begin = ReferencedIterator{_null_values, _mapped_chunk_offsets->cbegin()};
    auto end = ReferencedIterator{_null_values, _mapped_chunk_offsets->cend()};
    func(begin, end);
  }

 private:
  const tbb::concurrent_vector<bool>& _null_values;
  const std::vector<std::pair<ChunkOffset, ChunkOffset>>* _mapped_chunk_offsets;
};

}  // namespace opossum
