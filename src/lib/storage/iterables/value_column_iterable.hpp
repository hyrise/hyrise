#pragma once

#include <iterator>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "column_value.hpp"
#include "storage/value_column.hpp"

namespace opossum {

enum class ValueColumnIterableType { NullableReferenced, Referenced, Nullable, Simple };

template <typename T>
class ValueColumnIterable {
 public:
  using Type = ValueColumnIterableType;

 public:
  class Iterator : public BaseIterator<Iterator, ColumnValue<T>> {
   public:
    using ValueIterator = typename tbb::concurrent_vector<T>::const_iterator;

   public:
    explicit Iterator(const ValueIterator& begin_value_it, const ValueIterator& value_it)
        : _begin_value_it{begin_value_it}, _value_it(value_it) {}

   private:
    friend class BaseIteratorAccess;

    void increment() { ++_value_it; }
    bool equal(const Iterator & other) const { return _value_it == other._value_it; }

    ColumnValue<T> dereference() const {
      return ColumnValue<T>{*_value_it, static_cast<ChunkOffset>(std::distance(_begin_value_it, _value_it))};
    }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
  };

  class NullableIterator : public BaseIterator<NullableIterator, NullableColumnValue<T>> {
   public:
    using ValueIterator = typename tbb::concurrent_vector<T>::const_iterator;
    using NullValueIterator = tbb::concurrent_vector<bool>::const_iterator;

   public:
    explicit NullableIterator(const ValueIterator& begin_value_it, const ValueIterator& value_it,
                              const NullValueIterator& null_value_it)
        : _begin_value_it{begin_value_it}, _value_it(value_it), _null_value_it{null_value_it} {}

   private:
    friend class BaseIteratorAccess;

    void increment() { 
      ++_value_it;
      ++_null_value_it; 
    }

    bool equal(const NullableIterator & other) const { return _value_it == other._value_it; }

    NullableColumnValue<T> dereference() const {
      return NullableColumnValue<T>{*_value_it, *_null_value_it,
                                    static_cast<ChunkOffset>(std::distance(_begin_value_it, _value_it))};
    }

   private:
    const ValueIterator _begin_value_it;
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
  };

  class ReferencedIterator : public BaseReferencedIterator<ReferencedIterator, ColumnValue<T>> {
   public:
    using ValueVector = tbb::concurrent_vector<T>;

   public:
    explicit ReferencedIterator(const ValueVector& values, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseReferencedIterator<ReferencedIterator, ColumnValue<T>>{chunk_offsets_it},
          _values{values} {}

   private:
    friend class BaseIteratorAccess;

    ColumnValue<T> dereference() const { return ColumnValue<T>{_values[this->index_into_referenced()], this->index_of_referencing()}; }

   private:
    const ValueVector& _values;
  };

  class NullableReferencedIterator : public BaseReferencedIterator<NullableReferencedIterator, NullableColumnValue<T>> {
   public:
    using ValueVector = tbb::concurrent_vector<T>;
    using NullValueVector = tbb::concurrent_vector<bool>;

   public:
    explicit NullableReferencedIterator(const ValueVector& values, const NullValueVector& null_values,
                                        const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseReferencedIterator<NullableReferencedIterator, NullableColumnValue<T>>{chunk_offsets_it},
          _values{values},
          _null_values{null_values} {}

   private:
    friend class BaseIteratorAccess;
    
    NullableColumnValue<T> dereference() const {
      return NullableColumnValue<T>{_values[this->index_into_referenced()], _null_values[this->index_into_referenced()],
                                    this->index_of_referencing()};
    }

   private:
    const ValueVector& _values;
    const NullValueVector& _null_values;
  };

  ValueColumnIterable(const ValueColumn<T>& column,
                      const std::vector<std::pair<ChunkOffset, ChunkOffset>>* mapped_chunk_offsets = nullptr)
      : _column{column}, _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  void execute_for_all_no_mapping(const Functor& func) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    if (_column.is_nullable()) {
      auto begin =
          NullableIterator{_column.values().cbegin(), _column.values().cbegin(), _column.null_values().cbegin()};
      auto end = NullableIterator{_column.values().cbegin(), _column.values().cend(), _column.null_values().cend()};
      func(begin, end);
      return;
    }

    auto begin = Iterator{_column.values().cbegin(), _column.values().cbegin()};
    auto end = Iterator{_column.values().cend(), _column.values().cend()};
    func(begin, end);
  }

  template <typename Functor>
  void execute_for_all(const Functor& func) const {
    if (_mapped_chunk_offsets == nullptr) {
      execute_for_all_no_mapping(func);
      return;
    }

    if (_column.is_nullable()) {
      auto begin = NullableReferencedIterator{_column.values(), _column.null_values(), _mapped_chunk_offsets->cbegin()};
      auto end = NullableReferencedIterator{_column.values(), _column.null_values(), _mapped_chunk_offsets->cend()};
      func(begin, end);
      return;
    }

    auto begin = ReferencedIterator{_column.values(), _mapped_chunk_offsets->cbegin()};
    auto end = ReferencedIterator{_column.values(), _mapped_chunk_offsets->cend()};
    func(begin, end);
    return;
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
  const ValueColumn<T>& _column;
  const std::vector<std::pair<ChunkOffset, ChunkOffset>>* _mapped_chunk_offsets;
};

}  // namespace opossum
