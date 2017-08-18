#pragma once

#include <iterator>
#include <utility>

#include "tbb/concurrent_vector.h"

#include "column_value.hpp"
#include "iterator_utils.hpp"

#include "types.hpp"


namespace opossum {

class NullValueVectorIterable {
 public:
  NullValueVectorIterable(const tbb::concurrent_vector<bool>& null_values,
                               const ChunkOffsetsList* mapped_chunk_offsets = nullptr)
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

    auto begin = IndexedIterator{_null_values, _mapped_chunk_offsets->cbegin()};
    auto end = IndexedIterator{_null_values, _mapped_chunk_offsets->cend()};
    func(begin, end);
  }

 private:
  const tbb::concurrent_vector<bool>& _null_values;
  const ChunkOffsetsList* _mapped_chunk_offsets;

 private:
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

  class IndexedIterator : public BaseIndexedIterator<IndexedIterator, ColumnNullValue> {
   public:
    using NullValueVector = tbb::concurrent_vector<bool>;

   public:
    explicit IndexedIterator(const NullValueVector& null_values, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<IndexedIterator, ColumnNullValue>{chunk_offsets_it},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;

    ColumnNullValue dereference() const {
      if (this->index_into_referenced() == INVALID_CHUNK_OFFSET) return ColumnNullValue{true, this->index_of_referencing()};

      return ColumnNullValue{_null_values[this->index_into_referenced()], this->index_of_referencing()};
    }

   private:
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
