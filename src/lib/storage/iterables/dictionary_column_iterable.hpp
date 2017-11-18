#pragma once

#include <utility>
#include <vector>

#include "iterables.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/dictionary_column.hpp"

namespace opossum {

template <typename T>
class DictionaryColumnIterable : public IndexableIterable<DictionaryColumnIterable<T>> {
 public:
  explicit DictionaryColumnIterable(const DictionaryColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin = Iterator{*_column.dictionary(), *_column.attribute_vector(), 0u};
    auto end = Iterator{*_column.dictionary(), *_column.attribute_vector(), static_cast<ChunkOffset>(_column.size())};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    auto begin = IndexedIterator{*_column.dictionary(), *_column.attribute_vector(), mapped_chunk_offsets.cbegin()};
    auto end = IndexedIterator{*_column.dictionary(), *_column.attribute_vector(), mapped_chunk_offsets.cend()};
    functor(begin, end);
  }

 private:
  const DictionaryColumn<T>& _column;

 private:
  class Iterator : public BaseIterator<Iterator, NullableColumnValue<T>> {
   public:
    using Dictionary = pmr_vector<T>;

   public:
    explicit Iterator(const Dictionary& dictionary, const BaseAttributeVector& attribute_vector,
                      ChunkOffset chunk_offset)
        : _dictionary{dictionary}, _attribute_vector{attribute_vector}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }
    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    NullableColumnValue<T> dereference() const {
      const auto value_id = _attribute_vector.get(_chunk_offset);
      const auto is_null = (value_id == NULL_VALUE_ID);

      if (is_null) return NullableColumnValue<T>{T{}, true, _chunk_offset};

      return NullableColumnValue<T>{_dictionary[value_id], false, _chunk_offset};
    }

   private:
    const Dictionary& _dictionary;
    const BaseAttributeVector& _attribute_vector;
    ChunkOffset _chunk_offset;
  };

  class IndexedIterator : public BaseIndexedIterator<IndexedIterator, NullableColumnValue<T>> {
   public:
    using Dictionary = pmr_vector<T>;

   public:
    explicit IndexedIterator(const Dictionary& dictionary, const BaseAttributeVector& attribute_vector,
                             const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<IndexedIterator, NullableColumnValue<T>>{chunk_offsets_it},
          _dictionary{dictionary},
          _attribute_vector{attribute_vector} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    NullableColumnValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return NullableColumnValue<T>{T{}, true, chunk_offsets.into_referencing};

      const auto value_id = _attribute_vector.get(chunk_offsets.into_referenced);
      const auto is_null = (value_id == NULL_VALUE_ID);

      if (is_null) return NullableColumnValue<T>{T{}, true, chunk_offsets.into_referencing};

      return NullableColumnValue<T>{_dictionary[value_id], false, chunk_offsets.into_referencing};
    }

   private:
    const Dictionary& _dictionary;
    const BaseAttributeVector& _attribute_vector;
  };
};

}  // namespace opossum
