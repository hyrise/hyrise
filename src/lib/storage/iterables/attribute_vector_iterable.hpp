#pragma once

#include <iterator>

#include "column_value.hpp"
#include "storage/base_attribute_vector.hpp"

namespace opossum {

class AttributeVectorIterable {
 public:
  class Iterator : public std::iterator<std::input_iterator_tag, NullableColumnValue<ValueID>, std::ptrdiff_t,
                                        NullableColumnValue<ValueID>*, NullableColumnValue<ValueID>> {
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

      return NullableColumnValue<ValueID>{value_id, is_null, _index};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    size_t _index;
  };

  class ReferencedIterator : public std::iterator<std::input_iterator_tag, NullableColumnValue<ValueID>, std::ptrdiff_t,
                                                  NullableColumnValue<ValueID>*, NullableColumnValue<ValueID>> {
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
      const auto value_id = _attribute_vector.get(_chunk_offsets_it->second);
      const auto is_null = (value_id == NULL_VALUE_ID);

      return NullableColumnValue<ValueID>{value_id, is_null, _chunk_offsets_it->first};
    }

   private:
    const BaseAttributeVector& _attribute_vector;
    ChunkOffsetsIterator _chunk_offsets_it;
  };

  AttributeVectorIterable(const BaseAttributeVector& attribute_vector,
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

}  // namespace opossum
