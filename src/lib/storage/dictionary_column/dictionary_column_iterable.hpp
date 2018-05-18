#pragma once

#include <type_traits>

#include "storage/column_iterables.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/fixed_string_dictionary_column/fixed_string_column.hpp"

#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class DictionaryColumnIterable : public PointAccessibleColumnIterable<DictionaryColumnIterable<T>> {
 public:
  explicit DictionaryColumnIterable(const DictionaryColumn<T>& column) : _column{column}, _dictionary(column.dictionary()) {
    std::cout << "regular dict one";
}

  template<typename U = T,
  typename std::enable_if<std::is_same<U, std::string>::value>::type>
  explicit DictionaryColumnIterable(const FixedStringColumn<T>& column)
  : _column{column}, _dictionary(column.dictionary()) {
    std::cout << "new constr." << std::endl;
  }  
  explicit DictionaryColumnIterable(const FixedStringColumn<T>& column) : _column{column}, _dictionary{std::make_shared<const pmr_vector<T>>()} {
    std::cout << "shitty one";
  }

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    std::cout << "im here 1" << std::endl;
    resolve_compressed_vector_type(*_column.attribute_vector(), [&](const auto& vector) {
      using ZsIteratorType = decltype(vector.cbegin());

      auto begin =
          Iterator<ZsIteratorType>{*_dictionary, _column.null_value_id(), vector.cbegin(), ChunkOffset{0u}};
      auto end = Iterator<ZsIteratorType>{*_dictionary, _column.null_value_id(), vector.cend(),
                                          static_cast<ChunkOffset>(_column.size())};
      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    std::cout << "im here 2" << std::endl;
    resolve_compressed_vector_type(*_column.attribute_vector(), [&](const auto& vector) {
      auto decoder = vector.create_decoder();
      using ZsDecoderType = std::decay_t<decltype(*decoder)>;

      auto begin = PointAccessIterator<ZsDecoderType>{*_dictionary, _column.null_value_id(), *decoder,
                                                      mapped_chunk_offsets.cbegin()};
      auto end = PointAccessIterator<ZsDecoderType>{*_dictionary, _column.null_value_id(), *decoder,
                                                    mapped_chunk_offsets.cend()};
      functor(begin, end);
    });
  }

 private:
  const BaseDictionaryColumn& _column;
  const std::shared_ptr<const pmr_vector<T>> _dictionary;

 private:
  template <typename ZsIteratorType>
  class Iterator : public BaseColumnIterator<Iterator<ZsIteratorType>, ColumnIteratorValue<T>> {
   public:
    explicit Iterator(const pmr_vector<T>& dictionary, const ValueID null_value_id, const ZsIteratorType attribute_it,
                      ChunkOffset chunk_offset)
        : _dictionary{dictionary},
          _null_value_id{null_value_id},
          _attribute_it{attribute_it},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_attribute_it;
      ++_chunk_offset;
    }

    bool equal(const Iterator& other) const { return _attribute_it == other._attribute_it; }

    ColumnIteratorValue<T> dereference() const {
      const auto value_id = *_attribute_it;
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return ColumnIteratorValue<T>{T{}, true, _chunk_offset};

      return ColumnIteratorValue<T>{_dictionary[value_id], false, _chunk_offset};
    }

   private:
    const pmr_vector<T>& _dictionary;
    const ValueID _null_value_id;
    ZsIteratorType _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename ZsDecoderType>
  class PointAccessIterator
      : public BasePointAccessColumnIterator<PointAccessIterator<ZsDecoderType>, ColumnIteratorValue<T>> {
   public:
    PointAccessIterator(const pmr_vector<T>& dictionary, const ValueID null_value_id, ZsDecoderType& attribute_decoder,
                        ChunkOffsetsIterator chunk_offsets_it)
        : BasePointAccessColumnIterator<PointAccessIterator<ZsDecoderType>, ColumnIteratorValue<T>>{chunk_offsets_it},
          _dictionary{dictionary},
          _null_value_id{null_value_id},
          _attribute_decoder{attribute_decoder} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = _attribute_decoder.get(chunk_offsets.into_referenced);
      const auto is_null = (value_id == _null_value_id);

      if (is_null) return ColumnIteratorValue<T>{T{}, true, chunk_offsets.into_referencing};

      return ColumnIteratorValue<T>{_dictionary[value_id], false, chunk_offsets.into_referencing};
    }

   private:
    const pmr_vector<T>& _dictionary;
    const ValueID _null_value_id;
    ZsDecoderType& _attribute_decoder;
  };
};

}  // namespace opossum
