#pragma once

#include <utility>

#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

class AttributeVectorIterable : public PointAccessibleSegmentIterable<AttributeVectorIterable> {
 public:
  using ValueType = ValueID;

  explicit AttributeVectorIterable(const BaseDictionarySegment& segment, const ValueID null_value_id)
      : _attribute_vector{*segment.attribute_vector()},
        _null_value_id{null_value_id},
        _access_counter(segment.access_counter) {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(_attribute_vector, [&](const auto& vector) {
      using CompressedVectorIterator = decltype(vector.cbegin());

      auto begin = Iterator<CompressedVectorIterator>{_null_value_id, vector.cbegin(), ChunkOffset{0u}};
      auto end = Iterator<CompressedVectorIterator>{_null_value_id, vector.cend(),
                                                    static_cast<ChunkOffset>(_attribute_vector.size())};
      functor(begin, end);
    });

    _access_counter[SegmentAccessCounter::AccessType::Sequential] += _attribute_vector.size();
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    resolve_compressed_vector_type(_attribute_vector, [&](const auto& vector) {
      using Decompressor = std::decay_t<decltype(vector.create_decompressor())>;

      using PosListIteratorType = std::decay_t<decltype(position_filter->cbegin())>;
      auto begin = PointAccessIterator<Decompressor, PosListIteratorType>{
          _null_value_id, vector.create_decompressor(), position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator<Decompressor, PosListIteratorType>{
          _null_value_id, vector.create_decompressor(), position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    });

    _access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();
  }

  size_t _on_size() const { return _attribute_vector.size(); }

 private:
  const BaseCompressedVector& _attribute_vector;
  const ValueID _null_value_id;
  SegmentAccessCounter& _access_counter;

 private:
  template <typename CompressedVectorIterator>
  class Iterator : public AbstractSegmentIterator<Iterator<CompressedVectorIterator>, SegmentPosition<ValueID>> {
   public:
    using ValueType = ValueID;
    explicit Iterator(const ValueID null_value_id, CompressedVectorIterator&& attribute_it, ChunkOffset chunk_offset)
        : _null_value_id{null_value_id}, _attribute_it{std::move(attribute_it)}, _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_attribute_it;
      ++_chunk_offset;
    }

    void decrement() {
      --_attribute_it;
      --_chunk_offset;
    }

    bool equal(const Iterator& other) const { return _attribute_it == other._attribute_it; }

    void advance(std::ptrdiff_t n) {
      _attribute_it += n;
      _chunk_offset += n;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._attribute_it - _attribute_it; }

    SegmentPosition<ValueID> dereference() const {
      const auto value_id = static_cast<ValueID>(*_attribute_it);
      const auto is_null = (value_id == _null_value_id);

      return {value_id, is_null, _chunk_offset};
    }

   private:
    const ValueID _null_value_id;
    CompressedVectorIterator _attribute_it;
    ChunkOffset _chunk_offset;
  };

  template <typename Decompressor, typename PosListIteratorType>
  class PointAccessIterator
      : public AbstractPointAccessSegmentIterator<PointAccessIterator<Decompressor, PosListIteratorType>,
                                                  SegmentPosition<ValueID>, PosListIteratorType> {
   public:
    using ValueType = ValueID;
    PointAccessIterator(const ValueID null_value_id, Decompressor&& attribute_decompressor,
                        const PosListIteratorType&& position_filter_begin, PosListIteratorType&& position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator<Decompressor, PosListIteratorType>,
                                             SegmentPosition<ValueID>, PosListIteratorType>{std::move(
                                                                                                position_filter_begin),
                                                                                            std::move(
                                                                                                position_filter_it)},
          _null_value_id{null_value_id},
          _attribute_decompressor{std::move(attribute_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<ValueID> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto value_id = static_cast<ValueID>(_attribute_decompressor.get(chunk_offsets.offset_in_referenced_chunk));
      const auto is_null = (value_id == _null_value_id);

      return {value_id, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const ValueID _null_value_id;
    mutable Decompressor _attribute_decompressor;
  };
};

}  // namespace opossum
