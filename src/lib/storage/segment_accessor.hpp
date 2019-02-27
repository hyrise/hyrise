#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "storage/base_segment_accessor.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

namespace detail {

// We want to instantiate create_segment_accessor() for all data types, but our EXPLICITLY_INSTANTIATE_DATA_TYPES macro
// only supports classes. So we wrap create_segment_accessor() in this class and instantiate the class in the .cpp
template <typename T>
class CreateSegmentAccessor {
 public:
  static std::unique_ptr<AbstractSegmentAccessor<T>> create(const std::shared_ptr<const BaseSegment>& segment);
};

}  // namespace detail

/**
 * Utility method to create a SegmentAccessor for a given BaseSegment.
 */
template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> create_segment_accessor(const std::shared_ptr<const BaseSegment>& segment) {
  return opossum::detail::CreateSegmentAccessor<T>::create(segment);
}

/**
 * A SegmentAccessor is templated per SegmentType and DataType (T).
 * It requires that the underlying segment implements an implicit interface:
 *
 *   const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;
 *
 */
template <typename T, typename SegmentType>
class SegmentAccessor : public AbstractSegmentAccessor<T> {
 public:
  explicit SegmentAccessor(const SegmentType& segment) : AbstractSegmentAccessor<T>{}, _segment{segment} {}

  const std::optional<T> access(ChunkOffset offset) const final { return _segment.get_typed_value(offset); }

 protected:
  const SegmentType& _segment;
};

/**
 * For ReferenceSegments, we don't use the SegmentAccessor but either the MultipleChunkReferenceSegmentAccessor or the.
 * SingleChunkReferenceSegmentAccessor. The first one is generally applicable. However, we will have more overhead,
 * because we cannot be sure that two consecutive offsets reference the same chunk. In the
 * SingleChunkReferenceSegmentAccessor, we know that the same chunk is referenced, so we create the accessor only once.
 */
template <typename T>
class MultipleChunkReferenceSegmentAccessor : public AbstractSegmentAccessor<T> {
 public:
  /**
   * AVG_CHUNKS is a constant to estimate how many different chunks are to be expected within a ReferenceSegment.
   * It is used if the constructor isn't called with the maximum chunk_id within the ReferenceSegment.
   * Setting AVG_CHUNKS too small will lead to having to resize _accessors more than once.
   * If AVG_CHUNKS is set too big, _accessors will reserve more space than necessary.
   */
  static constexpr ChunkID AVG_CHUNKS{10u};

  explicit MultipleChunkReferenceSegmentAccessor(const ReferenceSegment& segment)
      : MultipleChunkReferenceSegmentAccessor(segment, AVG_CHUNKS) {}

  MultipleChunkReferenceSegmentAccessor(const ReferenceSegment& segment, ChunkID max_chunk_id)
      : _segment{segment}, _table{segment.referenced_table()}, _accessors{max_chunk_id + 1} {}

  const std::optional<T> access(ChunkOffset offset) const final {
    const auto& row_id = (*_segment.pos_list())[offset];
    if (row_id.is_null()) {
      return std::nullopt;
    }

    const auto chunk_id = row_id.chunk_id;

    if (static_cast<size_t>(chunk_id) >= _accessors.size()) {
      _accessors.resize(static_cast<size_t>(chunk_id + AVG_CHUNKS));
    }

    if (!_accessors[chunk_id]) {
      _accessors[chunk_id] =
          create_segment_accessor<T>(_table->get_chunk(chunk_id)->get_segment(_segment.referenced_column_id()));
    }

    return _accessors[chunk_id]->access(row_id.chunk_offset);
  }

 protected:
  const ReferenceSegment& _segment;
  const std::shared_ptr<const Table> _table;
  mutable std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> _accessors;
};

// Accessor for ReferenceSegments that reference single chunks - see comment above
template <typename T>
class SingleChunkReferenceSegmentAccessor : public AbstractSegmentAccessor<T> {
 public:
  explicit SingleChunkReferenceSegmentAccessor(const ReferenceSegment& segment)
      : _segment{segment},
        _chunk_id((*_segment.pos_list())[ChunkOffset{0}].chunk_id),
        // If *_segment.pos_list()[ChunkOffset{0}] is NULL, its chunk_id is INVALID_CHUNK_OFFSET. When the
        // SingleChunkReferenceSegmentAccessor is used, all entries reference the same chunk_id (INVALID_CHUNK_OFFSET).
        // Therefore, we can safely assume that all other entries are also NULL and always return std::nullopt.
        _accessor((*_segment.pos_list())[ChunkOffset{0}].is_null()
                      ? std::make_unique<NullAccessor>()
                      : create_segment_accessor<T>(segment.referenced_table()->get_chunk(_chunk_id)->get_segment(
                            _segment.referenced_column_id()))) {}

  const std::optional<T> access(ChunkOffset offset) const final {
    const auto referenced_chunk_offset = (*_segment.pos_list())[offset].chunk_offset;
    return _accessor->access(referenced_chunk_offset);
  }

 protected:
  class NullAccessor : public AbstractSegmentAccessor<T> {
    const std::optional<T> access(ChunkOffset offset) const final { return std::nullopt; }
  };

  const ReferenceSegment& _segment;
  const ChunkID _chunk_id;
  const std::unique_ptr<AbstractSegmentAccessor<T>> _accessor;
};

}  // namespace opossum
