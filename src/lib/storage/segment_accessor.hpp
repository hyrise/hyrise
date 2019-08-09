#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "storage/base_segment_accessor.hpp"
#include "storage/dictionary_segment.hpp"
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
 * Accessors are not guaranteed to be thread-safe. For multiple threads that access the same segment, create one
 * accessor each.
 */
template <typename T, typename SegmentType>
class SegmentAccessor final : public AbstractSegmentAccessor<T> {
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
class MultipleChunkReferenceSegmentAccessor final : public AbstractSegmentAccessor<T> {
 public:
  explicit MultipleChunkReferenceSegmentAccessor(const ReferenceSegment& segment)
      : _segment{segment}, _table{segment.referenced_table()}, _accessors{1} {}

  const std::optional<T> access(ChunkOffset offset) const final {
    const auto& row_id = (*_segment.pos_list())[offset];
    if (row_id.is_null()) {
      return std::nullopt;
    }

    const auto chunk_id = row_id.chunk_id;

    // Grow the _accessors vector faster than linearly if the chunk_id is out of its current bounds
    if (static_cast<size_t>(chunk_id) >= _accessors.size()) {
      _accessors.resize(static_cast<size_t>(chunk_id + _accessors.size()));
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
  // Serves as a "dictionary" from ChunkID to Accessor. Lazily increased in size as Chunks are accessed.
  mutable std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> _accessors;
};

// Accessor for ReferenceSegments that reference single chunks - see comment above
template <typename T, typename Segment>
class SingleChunkReferenceSegmentAccessor final : public AbstractSegmentAccessor<T> {
 public:
  explicit SingleChunkReferenceSegmentAccessor(const PosList& pos_list, const ChunkID chunk_id, const Segment& segment)
      : _pos_list{pos_list}, _chunk_id(chunk_id), _segment(segment) {}

  const std::optional<T> access(ChunkOffset offset) const final {
    const auto referenced_chunk_offset = _pos_list[offset].chunk_offset;
    return _segment.get_typed_value(referenced_chunk_offset);
  }

 protected:
  const PosList& _pos_list;
  const ChunkID _chunk_id;
  const Segment& _segment;
};

// Accessor for ReferenceSegments that reference only NULL values
template <typename T>
class NullAccessor final : public AbstractSegmentAccessor<T> {
  const std::optional<T> access(ChunkOffset offset) const final { return std::nullopt; }
};

}  // namespace opossum
