#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
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

template <typename T>
class ValueSegmentAccessor : public AbstractSegmentAccessor<T> {
 public:
  explicit ValueSegmentAccessor(const ValueSegment<T>& segment) : AbstractSegmentAccessor<T>{}, _segment{segment} {}

  const std::optional<T> access(ChunkOffset offset) const final { return _segment.get_typed_value(offset); }

  const void* get_void_ptr(ChunkOffset offset) const final {
    if (_segment.is_null(offset)) {
      return nullptr;
    }

    return &(_segment.values()[offset]);
  }

 protected:
  const ValueSegment<T>& _segment;
};

template <typename T>
class DictionarySegmentAccessor : public AbstractSegmentAccessor<T> {
 public:
  explicit DictionarySegmentAccessor(const DictionarySegment<T>& segment)
      : AbstractSegmentAccessor<T>{},
        _segment{segment},
        _dictionary{segment.dictionary()},
        _decompressor{segment.attribute_vector()->create_base_decompressor()} {}

  const std::optional<T> access(ChunkOffset offset) const final { return _segment.get_typed_value(offset); }

  const void* get_void_ptr(ChunkOffset offset) const final {
    const auto value_id = _decompressor->get(offset);
    if (value_id == INVALID_VALUE_ID) {
      return nullptr;
    }
    return &((*_dictionary)[value_id]);
  }

 protected:
  const DictionarySegment<T>& _segment;
  const std::shared_ptr<const pmr_vector<T>> _dictionary;
  const std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> create_segment_accessor(const std::shared_ptr<const BaseSegment>& segment);

template <typename T>
std::unique_ptr<BaseSegmentAccessor> create_base_segment_accessor(const std::shared_ptr<const BaseSegment>& segment);

/**
 * For ReferenceSegments, we don't use the SegmentAccessor but either the MultipleChunkReferenceSegmentAccessor or the.
 * SingleChunkReferenceSegmentAccessor. The first one is generally applicable. For each offset that is accessed, a new
 * accessor has to be created. This is because we cannot be sure that two consecutive offsets reference the same chunk.
 * In the SingleChunkReferenceSegmentAccessor, we know that the same chunk is referenced, so we create the accessor
 * only once.
 */
template <typename T>
class MultipleChunkReferenceSegmentAccessor : public AbstractSegmentAccessor<T> {
 public:
  static const size_t AVG_CHUNKS = 10;

  explicit MultipleChunkReferenceSegmentAccessor(const ReferenceSegment& segment)
      : MultipleChunkReferenceSegmentAccessor(segment, AVG_CHUNKS) {}

  MultipleChunkReferenceSegmentAccessor(const ReferenceSegment& segment, size_t max_chunk_id)
      : _table{segment.referenced_table()}, _segment{segment}, _accessors{max_chunk_id + 1} {
    for (const auto& row : *segment.pos_list()) {
      const auto& chunk_id = row.chunk_id;

      if (row.is_null()) {
        continue;
      }

      if (static_cast<size_t>(chunk_id) >= _accessors.size()) {
        _accessors.resize(static_cast<size_t>(chunk_id) + AVG_CHUNKS);
      }

      const auto& accessor = _accessors[chunk_id];
      if (!accessor) {
        _accessors[chunk_id] = std::move(
            create_segment_accessor<T>(_table->get_chunk(chunk_id)->get_segment(_segment.referenced_column_id())));
      }
    }
  }

  const std::optional<T> access(ChunkOffset offset) const final {
    const auto& referenced_row_id = (*_segment.pos_list())[offset];
    if (referenced_row_id.is_null()) return std::nullopt;

    const auto& table = _segment.referenced_table();
    const auto referenced_column_id = _segment.referenced_column_id();
    const auto referenced_chunk_id = referenced_row_id.chunk_id;
    const auto referenced_chunk_offset = referenced_row_id.chunk_offset;
    const auto accessor =
        create_segment_accessor<T>(table->get_chunk(referenced_chunk_id)->get_segment(referenced_column_id));
    return accessor->access(referenced_chunk_offset);
  }

  const void* get_void_ptr(ChunkOffset offset) const final {
    const auto& row_id = (*_segment.pos_list())[offset];
    if (row_id.is_null()) {
      return nullptr;
    }
    const auto& accessor = _accessors[row_id.chunk_id];
    return accessor->get_void_ptr(row_id.chunk_offset);
  }

 protected:
  const std::shared_ptr<const Table> _table;
  const ReferenceSegment& _segment;
  std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> _accessors;
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

  const void* get_void_ptr(ChunkOffset offset) const final {
    const auto referenced_chunk_offset = (*_segment.pos_list())[offset].chunk_offset;
    return _accessor->get_void_ptr(referenced_chunk_offset);
  }

 protected:
  class NullAccessor : public AbstractSegmentAccessor<T> {
    const std::optional<T> access(ChunkOffset offset) const final { return std::nullopt; }
  };

  const ReferenceSegment& _segment;
  const ChunkID _chunk_id;
  const std::unique_ptr<AbstractSegmentAccessor<T>> _accessor;
};

template <typename T>
std::unique_ptr<BaseSegmentAccessor> create_base_segment_accessor(const std::shared_ptr<const BaseSegment>& segment) {
  return create_segment_accessor<T>(segment);
}

}  // namespace opossum
