#pragma once

#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include "storage/base_segment_accessor.hpp"
#include "storage/dictionary_segment.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

namespace detail {

// We want to instantiate create_segment_accessor() for all data types, but our EXPLICITLY_INSTANTIATE_DATA_TYPES macro
// only supports classes. So we wrap create_segment_accessor() in this class and instantiate the class in the .cpp
template <typename T>
class CreateSegmentAccessor {
 public:
  static std::unique_ptr<AbstractSegmentAccessor<T>> create(const std::shared_ptr<const AbstractSegment>& segment);
};

EXPLICITLY_DECLARE_DATA_TYPES(CreateSegmentAccessor);

}  // namespace detail

/**
 * Utility method to create a SegmentAccessor for a given AbstractSegment.
 */
template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> create_segment_accessor(
    const std::shared_ptr<const AbstractSegment>& segment) {
  return hyrise::detail::CreateSegmentAccessor<T>::create(segment);
}

/**
 * A SegmentAccessor is templated per SegmentType and DataType (T).
 * It requires that the underlying segment implements an implicit interface:
 *
 *   std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;
 *
 * Accessors are not guaranteed to be thread-safe. For multiple threads that access the same segment, create one
 * accessor each.
 */
template <typename T, typename SegmentType>
class SegmentAccessor final : public AbstractSegmentAccessor<T> {
 public:
  SegmentAccessor(const SegmentAccessor&) = default;
  SegmentAccessor(SegmentAccessor&&) = default;
  // These are deleted because `_segment` is a const reference
  SegmentAccessor& operator=(const SegmentAccessor&) = delete;
  SegmentAccessor& operator=(SegmentAccessor&&) = delete;

  explicit SegmentAccessor(const SegmentType& segment) : AbstractSegmentAccessor<T>{}, _segment{segment} {
    if constexpr (std::is_same_v<SegmentType, ValueSegment<T>>) {
     _ptr = const_cast<T*>(_segment.values().data());
   }
  }

  std::optional<T> access(ChunkOffset offset) const final {
    ++_accesses;
    // if constexpr (std::is_same_v<SegmentType, ValueSegment<T>>) {
      // std::cerr << std::format("(acc_o:{}-v:{}-ptr:", static_cast<size_t>(offset), _ptr[offset]) << &_ptr[offset] << ")";
    // }
    return _segment.get_typed_value(offset);
  }

  void prefetch(ChunkOffset offset) const override final {
    if constexpr (std::is_same_v<SegmentType, ValueSegment<T>>) {
      // std::cerr << std::format("(pref_o:{}-v:{}-ptr:", static_cast<size_t>(offset), _ptr[offset]) << &_ptr[offset] << ")";
      __builtin_prefetch(&_ptr[offset], 0, 0);
    }
  }

  ~SegmentAccessor() override {
    _segment.access_counter[SegmentAccessCounter::AccessType::Random] += _accesses;
  }

 protected:
  mutable uint64_t _accesses{0};
  const SegmentType& _segment;
  T* _ptr;
};

/**
 * For ReferenceSegments, we do not use the SegmentAccessor but either the MultipleChunkReferenceSegmentAccessor or the
 * SingleChunkReferenceSegmentAccessor. The first one is generally applicable. However, we will have more overhead,
 * because we cannot be sure that two consecutive offsets reference the same chunk. In the
 * SingleChunkReferenceSegmentAccessor, we know that the same chunk is referenced, so we create the accessor only once.
 */
template <typename T>
class MultipleChunkReferenceSegmentAccessor final : public AbstractSegmentAccessor<T> {
 public:
  explicit MultipleChunkReferenceSegmentAccessor(const ReferenceSegment& segment)
      : _segment{segment}, _table{segment.referenced_table()} {}

  std::optional<T> access(ChunkOffset offset) const final {
    const auto& pos_list = *_segment.pos_list();
    const auto& row_id = pos_list[offset];
    if (row_id.is_null()) {
      return std::nullopt;
    }

    const auto chunk_id = row_id.chunk_id;

    // Grow the _accessors vector faster than linearly if the chunk_id is out of its current bounds
    if (static_cast<size_t>(chunk_id) >= _accessors.size()) {
      _accessors.resize(static_cast<size_t>(chunk_id + _accessors.size()));
    }

    if (!_accessors[chunk_id].second) {
      const auto segment = _table->get_chunk(chunk_id)->get_segment(_segment.referenced_column_id());
      const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
      _accessors[chunk_id] =
          {value_segment != nullptr, create_segment_accessor<T>(std::move(segment))};
    }

    if (_accessors[chunk_id].first && (offset+32 < pos_list.size())) {
      _accessors[chunk_id].second->prefetch(pos_list[offset+32].chunk_offset);
    }

    return _accessors[chunk_id].second->access(row_id.chunk_offset);
  }

 protected:
  const ReferenceSegment& _segment;
  const std::shared_ptr<const Table> _table;
  // Serves as a "dictionary" from ChunkID to Accessor. Lazily increased in size as Chunks are accessed.
  mutable std::vector<std::pair<bool, std::unique_ptr<AbstractSegmentAccessor<T>>>> _accessors{1};
};

// Accessor for ReferenceSegments that reference single chunks - see comment above
template <typename T, typename Segment>
class SingleChunkReferenceSegmentAccessor final : public AbstractSegmentAccessor<T> {
 public:
  SingleChunkReferenceSegmentAccessor(const SingleChunkReferenceSegmentAccessor&) = default;
  SingleChunkReferenceSegmentAccessor(SingleChunkReferenceSegmentAccessor&&) = default;
  SingleChunkReferenceSegmentAccessor& operator=(const SingleChunkReferenceSegmentAccessor&) = default;
  SingleChunkReferenceSegmentAccessor& operator=(SingleChunkReferenceSegmentAccessor&&) = default;

  explicit SingleChunkReferenceSegmentAccessor(const AbstractPosList& pos_list, const ChunkID chunk_id,
                                               const Segment& segment)
      : _pos_list{pos_list}, _chunk_id(chunk_id), _segment(segment) {}

  std::optional<T> access(ChunkOffset offset) const final {
    ++_accesses;
    const auto referenced_chunk_offset = _pos_list[offset].chunk_offset;
    return _segment.get_typed_value(referenced_chunk_offset);
  }

  ~SingleChunkReferenceSegmentAccessor() override {
    _segment.access_counter[SegmentAccessCounter::AccessType::Random] += _accesses;
  }

 protected:
  mutable uint64_t _accesses{0};
  const AbstractPosList& _pos_list;
  const ChunkID _chunk_id;
  const Segment& _segment;
};

// Accessor for ReferenceSegments that reference only NULL values
template <typename T>
class NullAccessor final : public AbstractSegmentAccessor<T> {
 public:
  std::optional<T> access(ChunkOffset /*offset*/) const final {
    return std::nullopt;
  }
};

}  // namespace hyrise
