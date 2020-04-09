#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "storage/base_segment_accessor.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/segment_iterables/segment_positions.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

namespace detail {

// We want to instantiate create_segment_accessor() for all data types, but our EXPLICITLY_INSTANTIATE_DATA_TYPES macro
// only supports classes. So we wrap create_segment_accessor() in this class and instantiate the class in the .cpp
template <typename T>
class CreateSegmentAccessor {
 public:
  static std::unique_ptr<AbstractSegmentAccessor<T>> create(const std::shared_ptr<const BaseSegment>& segment);
  static std::unique_ptr<AbstractSegmentAccessor<T>> create(const std::shared_ptr<const BaseSegment>& segment, const std::shared_ptr<const RowIDPosList>& pos_list);
};

}  // namespace detail

/**
 * Utility method to create a SegmentAccessor for a given BaseSegment.
 */
template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> create_segment_accessor(const std::shared_ptr<const BaseSegment>& segment) {
  return opossum::detail::CreateSegmentAccessor<T>::create(segment);
}
template <typename T>
std::unique_ptr<AbstractSegmentAccessor<T>> create_segment_accessor(const std::shared_ptr<const BaseSegment>& segment, const std::shared_ptr<const RowIDPosList>& pos_list) {
  return opossum::detail::CreateSegmentAccessor<T>::create(segment, pos_list);
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
template <typename T, typename Segment>
class SegmentAccessor final : public AbstractSegmentAccessor<T> {
 public:
  explicit SegmentAccessor(const Segment& segment) : AbstractSegmentAccessor<T>{}, _segment{segment} {}

  const std::optional<T> access(ChunkOffset offset) const final {
    ++_accesses;
    return _segment.get_typed_value(offset);
  }

  ~SegmentAccessor() { _segment.access_counter[SegmentAccessCounter::AccessType::Random] += _accesses; }

 protected:
  mutable uint64_t _accesses{0};
  const Segment& _segment;
};

template <typename T, typename Segment, typename Iterable>
class SegmentAccessor2 final : public AbstractSegmentAccessor<T> {
 public:
  explicit SegmentAccessor2(const Segment& segment, const std::shared_ptr<const AbstractPosList>& pos_list) :
    AbstractSegmentAccessor<T>{}, _segment{segment}, _pos_list{pos_list} {
      std::cout << "SegmentAccessor2" << std::endl;
      // Gather referenced positions for iterables
      std::vector<std::shared_ptr<RowIDPosList>> pos_lists_per_segment{8};

      auto max_chunk_id = ChunkID{0};
      for (const auto position : *pos_list) { // TODO: back to auto& ????
        const auto chunk_id = position.chunk_id;
        max_chunk_id = std::max(chunk_id, max_chunk_id);
        if (static_cast<size_t>(chunk_id) >= pos_lists_per_segment.size()) {
          pos_lists_per_segment.resize(static_cast<size_t>(chunk_id * 2));  // grow fast, don't care about a little bit of oversizing here.
        }

        if (!pos_lists_per_segment[chunk_id]) {
          pos_lists_per_segment[chunk_id] = std::make_shared<RowIDPosList>();
          pos_lists_per_segment[chunk_id]->reserve(64);
        }
        pos_lists_per_segment[chunk_id]->emplace_back(position);
      }

      std::vector<std::vector<SegmentPosition<T>>> segment_positions_per_segment{max_chunk_id + 1};
      for (auto chunk_id = size_t{0}; chunk_id < pos_lists_per_segment.size(); ++chunk_id) {
        const auto& iterable_pos_list = pos_lists_per_segment[chunk_id];
        if (!iterable_pos_list) {
          continue;
        }
        iterable_pos_list->guarantee_single_chunk();

        auto& segment_positions = segment_positions_per_segment[chunk_id];
        segment_positions.reserve(iterable_pos_list->size());

        const auto iterable = create_iterable_from_segment<T>(segment);
        iterable.with_iterators(iterable_pos_list, [&](auto it, const auto end) {
          while (it != end) {
            segment_positions.emplace_back(SegmentPosition<T>{it->value(), it->is_null(), it->chunk_offset()});
            ++it;
          }
        });
      }

      _segment_positions.reserve(pos_list->size());
      std::vector<size_t> segment_position_list_offsets(max_chunk_id + 1, 0ul);
      for (const auto position : *pos_list) {
        const auto chunk_id = position.chunk_id;

        const auto& segment_position = segment_positions_per_segment[chunk_id][segment_position_list_offsets[chunk_id]];
        _segment_positions.emplace_back(SegmentPosition<T>{segment_position.value(), segment_position.is_null(), segment_position.chunk_offset()});
        const auto insert = _tmp_pos_list_offset_mapping.emplace(position.chunk_offset, _segment_positions.size() - 1);
        DebugAssert(insert.second, "Inserted chunk offset " + std::to_string(position.chunk_offset) + " twice");

        ++segment_position_list_offsets[chunk_id];
      }
    }

  const std::optional<T> access(ChunkOffset offset) const final {
    // if constexpr (std::is_same_v<Segment, DictionarySegment<T>> || std::is_same_v<Segment, ValueSegment<T>>) {
    if constexpr (std::is_same_v<Segment, DictionarySegment<T>>) {
      ++_accesses;
      const auto referenced_chunk_offset = (*_pos_list)[offset].chunk_offset;
      return _segment.get_typed_value(referenced_chunk_offset);
    } else {
      PerformanceWarning("iterating ...");
      const auto segment_position = _segment_positions[_tmp_pos_list_offset_mapping.at(offset)];
      if (segment_position.is_null()) return std::nullopt;
      return segment_position.value();

      // for (const auto& pos : *_pos_list) {
      //   std::cout << pos << std::endl;
      // }
      // std::cout << std::endl;
      Fail("Desired offset (" + std::to_string(offset) + ") is not in pos list.");
    }
  }

  ~SegmentAccessor2() { _segment.access_counter[SegmentAccessCounter::AccessType::Random] += _accesses; }

 protected:
  mutable uint64_t _accesses{0};  // TODO: remove once iterators are properly used.
  const Segment& _segment;
  const std::shared_ptr<const AbstractPosList>& _pos_list;

  std::unordered_map<size_t, size_t> _tmp_pos_list_offset_mapping;

  std::vector<SegmentPosition<T>> _segment_positions;
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
      : _segment{segment}, _table{segment.referenced_table()}, _accessors{8} {}

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
  explicit SingleChunkReferenceSegmentAccessor(const AbstractPosList& pos_list, const ChunkID chunk_id, const Segment& segment)
      : _pos_list{pos_list}, _chunk_id(chunk_id), _segment(segment) {
      }

  const std::optional<T> access(ChunkOffset offset) const final {
    ++_accesses;
    const auto referenced_chunk_offset = _pos_list[offset].chunk_offset;
    return _segment.get_typed_value(referenced_chunk_offset);
  }

  ~SingleChunkReferenceSegmentAccessor() {
    _segment.access_counter[SegmentAccessCounter::AccessType::Random] += _accesses;
  }

 protected:
  mutable uint64_t _accesses{0};
  const AbstractPosList& _pos_list;
  const ChunkID _chunk_id;
  const Segment& _segment;
};

// Accessor for ReferenceSegments that reference single chunks - see comment above
template <typename T, typename Segment, typename Iterable>
class SingleChunkReferenceSegmentAccessor2 final : public AbstractSegmentAccessor<T> {
 public:
  explicit SingleChunkReferenceSegmentAccessor2(const std::shared_ptr<const AbstractPosList>& pos_list, const ChunkID chunk_id, const Segment& segment)
      : _pos_list{pos_list}, _chunk_id(chunk_id), _segment(segment), _iterable{create_iterable_from_segment<T>(segment)} {
        std::cout << "SingleChunkReferenceSegmentAccessor2" << std::endl;
        _segment_positions.reserve(pos_list->size());

        _iterable.with_iterators(pos_list, [&](auto it, const auto end) {
          while (it != end) {
            _segment_positions.emplace_back(SegmentPosition<T>{it->value(), it->is_null(), it->chunk_offset()});
            ++it;
          }
        });
      }

  const std::optional<T> access(ChunkOffset offset) const final {
    if constexpr (std::is_same_v<Segment, DictionarySegment<T>>) {
      ++_accesses;
      const auto referenced_chunk_offset = (*_pos_list)[offset].chunk_offset;
      return _segment.get_typed_value(referenced_chunk_offset);
    } else {
      PerformanceWarning("iterating ...");
      for (const auto& segment_position : _segment_positions) {
        if (segment_position.chunk_offset() == offset) {
          if (segment_position.is_null()) return std::nullopt;
          return segment_position.value();
        }
      }
      Fail("Desired offset not in pos list.");
    }
  }

  ~SingleChunkReferenceSegmentAccessor2() {
    _segment.access_counter[SegmentAccessCounter::AccessType::Random] += _accesses;
  }

 protected:
  mutable uint64_t _accesses{0};
  const std::shared_ptr<const AbstractPosList>& _pos_list;
  const ChunkID _chunk_id;
  const Segment& _segment;
  const Iterable _iterable;

  std::vector<SegmentPosition<T>> _segment_positions;
};

// Accessor for ReferenceSegments that reference only NULL values
template <typename T>
class NullAccessor final : public AbstractSegmentAccessor<T> {
  const std::optional<T> access(ChunkOffset offset) const final { return std::nullopt; }
};

}  // namespace opossum
