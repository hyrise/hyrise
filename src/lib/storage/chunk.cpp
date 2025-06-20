#include "chunk.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "all_type_variant.hpp"
#include "base_value_segment.hpp"
#include "index/abstract_chunk_index.hpp"
#include "reference_segment.hpp"
#include "storage/index/chunk_index_type.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/atomic_max.hpp"

namespace hyrise {

Chunk::Chunk(Segments segments, const std::shared_ptr<MvccData>& mvcc_data, PolymorphicAllocator<Chunk> alloc,
             Indexes indexes)
    : _alloc(alloc), _segments(std::move(segments)), _mvcc_data(mvcc_data), _indexes(std::move(indexes)) {
  DebugAssert(!_segments.empty(),
              "Chunks without segments are not legal, as the row count of such a chunk cannot be determined.");

  if constexpr (HYRISE_DEBUG) {
    const auto is_reference_chunk =
        !_segments.empty() ? std::dynamic_pointer_cast<ReferenceSegment>(_segments.front()) != nullptr : false;

    for (const auto& segment : _segments) {
      Assert(segment, "Segment must not be nullptr.");
      Assert(!mvcc_data || !std::dynamic_pointer_cast<ReferenceSegment>(segment),
             "Chunks containing ReferenceSegments should not contain MvccData. They implicitly use the MvccData of the "
             "referenced table.");
      Assert((std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr) == is_reference_chunk,
             "Chunk can either contain only ReferenceSegments or only non-ReferenceSegments.");
    }
  }
}

bool Chunk::is_mutable() const {
  return _is_mutable.load();
}

void Chunk::replace_segment(size_t column_id, const std::shared_ptr<AbstractSegment>& segment) {
  std::atomic_store(&_segments.at(column_id), segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(is_mutable(), "Cannot append to immutable chunk.");

  if (has_mvcc_data()) {
    // Make the row visible - mvcc_data has been pre-allocated
    mvcc_data()->set_begin_cid(size(), UNSET_COMMIT_ID);
  }

  // The added values, i.e., a new row, must have the same number of attributes as the table.
  DebugAssert((_segments.size() == values.size()),
              ("append: number of segments (" + std::to_string(_segments.size()) + ") does not match value list (" +
               std::to_string(values.size()) + ")."));

  auto segment_it = _segments.cbegin();
  auto value_it = values.begin();
  for (; segment_it != _segments.end(); segment_it++, value_it++) {
    const auto& base_value_segment = std::dynamic_pointer_cast<BaseValueSegment>(*segment_it);
    DebugAssert(base_value_segment, "Cannot append to segment that is not a ValueSegment.");
    base_value_segment->append(*value_it);
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(ColumnID column_id) const {
  return std::atomic_load(&_segments.at(column_id));
}

ColumnCount Chunk::column_count() const {
  return ColumnCount{static_cast<ColumnCount::base_type>(_segments.size())};
}

ChunkOffset Chunk::size() const {
  if (_segments.empty()) {
    return ChunkOffset{0};
  }
  const auto first_segment = get_segment(ColumnID{0});
  return static_cast<ChunkOffset>(first_segment->size());
}

bool Chunk::has_mvcc_data() const {
  return _mvcc_data != nullptr;
}

std::shared_ptr<MvccData> Chunk::mvcc_data() const {
  return _mvcc_data;
}

std::vector<std::shared_ptr<AbstractChunkIndex>> Chunk::get_indexes(
    const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const {
  auto result = std::vector<std::shared_ptr<AbstractChunkIndex>>();
  std::copy_if(_indexes.cbegin(), _indexes.cend(), std::back_inserter(result), [&](const auto& index) {
    return index->is_index_for(segments);
  });
  return result;
}

void Chunk::set_immutable() {
  auto success = true;
  Assert(_is_mutable.compare_exchange_strong(success, false), "Only mutable chunks can be set immutable.");
  DebugAssert(success, "Value exchanged but value was actually false.");

  // Only perform the `max_begin_cid` check if it has not already been set.
  if (has_mvcc_data() && _mvcc_data->max_begin_cid.load() == MAX_COMMIT_ID) {
    const auto chunk_size = size();
    Assert(chunk_size > 0, "`set_immutable()` should not be called on an empty chunk.");
    auto max_begin_cid = CommitID{0};
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      max_begin_cid = std::max(max_begin_cid, _mvcc_data->get_begin_cid(chunk_offset));
    }
    set_atomic_max(_mvcc_data->max_begin_cid, max_begin_cid);

    Assert(_mvcc_data->max_begin_cid != MAX_COMMIT_ID,
           "`max_begin_cid` should not be MAX_COMMIT_ID when marking a chunk as immutable.");
  }
}

std::vector<std::shared_ptr<AbstractChunkIndex>> Chunk::get_indexes(const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_indexes(segments);
}

std::shared_ptr<AbstractChunkIndex> Chunk::get_index(
    const ChunkIndexType index_type, const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const {
  auto index_it = std::find_if(_indexes.cbegin(), _indexes.cend(), [&](const auto& index) {
    return index->is_index_for(segments) && index->type() == index_type;
  });

  return (index_it == _indexes.cend()) ? nullptr : *index_it;
}

std::shared_ptr<AbstractChunkIndex> Chunk::get_index(const ChunkIndexType index_type,
                                                     const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_index(index_type, segments);
}

void Chunk::remove_index(const std::shared_ptr<AbstractChunkIndex>& index) {
  auto it = std::find(_indexes.cbegin(), _indexes.cend(), index);
  DebugAssert(it != _indexes.cend(), "Trying to remove a non-existing index.");
  _indexes.erase(it);
}

bool Chunk::references_exactly_one_table() const {
  if (column_count() == 0) {
    return false;
  }

  auto first_segment = std::dynamic_pointer_cast<const ReferenceSegment>(get_segment(ColumnID{0}));
  if (!first_segment) {
    return false;
  }
  auto first_referenced_table = first_segment->referenced_table();
  auto first_pos_list = first_segment->pos_list();

  for (auto column_id = ColumnID{1}; column_id < column_count(); ++column_id) {
    const auto segment = std::dynamic_pointer_cast<const ReferenceSegment>(get_segment(column_id));
    if (!segment) {
      return false;
    }

    if (first_referenced_table != segment->referenced_table()) {
      return false;
    }

    if (first_pos_list != segment->pos_list()) {
      return false;
    }
  }

  return true;
}

void Chunk::migrate(MemoryResource& memory_resource) {
  // Migrating chunks with indexes is not implemented yet.
  if (!_indexes.empty()) {
    Fail("Cannot migrate chunk with indexes.");
  }

  auto new_segments = Segments(&memory_resource);
  for (const auto& segment : _segments) {
    new_segments.push_back(segment->copy_using_memory_resource(memory_resource));
  }
  _segments = std::move(new_segments);
}

const PolymorphicAllocator<Chunk>& Chunk::get_allocator() const {
  return _alloc;
}

size_t Chunk::memory_usage(const MemoryUsageCalculationMode mode) const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& segment : _segments) {
    bytes += segment->memory_usage(mode);
  }

  // TODO(anybody) Index memory usage missing

  if (_mvcc_data) {
    bytes += _mvcc_data->memory_usage();
  }

  return bytes;
}

std::vector<std::shared_ptr<const AbstractSegment>> Chunk::_get_segments_for_ids(
    const std::vector<ColumnID>& column_ids) const {
  if constexpr (HYRISE_DEBUG) {
    const auto number_of_columns = static_cast<ColumnID>(column_count());
    for (const auto& column_id : column_ids) {
      Assert(column_id < number_of_columns, "ColumnID " + std::to_string(column_id) +
                                                " exceeds the maximum column index which is " +
                                                std::to_string(column_count() - 1) + ".");
    }
  }

  auto segments = std::vector<std::shared_ptr<const AbstractSegment>>{};
  segments.reserve(column_ids.size());
  std::transform(column_ids.cbegin(), column_ids.cend(), std::back_inserter(segments), [&](const auto& column_id) {
    return get_segment(column_id);
  });
  return segments;
}

const std::optional<ChunkPruningStatistics>& Chunk::pruning_statistics() const {
  return _pruning_statistics;
}

void Chunk::set_pruning_statistics(const std::optional<ChunkPruningStatistics>& pruning_statistics) {
  Assert(!is_mutable(), "Cannot set pruning statistics on mutable chunks.");
  Assert(!pruning_statistics || pruning_statistics->size() == static_cast<size_t>(column_count()),
         "Pruning statistics must have same number of segments as chunk.");

  _pruning_statistics = pruning_statistics;
}

void Chunk::increase_invalid_row_count(const ChunkOffset count, const std::memory_order memory_order) const {
  _invalid_row_count.fetch_add(count, memory_order);
}

const std::vector<SortColumnDefinition>& Chunk::individually_sorted_by() const {
  return _sorted_by;
}

void Chunk::set_individually_sorted_by(const SortColumnDefinition& sorted_by) {
  set_individually_sorted_by(std::vector<SortColumnDefinition>{sorted_by});
}

void Chunk::set_individually_sorted_by(const std::vector<SortColumnDefinition>& sorted_by) {
  Assert(!is_mutable(), "Cannot set_individually_sorted_by on mutable chunks.");
  // Currently, we assume that set_individually_sorted_by is called only once at most.
  // As such, there should be no existing sorting and the new sorting should contain at least one column.
  // Feel free to remove this assertion if necessary.
  Assert(!sorted_by.empty() && _sorted_by.empty(), "Sorting information cannot be empty or reset.");

  if constexpr (HYRISE_DEBUG) {
    for (const auto& sorted_by_column : sorted_by) {
      const auto& sorted_segment = get_segment(sorted_by_column.column);
      if (sorted_segment->size() < 2) {
        break;
      }

      segment_with_iterators(*sorted_segment, [&](auto begin, auto end) {
        Assert(std::is_sorted(begin, end,
                              [sort_mode = sorted_by_column.sort_mode](const auto& left, const auto& right) {
                                // is_sorted evaluates the segment by calling the lambda with the SegmentPositions at
                                // it+n and it (n being non-negative), which needs to evaluate to false.
                                if (right.is_null()) {
                                  return false;  // handles right side is NULL and both are NULL
                                }
                                if (left.is_null()) {
                                  return true;
                                }
                                const auto ascending = sort_mode == SortMode::Ascending;
                                return ascending ? left.value() < right.value() : left.value() > right.value();
                              }),
               "Setting a sort order for a segment which is not sorted accordingly.");
      });
    }
  }

  _sorted_by = sorted_by;
}

std::optional<CommitID> Chunk::get_cleanup_commit_id() const {
  if (_cleanup_commit_id.load() == UNSET_COMMIT_ID) {
    // Cleanup-Commit-ID is not yet set
    return std::nullopt;
  }
  return std::optional<CommitID>{_cleanup_commit_id.load()};
}

void Chunk::set_cleanup_commit_id(const CommitID cleanup_commit_id) {
  Assert(!get_cleanup_commit_id(), "Cleanup-commit-ID can only be set once.");
  _cleanup_commit_id.store(cleanup_commit_id);
}

void Chunk::mark_as_full() {
  Assert(!_reached_target_size, "Chunk should not be marked as full multiple times.");
  _reached_target_size = true;
}

void Chunk::try_set_immutable() {
  DebugAssert(_mvcc_data, "Expected to be executed with MVCC enabled.");
  // Mark the chunk as immutable if (i) it reached the target size and a new chunk was added to the table, (ii) it is
  // still mutable, and (iii) all pending Insert operators are either committed or rolled back. We do not have to set
  // the `max_begin_cid` here because committed Insert operators already set it.
  if (!_reached_target_size || !is_mutable() || _mvcc_data->pending_inserts() != 0) {
    return;
  }

  // Mark chunk as immutable. `fetch_and() is only defined for integral types, so we use `compare_exchange_strong()`.
  auto success = true;
  if (_is_mutable.compare_exchange_strong(success, false)) {
    // We were the first ones to mark the chunk as immutable. Thus, we have to take care of anything else that needs to
    // be done. In the future, this can mean to start background statistics generation, encoding, etc.
    Assert(success, "Value exchanged but value was actually false.");
  } else {
    // Another thread is about to mark this chunk as immutable. Do nothing.
    Assert(!success, "Value not exchanged but value was actually true.");
  }
}

}  // namespace hyrise
