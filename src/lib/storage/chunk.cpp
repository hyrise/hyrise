#include "chunk.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "index/abstract_index.hpp"
#include "reference_segment.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/assert.hpp"

namespace opossum {

Chunk::Chunk(Segments segments, const std::shared_ptr<MvccData>& mvcc_data,
             const std::optional<PolymorphicAllocator<Chunk>>& alloc, Indexes indexes)
    : _segments(std::move(segments)), _mvcc_data(mvcc_data), _indexes(std::move(indexes)) {
  DebugAssert(!_segments.empty(),
              "Chunks without Segments are not legal, as the row count of such a Chunk cannot be determined");

  if constexpr (HYRISE_DEBUG) {
    const auto is_reference_chunk =
        !_segments.empty() ? std::dynamic_pointer_cast<ReferenceSegment>(_segments.front()) != nullptr : false;

    for (const auto& segment : _segments) {
      Assert(segment, "Segment must not be nullptr");
      Assert(!mvcc_data || !std::dynamic_pointer_cast<ReferenceSegment>(segment),
             "Chunks containing ReferenceSegments should not contain MvccData. They implicitly use the MvccData of the "
             "referenced Table");
      Assert((std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr) == is_reference_chunk,
             "Chunk can either contain only ReferenceSegments or only non-ReferenceSegments");
    }
  }

  if (alloc) _alloc = *alloc;
}

bool Chunk::is_mutable() const { return _is_mutable; }

void Chunk::replace_segment(size_t column_id, const std::shared_ptr<AbstractSegment>& segment) {
  std::atomic_store(&_segments.at(column_id), segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(is_mutable(), "Can't append to immutable Chunk");

  if (has_mvcc_data()) {
    // Make the row visible - mvcc_data has been pre-allocated
    mvcc_data()->set_begin_cid(size(), CommitID{0});
  }

  // The added values, i.e., a new row, must have the same number of attributes as the table.
  DebugAssert((_segments.size() == values.size()),
              ("append: number of segments (" + std::to_string(_segments.size()) + ") does not match value list (" +
               std::to_string(values.size()) + ")"));

  auto segment_it = _segments.cbegin();
  auto value_it = values.begin();
  for (; segment_it != _segments.end(); segment_it++, value_it++) {
    const auto& base_value_segment = std::dynamic_pointer_cast<BaseValueSegment>(*segment_it);
    DebugAssert(base_value_segment, "Can't append to segment that is not a ValueSegment");
    base_value_segment->append(*value_it);
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(ColumnID column_id) const {
  return std::atomic_load(&_segments.at(column_id));
}

ColumnCount Chunk::column_count() const { return ColumnCount{static_cast<ColumnCount::base_type>(_segments.size())}; }

ChunkOffset Chunk::size() const {
  if (_segments.empty()) return 0;
  const auto first_segment = get_segment(ColumnID{0});
  return static_cast<ChunkOffset>(first_segment->size());
}

bool Chunk::has_mvcc_data() const { return _mvcc_data != nullptr; }

std::shared_ptr<MvccData> Chunk::mvcc_data() const { return _mvcc_data; }

std::vector<std::shared_ptr<AbstractIndex>> Chunk::get_indexes(
    const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const {
  auto result = std::vector<std::shared_ptr<AbstractIndex>>();
  std::copy_if(_indexes.cbegin(), _indexes.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(segments); });
  return result;
}

void Chunk::finalize() {
  Assert(is_mutable(), "Only mutable chunks can be finalized. Chunks cannot be finalized twice.");
  _is_mutable = false;

  // Only perform the max_begin_cid check if it hasn't already been set.
  if (has_mvcc_data() && !_mvcc_data->max_begin_cid) {
    const auto chunk_size = size();
    Assert(chunk_size > 0, "finalize() should not be called on an empty chunk");
    _mvcc_data->max_begin_cid = CommitID{0};
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      _mvcc_data->max_begin_cid = std::max(*_mvcc_data->max_begin_cid, _mvcc_data->get_begin_cid(chunk_offset));
    }

    Assert(_mvcc_data->max_begin_cid != MvccData::MAX_COMMIT_ID,
           "max_begin_cid should not be MAX_COMMIT_ID when finalizing a chunk. This probably means the chunk was "
           "finalized before all transactions committed/rolled back.");
  }
}

std::vector<std::shared_ptr<AbstractIndex>> Chunk::get_indexes(const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_indexes(segments);
}

std::shared_ptr<AbstractIndex> Chunk::get_index(
    const SegmentIndexType index_type, const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const {
  auto index_it = std::find_if(_indexes.cbegin(), _indexes.cend(), [&](const auto& index) {
    return index->is_index_for(segments) && index->type() == index_type;
  });

  return (index_it == _indexes.cend()) ? nullptr : *index_it;
}

std::shared_ptr<AbstractIndex> Chunk::get_index(const SegmentIndexType index_type,
                                                const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_index(index_type, segments);
}

void Chunk::remove_index(const std::shared_ptr<AbstractIndex>& index) {
  auto it = std::find(_indexes.cbegin(), _indexes.cend(), index);
  DebugAssert(it != _indexes.cend(), "Trying to remove a non-existing index");
  _indexes.erase(it);
}

bool Chunk::references_exactly_one_table() const {
  if (column_count() == 0) return false;

  auto first_segment = std::dynamic_pointer_cast<const ReferenceSegment>(get_segment(ColumnID{0}));
  if (!first_segment) return false;
  auto first_referenced_table = first_segment->referenced_table();
  auto first_pos_list = first_segment->pos_list();

  for (ColumnID column_id{1}; column_id < column_count(); ++column_id) {
    const auto segment = std::dynamic_pointer_cast<const ReferenceSegment>(get_segment(column_id));
    if (!segment) return false;

    if (first_referenced_table != segment->referenced_table()) return false;

    if (first_pos_list != segment->pos_list()) return false;
  }

  return true;
}

void Chunk::migrate(boost::container::pmr::memory_resource* memory_source) {
  // Migrating chunks with indexes is not implemented yet.
  if (!_indexes.empty()) {
    Fail("Cannot migrate Chunk with Indexes.");
  }

  _alloc = PolymorphicAllocator<size_t>(memory_source);
  Segments new_segments(_alloc);
  for (const auto& segment : _segments) {
    new_segments.push_back(segment->copy_using_allocator(_alloc));
  }
  _segments = std::move(new_segments);
}

const PolymorphicAllocator<Chunk>& Chunk::get_allocator() const { return _alloc; }

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
  DebugAssert(([&]() {
                for (auto column_id : column_ids)
                  if (column_id >= static_cast<ColumnID>(column_count())) return false;
                return true;
              }()),
              "column ids not within range [0, column_count()).");

  auto segments = std::vector<std::shared_ptr<const AbstractSegment>>{};
  segments.reserve(column_ids.size());
  std::transform(column_ids.cbegin(), column_ids.cend(), std::back_inserter(segments),
                 [&](const auto& column_id) { return get_segment(column_id); });
  return segments;
}

const std::optional<ChunkPruningStatistics>& Chunk::pruning_statistics() const { return _pruning_statistics; }

void Chunk::set_pruning_statistics(const std::optional<ChunkPruningStatistics>& pruning_statistics) {
  Assert(!is_mutable(), "Cannot set pruning statistics on mutable chunks.");
  Assert(!pruning_statistics || pruning_statistics->size() == static_cast<size_t>(column_count()),
         "Pruning statistics must have same number of segments as Chunk");

  _pruning_statistics = pruning_statistics;
}
void Chunk::increase_invalid_row_count(const uint32_t count) const { _invalid_row_count += count; }

const std::vector<SortColumnDefinition>& Chunk::individually_sorted_by() const { return _sorted_by; }

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
      if (sorted_segment->size() < 2) break;

      segment_with_iterators(*sorted_segment, [&](auto begin, auto end) {
        Assert(std::is_sorted(begin, end,
                              [sort_mode = sorted_by_column.sort_mode](const auto& left, const auto& right) {
                                // is_sorted evaluates the segment by calling the lambda with the SegmentPositions at
                                // it+n and it (n being non-negative), which needs to evaluate to false.
                                if (right.is_null()) return false;  // handles right side is NULL and both are NULL
                                if (left.is_null()) return true;
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
  if (_cleanup_commit_id == 0) {
    // Cleanup-Commit-ID is not yet set
    return std::nullopt;
  }
  return std::optional<CommitID>{_cleanup_commit_id.load()};
}

void Chunk::set_cleanup_commit_id(const CommitID cleanup_commit_id) {
  Assert(!get_cleanup_commit_id(), "Cleanup-commit-ID can only be set once.");
  _cleanup_commit_id.store(cleanup_commit_id);
}

}  // namespace opossum
