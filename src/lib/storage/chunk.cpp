#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"
#include "index/abstract_index.hpp"
#include "reference_segment.hpp"
#include "resolve_type.hpp"
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

void Chunk::replace_segment(size_t column_id, const std::shared_ptr<BaseSegment>& segment) {
  std::atomic_store(&_segments.at(column_id), segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(is_mutable(), "Can't append to immutable Chunk");

  // Do this first to ensure that the first thing to exist in a row is the MVCC data.
  if (has_mvcc_data()) get_scoped_mvcc_data_lock()->grow_by(1u, INVALID_TRANSACTION_ID, CommitID{0});

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

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  return std::atomic_load(&_segments.at(column_id));
}

ColumnCount Chunk::column_count() const { return ColumnCount{static_cast<ColumnCount::base_type>(_segments.size())}; }

uint32_t Chunk::size() const {
  if (_segments.empty()) return 0;
  const auto first_segment = get_segment(ColumnID{0});
  return static_cast<uint32_t>(first_segment->size());
}

bool Chunk::has_mvcc_data() const { return _mvcc_data != nullptr; }

SharedScopedLockingPtr<MvccData> Chunk::get_scoped_mvcc_data_lock() const {
  DebugAssert((has_mvcc_data()), "Chunk does not have mvcc data");

  return {*_mvcc_data, _mvcc_data->_mutex};
}

std::shared_ptr<MvccData> Chunk::mvcc_data() const { return _mvcc_data; }

std::vector<std::shared_ptr<AbstractIndex>> Chunk::get_indexes(
    const std::vector<std::shared_ptr<const BaseSegment>>& segments) const {
  auto result = std::vector<std::shared_ptr<AbstractIndex>>();
  std::copy_if(_indexes.cbegin(), _indexes.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(segments); });
  return result;
}

void Chunk::finalize() {
  Assert(is_mutable(), "Only mutable chunks can be finalized. Chunks cannot be finalized twice.");
  _is_mutable = false;

  if (has_mvcc_data()) {
    auto mvcc = get_scoped_mvcc_data_lock();
    Assert(!mvcc->begin_cids.empty(), "Cannot calculate max_begin_cid on an empty begin_cid vector.");

    mvcc->max_begin_cid = *(std::max_element(mvcc->begin_cids.begin(), mvcc->begin_cids.end()));
    Assert(mvcc->max_begin_cid != MvccData::MAX_COMMIT_ID,
           "max_begin_cid should not be MAX_COMMIT_ID when finalizing a chunk. This probably means the chunk was "
           "finalized before all transactions committed/rolled back.");
  }
}

std::vector<std::shared_ptr<AbstractIndex>> Chunk::get_indexes(const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_indexes(segments);
}

std::shared_ptr<AbstractIndex> Chunk::get_index(const SegmentIndexType index_type,
                                                const std::vector<std::shared_ptr<const BaseSegment>>& segments) const {
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

size_t Chunk::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& segment : _segments) {
    bytes += segment->estimate_memory_usage();
  }

  // TODO(anybody) Index memory usage missing

  if (_mvcc_data) {
    bytes += sizeof(_mvcc_data->tids) + sizeof(_mvcc_data->begin_cids) + sizeof(_mvcc_data->end_cids);
    bytes += _mvcc_data->tids.size() * sizeof(decltype(_mvcc_data->tids)::value_type);
    bytes += _mvcc_data->begin_cids.size() * sizeof(decltype(_mvcc_data->begin_cids)::value_type);
    bytes += _mvcc_data->end_cids.size() * sizeof(decltype(_mvcc_data->end_cids)::value_type);
  }

  return bytes;
}

std::vector<std::shared_ptr<const BaseSegment>> Chunk::_get_segments_for_ids(
    const std::vector<ColumnID>& column_ids) const {
  DebugAssert(([&]() {
                for (auto column_id : column_ids)
                  if (column_id >= static_cast<ColumnID>(column_count())) return false;
                return true;
              }()),
              "column ids not within range [0, column_count()).");

  auto segments = std::vector<std::shared_ptr<const BaseSegment>>{};
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

void Chunk::set_cleanup_commit_id(const CommitID cleanup_commit_id) {
  DebugAssert(!_cleanup_commit_id, "Cleanup commit ID can only be set once.");
  _cleanup_commit_id = cleanup_commit_id;
}

const std::optional<std::pair<ColumnID, OrderByMode>>& Chunk::ordered_by() const { return _ordered_by; }

void Chunk::set_ordered_by(const std::pair<ColumnID, OrderByMode>& ordered_by) { _ordered_by.emplace(ordered_by); }

}  // namespace opossum
