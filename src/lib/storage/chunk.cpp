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
#include "index/base_index.hpp"
#include "reference_segment.hpp"
#include "resolve_type.hpp"
#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

Chunk::Chunk(const Segments& segments, const std::shared_ptr<MvccData>& mvcc_data,
             const std::optional<PolymorphicAllocator<Chunk>>& alloc,
             const std::shared_ptr<ChunkAccessCounter>& access_counter)
    : _segments(segments), _mvcc_data(mvcc_data), _access_counter(access_counter) {
#if IS_DEBUG
  const auto chunk_size = segments.empty() ? 0u : segments[0]->size();
  Assert(!_mvcc_data || _mvcc_data->size() == chunk_size, "Invalid MvccData size");
  for (const auto& segment : segments) {
    Assert(segment->size() == chunk_size, "Segments don't have the same length");
  }
#endif

  if (alloc) _alloc = *alloc;
}

bool Chunk::is_mutable() const { return _is_mutable; }

void Chunk::mark_immutable() { _is_mutable = false; }

void Chunk::replace_segment(size_t column_id, const std::shared_ptr<BaseSegment>& segment) {
  std::atomic_store(&_segments.at(column_id), segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(is_mutable(), "Can't append to immutable Chunk");

  // Do this first to ensure that the first thing to exist in a row are the MVCC data.
  if (has_mvcc_data()) get_scoped_mvcc_data_lock()->grow_by(1u, MvccData::MAX_COMMIT_ID);

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

const Segments& Chunk::segments() const { return _segments; }

uint16_t Chunk::column_count() const { return _segments.size(); }

uint32_t Chunk::size() const {
  if (_segments.empty()) return 0;
  auto first_segment = get_segment(ColumnID{0});
  return first_segment->size();
}

bool Chunk::has_mvcc_data() const { return _mvcc_data != nullptr; }
bool Chunk::has_access_counter() const { return _access_counter != nullptr; }

SharedScopedLockingPtr<MvccData> Chunk::get_scoped_mvcc_data_lock() {
  DebugAssert((has_mvcc_data()), "Chunk does not have mvcc data");

  return {*_mvcc_data, _mvcc_data->_mutex};
}

SharedScopedLockingPtr<const MvccData> Chunk::get_scoped_mvcc_data_lock() const {
  DebugAssert((has_mvcc_data()), "Chunk does not have mvcc data");

  return {*_mvcc_data, _mvcc_data->_mutex};
}

std::shared_ptr<MvccData> Chunk::mvcc_data() const { return _mvcc_data; }

void Chunk::set_mvcc_data(const std::shared_ptr<MvccData>& mvcc_data) { _mvcc_data = mvcc_data; }

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(
    const std::vector<std::shared_ptr<const BaseSegment>>& segments) const {
  auto result = std::vector<std::shared_ptr<BaseIndex>>();
  std::copy_if(_indices.cbegin(), _indices.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(segments); });
  return result;
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_indices(segments);
}

std::shared_ptr<BaseIndex> Chunk::get_index(const SegmentIndexType index_type,
                                            const std::vector<std::shared_ptr<const BaseSegment>>& segments) const {
  auto index_it = std::find_if(_indices.cbegin(), _indices.cend(), [&](const auto& index) {
    return index->is_index_for(segments) && index->type() == index_type;
  });

  return (index_it == _indices.cend()) ? nullptr : *index_it;
}

std::shared_ptr<BaseIndex> Chunk::get_index(const SegmentIndexType index_type,
                                            const std::vector<ColumnID>& column_ids) const {
  auto segments = _get_segments_for_ids(column_ids);
  return get_index(index_type, segments);
}

void Chunk::remove_index(const std::shared_ptr<BaseIndex>& index) {
  auto it = std::find(_indices.cbegin(), _indices.cend(), index);
  DebugAssert(it != _indices.cend(), "Trying to remove a non-existing index");
  _indices.erase(it);
}

bool Chunk::references_exactly_one_table() const {
  if (column_count() == 0) return false;

  auto first_segment = std::dynamic_pointer_cast<const ReferenceSegment>(get_segment(ColumnID{0}));
  if (first_segment == nullptr) return false;
  auto first_referenced_table = first_segment->referenced_table();
  auto first_pos_list = first_segment->pos_list();

  for (ColumnID column_id{1}; column_id < column_count(); ++column_id) {
    const auto segment = std::dynamic_pointer_cast<const ReferenceSegment>(get_segment(column_id));
    if (segment == nullptr) return false;

    if (first_referenced_table != segment->referenced_table()) return false;

    if (first_pos_list != segment->pos_list()) return false;
  }

  return true;
}

void Chunk::migrate(boost::container::pmr::memory_resource* memory_source) {
  // Migrating chunks with indices is not implemented yet.
  if (!_indices.empty()) {
    Fail("Cannot migrate Chunk with Indices.");
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
  // TODO(anybody) ChunkAccessCounter memory usage missing

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
                  if (column_id >= column_count()) return false;
                return true;
              }()),
              "column ids not within range [0, column_count()).");

  auto segments = std::vector<std::shared_ptr<const BaseSegment>>{};
  segments.reserve(column_ids.size());
  std::transform(column_ids.cbegin(), column_ids.cend(), std::back_inserter(segments),
                 [&](const auto& column_id) { return get_segment(column_id); });
  return segments;
}

std::shared_ptr<ChunkStatistics> Chunk::statistics() const { return _statistics; }

void Chunk::set_statistics(const std::shared_ptr<ChunkStatistics>& chunk_statistics) {
  Assert(!is_mutable(), "Cannot set statistics on mutable chunks.");
  DebugAssert(chunk_statistics->statistics().size() == column_count(),
              "ChunkStatistics must have same number of segments as Chunk");
  _statistics = chunk_statistics;
}

}  // namespace opossum
