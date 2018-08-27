#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"
#include "index/base_index.hpp"
#include "reference_segment.hpp"
#include "resolve_type.hpp"
#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

Chunk::Chunk(const ChunkSegments& columns, const std::shared_ptr<MvccData>& mvcc_data,
             const std::optional<PolymorphicAllocator<Chunk>>& alloc,
             const std::shared_ptr<ChunkAccessCounter>& access_counter)
    : _columns(columns), _mvcc_data(mvcc_data), _access_counter(access_counter) {
#if IS_DEBUG
  const auto chunk_size = columns.empty() ? 0u : columns[0]->size();
  Assert(!_mvcc_data || _mvcc_data->size() == chunk_size, "Invalid MvccData size");
  for (const auto& column : columns) {
    Assert(column->size() == chunk_size, "Columns don't have the same length");
  }
#endif

  if (alloc) _alloc = *alloc;
}

bool Chunk::is_mutable() const { return _is_mutable; }

void Chunk::mark_immutable() { _is_mutable = false; }

void Chunk::replace_column(size_t cxlumn_id, const std::shared_ptr<BaseSegment>& column) {
  std::atomic_store(&_columns.at(cxlumn_id), column);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(is_mutable(), "Can't append to immutable Chunk");

  // Do this first to ensure that the first thing to exist in a row are the MVCC columns.
  if (has_mvcc_data()) get_scoped_mvcc_data_lock()->grow_by(1u, MvccData::MAX_COMMIT_ID);

  // The added values, i.e., a new row, must have the same number of attributes as the table.
  DebugAssert((_columns.size() == values.size()),
              ("append: number of columns (" + std::to_string(_columns.size()) + ") does not match value list (" +
               std::to_string(values.size()) + ")"));

  auto column_it = _columns.cbegin();
  auto value_it = values.begin();
  for (; column_it != _columns.end(); column_it++, value_it++) {
    (*column_it)->append(*value_it);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_column(CxlumnID cxlumn_id) const {
  return std::atomic_load(&_columns.at(cxlumn_id));
}

const ChunkSegments& Chunk::columns() const { return _columns; }

uint16_t Chunk::cxlumn_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) return 0;
  auto first_column = get_column(CxlumnID{0});
  return first_column->size();
}

bool Chunk::has_mvcc_data() const { return _mvcc_data != nullptr; }
bool Chunk::has_access_counter() const { return _access_counter != nullptr; }

SharedScopedLockingPtr<MvccData> Chunk::get_scoped_mvcc_data_lock() {
  DebugAssert((has_mvcc_data()), "Chunk does not have mvcc columns");

  return {*_mvcc_data, _mvcc_data->_mutex};
}

SharedScopedLockingPtr<const MvccData> Chunk::get_scoped_mvcc_data_lock() const {
  DebugAssert((has_mvcc_data()), "Chunk does not have mvcc columns");

  return {*_mvcc_data, _mvcc_data->_mutex};
}

std::shared_ptr<MvccData> Chunk::mvcc_data() const { return _mvcc_data; }

void Chunk::set_mvcc_data(const std::shared_ptr<MvccData>& mvcc_data) { _mvcc_data = mvcc_data; }

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(
    const std::vector<std::shared_ptr<const BaseSegment>>& columns) const {
  auto result = std::vector<std::shared_ptr<BaseIndex>>();
  std::copy_if(_indices.cbegin(), _indices.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(columns); });
  return result;
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(const std::vector<CxlumnID>& cxlumn_ids) const {
  auto columns = _get_columns_for_ids(cxlumn_ids);
  return get_indices(columns);
}

std::shared_ptr<BaseIndex> Chunk::get_index(const ColumnIndexType index_type,
                                            const std::vector<std::shared_ptr<const BaseSegment>>& columns) const {
  auto index_it = std::find_if(_indices.cbegin(), _indices.cend(), [&](const auto& index) {
    return index->is_index_for(columns) && index->type() == index_type;
  });

  return (index_it == _indices.cend()) ? nullptr : *index_it;
}

std::shared_ptr<BaseIndex> Chunk::get_index(const ColumnIndexType index_type,
                                            const std::vector<CxlumnID>& cxlumn_ids) const {
  auto columns = _get_columns_for_ids(cxlumn_ids);
  return get_index(index_type, columns);
}

void Chunk::remove_index(const std::shared_ptr<BaseIndex>& index) {
  auto it = std::find(_indices.cbegin(), _indices.cend(), index);
  DebugAssert(it != _indices.cend(), "Trying to remove a non-existing index");
  _indices.erase(it);
}

bool Chunk::references_exactly_one_table() const {
  if (cxlumn_count() == 0) return false;

  auto first_column = std::dynamic_pointer_cast<const ReferenceSegment>(get_column(CxlumnID{0}));
  if (first_column == nullptr) return false;
  auto first_referenced_table = first_column->referenced_table();
  auto first_pos_list = first_column->pos_list();

  for (CxlumnID cxlumn_id{1}; cxlumn_id < cxlumn_count(); ++cxlumn_id) {
    const auto column = std::dynamic_pointer_cast<const ReferenceSegment>(get_column(cxlumn_id));
    if (column == nullptr) return false;

    if (first_referenced_table != column->referenced_table()) return false;

    if (first_pos_list != column->pos_list()) return false;
  }

  return true;
}

void Chunk::migrate(boost::container::pmr::memory_resource* memory_source) {
  // Migrating chunks with indices is not implemented yet.
  if (!_indices.empty()) {
    Fail("Cannot migrate Chunk with Indices.");
  }

  _alloc = PolymorphicAllocator<size_t>(memory_source);
  ChunkSegments new_columns(_alloc);
  for (const auto& column : _columns) {
    new_columns.push_back(column->copy_using_allocator(_alloc));
  }
  _columns = std::move(new_columns);
}

const PolymorphicAllocator<Chunk>& Chunk::get_allocator() const { return _alloc; }

size_t Chunk::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& column : _columns) {
    bytes += column->estimate_memory_usage();
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

std::vector<std::shared_ptr<const BaseSegment>> Chunk::_get_columns_for_ids(
    const std::vector<CxlumnID>& cxlumn_ids) const {
  DebugAssert(([&]() {
                for (auto cxlumn_id : cxlumn_ids)
                  if (cxlumn_id >= cxlumn_count()) return false;
                return true;
              }()),
              "Column IDs not within range [0, cxlumn_count()).");

  auto columns = std::vector<std::shared_ptr<const BaseSegment>>{};
  columns.reserve(cxlumn_ids.size());
  std::transform(cxlumn_ids.cbegin(), cxlumn_ids.cend(), std::back_inserter(columns),
                 [&](const auto& cxlumn_id) { return get_column(cxlumn_id); });
  return columns;
}

std::shared_ptr<ChunkStatistics> Chunk::statistics() const { return _statistics; }

void Chunk::set_statistics(const std::shared_ptr<ChunkStatistics>& chunk_statistics) {
  Assert(!is_mutable(), "Cannot set statistics on mutable chunks.");
  DebugAssert(chunk_statistics->statistics().size() == cxlumn_count(),
              "ChunkStatistics must have same column amount as Chunk");
  _statistics = chunk_statistics;
}

}  // namespace opossum
