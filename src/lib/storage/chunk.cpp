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
#include "reference_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

// The last commit id is reserved for uncommitted changes
const CommitID Chunk::MAX_COMMIT_ID = std::numeric_limits<CommitID>::max() - 1;

// The last chunk offset is reserved for NULL as used in ReferenceColumns.
const ChunkOffset Chunk::MAX_SIZE = std::numeric_limits<ChunkOffset>::max() - 1;

Chunk::Chunk(UseMvcc mvcc_mode, ChunkUseAccessCounter counter_mode) : Chunk({}, mvcc_mode, counter_mode) {}

Chunk::Chunk(const PolymorphicAllocator<Chunk>& alloc, const std::shared_ptr<AccessCounter> access_counter)
    : _alloc(alloc), _access_counter(access_counter) {}

Chunk::Chunk(const PolymorphicAllocator<Chunk>& alloc, const UseMvcc mvcc_mode,
             const ChunkUseAccessCounter counter_mode)
    : _alloc(alloc), _columns(alloc), _indices(alloc) {
  if (mvcc_mode == UseMvcc::Yes) _mvcc_columns = std::make_shared<MvccColumns>();
  if (counter_mode == ChunkUseAccessCounter::Yes) _access_counter = std::make_shared<AccessCounter>(alloc);
}

void Chunk::add_column(std::shared_ptr<BaseColumn> column) {
  // The added column must have the same size as the chunk.
  DebugAssert((_columns.size() <= 0 || size() == column->size()),
              "Trying to add column with mismatching size to chunk");

  if (_columns.size() == 0 && has_mvcc_columns()) grow_mvcc_column_size_by(column->size(), 0);

  _columns.push_back(column);
}

void Chunk::replace_column(size_t column_id, std::shared_ptr<BaseColumn> column) {
  std::atomic_store(&_columns.at(column_id), column);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  // Do this first to ensure that the first thing to exist in a row are the MVCC columns.
  if (has_mvcc_columns()) grow_mvcc_column_size_by(1u, Chunk::MAX_COMMIT_ID);

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

std::shared_ptr<BaseColumn> Chunk::get_mutable_column(ColumnID column_id) const {
  return std::atomic_load(&_columns.at(column_id));
}

std::shared_ptr<const BaseColumn> Chunk::get_column(ColumnID column_id) const {
  return std::atomic_load(&_columns.at(column_id));
}

uint16_t Chunk::column_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) return 0;
  auto first_column = get_column(ColumnID{0});
  return first_column->size();
}

void Chunk::grow_mvcc_column_size_by(size_t delta, CommitID begin_cid) {
  auto mvcc_columns = this->mvcc_columns();

  mvcc_columns->tids.grow_to_at_least(size() + delta);
  mvcc_columns->begin_cids.grow_to_at_least(size() + delta, begin_cid);
  mvcc_columns->end_cids.grow_to_at_least(size() + delta, MAX_COMMIT_ID);
}

bool Chunk::has_mvcc_columns() const { return _mvcc_columns != nullptr; }
bool Chunk::has_access_counter() const { return _access_counter != nullptr; }

SharedScopedLockingPtr<Chunk::MvccColumns> Chunk::mvcc_columns() {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  return {*_mvcc_columns, _mvcc_columns->_mutex};
}

SharedScopedLockingPtr<const Chunk::MvccColumns> Chunk::mvcc_columns() const {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  return {*_mvcc_columns, _mvcc_columns->_mutex};
}

void Chunk::shrink_mvcc_columns() {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  std::unique_lock<std::shared_mutex> lock{_mvcc_columns->_mutex};

  _mvcc_columns->tids.shrink_to_fit();
  _mvcc_columns->begin_cids.shrink_to_fit();
  _mvcc_columns->end_cids.shrink_to_fit();
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(
    const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto result = std::vector<std::shared_ptr<BaseIndex>>();
  std::copy_if(_indices.cbegin(), _indices.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(columns); });
  return result;
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(const std::vector<ColumnID> column_ids) const {
  auto columns = get_columns_for_ids(column_ids);
  return get_indices(columns);
}

std::shared_ptr<BaseIndex> Chunk::get_index(const ColumnIndexType index_type,
                                            const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto index_it = std::find_if(_indices.cbegin(), _indices.cend(), [&](const auto& index) {
    return index->is_index_for(columns) && index->type() == index_type;
  });

  return (index_it == _indices.cend()) ? nullptr : *index_it;
}

std::shared_ptr<BaseIndex> Chunk::get_index(const ColumnIndexType index_type,
                                            const std::vector<ColumnID> column_ids) const {
  auto columns = get_columns_for_ids(column_ids);
  return get_index(index_type, columns);
}

void Chunk::remove_index(std::shared_ptr<BaseIndex> index) {
  auto it = std::find(_indices.cbegin(), _indices.cend(), index);
  DebugAssert(it != _indices.cend(), "Trying to remove a non-existing index");
  _indices.erase(it);
}

bool Chunk::references_exactly_one_table() const {
  if (column_count() == 0) return false;

  auto first_column = std::dynamic_pointer_cast<const ReferenceColumn>(get_column(ColumnID{0}));
  if (first_column == nullptr) return false;
  auto first_referenced_table = first_column->referenced_table();
  auto first_pos_list = first_column->pos_list();

  for (ColumnID column_id{1}; column_id < column_count(); ++column_id) {
    const auto column = std::dynamic_pointer_cast<const ReferenceColumn>(get_column(column_id));
    if (column == nullptr) return false;

    if (first_referenced_table != column->referenced_table()) return false;

    if (first_pos_list != column->pos_list()) return false;
  }

  return true;
}

void Chunk::migrate(boost::container::pmr::memory_resource* memory_source) {
  // Migrating chunks with indices is not implemented yet.
  if (_indices.size() > 0) {
    Fail("Cannot migrate Chunk with Indices.");
  }

  _alloc = PolymorphicAllocator<size_t>(memory_source);
  pmr_concurrent_vector<std::shared_ptr<BaseColumn>> new_columns(_alloc);
  for (size_t i = 0; i < _columns.size(); i++) {
    new_columns.push_back(_columns.at(i)->copy_using_allocator(_alloc));
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
  // TODO(anybody) AccessCounter memory usage missing

  if (_mvcc_columns) {
    bytes += sizeof(_mvcc_columns->tids) + sizeof(_mvcc_columns->begin_cids) + sizeof(_mvcc_columns->end_cids);
    bytes += _mvcc_columns->tids.size() * sizeof(decltype(_mvcc_columns->tids)::value_type);
    bytes += _mvcc_columns->begin_cids.size() * sizeof(decltype(_mvcc_columns->begin_cids)::value_type);
    bytes += _mvcc_columns->end_cids.size() * sizeof(decltype(_mvcc_columns->end_cids)::value_type);
  }

  return bytes;
}

uint64_t Chunk::AccessCounter::history_sample(size_t lookback) const {
  if (_history.size() < 2 || lookback == 0) return 0;
  const auto last = _history.back();
  const auto prelast_index =
      std::max(static_cast<int64_t>(0), static_cast<int64_t>(_history.size()) - static_cast<int64_t>(lookback));
  const auto prelast = _history.at(prelast_index);
  return last - prelast;
}

std::vector<std::shared_ptr<const BaseColumn>> Chunk::get_columns_for_ids(
    const std::vector<ColumnID>& column_ids) const {
  DebugAssert(([&]() {
                for (auto column_id : column_ids)
                  if (column_id >= column_count()) return false;
                return true;
              }()),
              "Column IDs not within range [0, column_count()).");

  auto columns = std::vector<std::shared_ptr<const BaseColumn>>{};
  columns.reserve(column_ids.size());
  std::transform(column_ids.cbegin(), column_ids.cend(), std::back_inserter(columns),
                 [&](const auto& column_id) { return get_column(column_id); });
  return columns;
}

}  // namespace opossum
