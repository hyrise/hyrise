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

const CommitID Chunk::MAX_COMMIT_ID = std::numeric_limits<CommitID>::max();

Chunk::Chunk(ChunkUseMvcc mvcc_mode) : Chunk({}, mvcc_mode) {}

Chunk::Chunk(const PolymorphicAllocator<Chunk>& alloc, ChunkUseMvcc mvcc_mode) : _columns(alloc), _indices(alloc) {
  if (mvcc_mode == ChunkUseMvcc::Yes) {
    _mvcc_columns = std::make_shared<MvccColumns>();
  }
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

void Chunk::use_mvcc_columns_from(const Chunk& chunk) {
  Assert(chunk.has_mvcc_columns(), "Passed chunk needs to have mvcc columns.");
  _mvcc_columns = chunk._mvcc_columns;
}

bool Chunk::has_mvcc_columns() const { return _mvcc_columns != nullptr; }

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

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices_for(
    const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto result = std::vector<std::shared_ptr<BaseIndex>>();
  std::copy_if(_indices.cbegin(), _indices.cend(), std::back_inserter(result),
               [&columns](const std::shared_ptr<BaseIndex>& index) { return index->is_index_for(columns); });
  return result;
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

}  // namespace opossum
