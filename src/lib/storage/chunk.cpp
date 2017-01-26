#include <iomanip>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "chunk.hpp"
#include "value_column.hpp"

namespace opossum {

Chunk::Chunk() : Chunk{false} {}

Chunk::Chunk(const bool has_mvcc_columns) {
  if (has_mvcc_columns) _mvcc_columns = std::make_unique<MvccColumns>();
}

void Chunk::add_column(std::shared_ptr<BaseColumn> column) {
  // The added column must have the same size as the chunk.
  if (IS_DEBUG && _columns.size() > 0 && size() != column->size()) {
    throw std::runtime_error("Trying to add column with mismatching size to chunk");
  }
  if (_columns.size() == 0 && has_mvcc_columns()) set_mvcc_column_size(column->size(), 0);

  _columns.emplace_back(column);
}

void Chunk::append(std::vector<AllTypeVariant> values) {
  // Do this first to ensure that the first thing to exist in a row are the MVCC columns.
  if (has_mvcc_columns()) set_mvcc_column_size(size() + 1u, std::numeric_limits<uint32_t>::max());

  // The added values, i.e., a new row, must have the same number of attribues as the table.
  if (IS_DEBUG && _columns.size() != values.size()) {
    throw std::runtime_error("append: number of columns (" + to_string(_columns.size()) +
                             ") does not match value list (" + to_string(values.size()) + ")");
  }

  auto column_it = _columns.cbegin();
  auto value_it = values.begin();
  for (; column_it != _columns.end(); column_it++, value_it++) {
    (*column_it)->append(*value_it);
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(size_t column_id) const { return _columns.at(column_id); }

size_t Chunk::col_count() const { return _columns.size(); }

size_t Chunk::size() const {
  if (_columns.size() == 0) return 0;
  return _columns.front()->size();
}

void Chunk::set_mvcc_column_size(size_t new_size, uint32_t begin_cid) {
  _mvcc_columns->tids.grow_to_at_least(new_size);
  _mvcc_columns->begin_cids.grow_to_at_least(new_size, begin_cid);
  _mvcc_columns->end_cids.grow_to_at_least(new_size, std::numeric_limits<uint32_t>::max());
}

bool Chunk::has_mvcc_columns() const { return _mvcc_columns != nullptr; }

Chunk::MvccColumns& Chunk::mvcc_columns() {
#ifdef IS_DEBUG
  if (!has_mvcc_columns()) {
    std::logic_error("Chunk does not have mvcc columns");
  }
#endif

  return *_mvcc_columns;
}
const Chunk::MvccColumns& Chunk::mvcc_columns() const {
#ifdef IS_DEBUG
  if (!has_mvcc_columns()) {
    std::logic_error("Chunk does not have mvcc columns");
  }
#endif

  return *_mvcc_columns;
}
}  // namespace opossum
