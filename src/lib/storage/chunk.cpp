#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "chunk.hpp"
#include "value_column.hpp"

namespace opossum {

void Chunk::add_column(std::shared_ptr<BaseColumn> column) {
  // The added column must have the same size as the chunk.
  if (IS_DEBUG && _columns.size() > 0 && size() != column->size()) {
    throw std::runtime_error("Trying to add column with mismatching size to chunk");
  }
  _columns.emplace_back(column);
}

void Chunk::append(std::vector<AllTypeVariant> values) {
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

size_t Chunk::size() const {
  if (_columns.size() == 0) return 0;
  return _columns.front()->size();
}
}  // namespace opossum
