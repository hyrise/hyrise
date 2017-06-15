#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_column.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

Table::Table(const size_t chunk_size) : _chunk_size(chunk_size), _append_mutex(std::make_unique<std::mutex>()) {
  _chunks.push_back(Chunk{true});
}

void Table::add_column(const std::string &name, const std::string &type, bool create_value_column) {
  Assert((name.size() < std::numeric_limits<ColumnNameLength>::max()), "Cannot add column. Column name is too long.");

  _column_names.push_back(name);
  _column_types.push_back(type);
  if (create_value_column) {
    for (auto &chunk : _chunks) {
      chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
    }
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // TODO(Anyone): Chunks should be preallocated for chunk size
  if (_chunk_size > 0 && _chunks.back().size() == _chunk_size) create_new_chunk();

  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  Chunk newChunk{true};
  for (auto &&type : _column_types) {
    newChunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
  _chunks.push_back(std::move(newChunk));
}

uint16_t Table::col_count() const { return _column_types.size(); }

uint32_t Table::row_count() const {
  size_t ret = 0;
  for (auto &&chunk : _chunks) {
    ret += chunk.size();
  }
  return ret;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string &column_name) const {
  for (ColumnID column_id = 0; column_id < col_count(); ++column_id) {
    // TODO(Anyone): make more efficient
    if (_column_names[column_id] == column_name) {
      return column_id;
    }
  }
  throw std::logic_error("column " + column_name + " not found");
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::string &Table::column_name(ColumnID column_id) const { return _column_names[column_id]; }

const std::string &Table::column_type(ColumnID column_id) const { return _column_types[column_id]; }

const std::vector<std::string> &Table::column_types() const { return _column_types; }

Chunk &Table::get_chunk(ChunkID chunk_id) { return _chunks[chunk_id]; }
const Chunk &Table::get_chunk(ChunkID chunk_id) const { return _chunks[chunk_id]; }

void Table::add_chunk(Chunk chunk) {
  if (_chunks.size() == 1 && _chunks.back().col_count() == 0) {
    // the initial chunk was not used yet
    _chunks.clear();
  }
  DebugAssert((_chunks.size() == 0 || chunk.col_count() == col_count()),
              std::string("adding chunk with ") + std::to_string(chunk.col_count()) + " columns to table with " +
                  std::to_string(col_count()) + " columns");
  _chunks.emplace_back(std::move(chunk));
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

}  // namespace opossum
