#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size(chunk_size), _append_mutex(std::make_unique<std::mutex>()) {
  _chunks.push_back(Chunk{true});
}

void Table::add_column_definition(const std::string &name, const std::string &type, bool nullable) {
  Assert((name.size() < std::numeric_limits<ColumnNameLength>::max()), "Cannot add column. Column name is too long.");

  _column_names.push_back(name);
  _column_types.push_back(type);
  _column_nullable.push_back(nullable);
}

void Table::add_column(const std::string &name, const std::string &type, bool nullable) {
  add_column_definition(name, type, nullable);

  for (auto &chunk : _chunks) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type, nullable));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // TODO(Anyone): Chunks should be preallocated for chunk size
  if (_chunk_size > 0 && _chunks.back().size() == _chunk_size) create_new_chunk();

  _chunks.back().append(values);
}

void Table::inc_invalid_row_count(uint64_t count) { _approx_invalid_row_count += count; }

void Table::create_new_chunk() {
  // Create chunk with mvcc columns
  Chunk newChunk{true};

  for (auto column_id = 0u; column_id < _column_types.size(); ++column_id) {
    const auto &type = _column_types[column_id];
    auto nullable = _column_nullable[column_id];

    newChunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type, nullable));
  }
  _chunks.push_back(std::move(newChunk));
}

uint16_t Table::col_count() const { return _column_types.size(); }

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (auto &&chunk : _chunks) {
    ret += chunk.size();
  }
  return ret;
}

uint64_t Table::approx_valid_row_count() const { return row_count() - _approx_invalid_row_count; }

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string &column_name) const {
  for (ColumnID column_id{0}; column_id < col_count(); ++column_id) {
    // TODO(Anyone): make more efficient
    if (_column_names[column_id] == column_name) {
      return column_id;
    }
  }
  Fail("Column " + column_name + " not found.");
  return {};
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string> &Table::column_names() const { return _column_names; }

const std::string &Table::column_name(ColumnID column_id) const { return _column_names[column_id]; }

const std::string &Table::column_type(ColumnID column_id) const { return _column_types[column_id]; }

bool Table::column_is_nullable(ColumnID column_id) const { return _column_nullable[column_id]; }

const std::vector<std::string> &Table::column_types() const { return _column_types; }

const std::vector<bool> &Table::column_nullables() const { return _column_nullable; }

Chunk &Table::get_chunk(ChunkID chunk_id) { return _chunks[chunk_id]; }
const Chunk &Table::get_chunk(ChunkID chunk_id) const { return _chunks[chunk_id]; }

void Table::add_chunk(Chunk chunk) {
  if (_chunks.size() == 1 && _chunks.back().col_count() == 0) {
    // the initial chunk was not used yet
    _chunks.clear();
  }
  DebugAssert(chunk.col_count() == col_count(),
              std::string("adding chunk with ") + std::to_string(chunk.col_count()) + " columns to table with " +
                  std::to_string(col_count()) + " columns");
  _chunks.emplace_back(std::move(chunk));
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

}  // namespace opossum
