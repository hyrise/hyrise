#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

namespace opossum {

std::shared_ptr<Table> Table::create_with_layout_from(const std::shared_ptr<const Table>& in_table,
                                                      const uint32_t max_chunk_size) {
  auto new_table = std::make_shared<Table>(max_chunk_size);

  for (ColumnID::base_type column_idx = 0; column_idx < in_table->column_count(); ++column_idx) {
    const auto type = in_table->column_type(ColumnID{column_idx});
    const auto name = in_table->column_name(ColumnID{column_idx});
    const auto is_nullable = in_table->column_is_nullable(ColumnID{column_idx});

    new_table->add_column_definition(name, type, is_nullable);
  }

  return new_table;
}

bool Table::layouts_equal(const std::shared_ptr<const Table>& left, const std::shared_ptr<const Table>& right) {
  if (left->column_count() != right->column_count()) {
    return false;
  }

  for (auto column_id = ColumnID{0}; column_id < left->column_count(); ++column_id) {
    if (left->column_type(column_id) != right->column_type(column_id)) {
      return false;
    }
    if (left->column_name(column_id) != right->column_name(column_id)) {
      return false;
    }
  }

  return true;
}

Table::Table(const uint32_t max_chunk_size)
    : _max_chunk_size(max_chunk_size), _append_mutex(std::make_unique<std::mutex>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
  _chunks.push_back(std::make_shared<Chunk>(UseMvcc::Yes));
}

void Table::add_column_definition(const std::string& name, DataType data_type, bool nullable) {
  Assert((name.size() < std::numeric_limits<ColumnNameLength>::max()), "Cannot add column. Column name is too long.");

  _column_names.push_back(name);
  _column_types.push_back(data_type);
  _column_nullable.push_back(nullable);
}

void Table::add_column(const std::string& name, DataType data_type, bool nullable) {
  add_column_definition(name, data_type, nullable);

  for (auto& chunk : _chunks) {
    chunk->add_column(make_shared_by_data_type<BaseColumn, ValueColumn>(data_type, nullable));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // TODO(Anyone): Chunks should be preallocated for chunk size
  if (_chunks.back()->size() == _max_chunk_size) create_new_chunk();

  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  // Create chunk with mvcc columns
  auto new_chunk = std::make_shared<Chunk>(UseMvcc::Yes);

  for (auto column_id = 0u; column_id < _column_types.size(); ++column_id) {
    const auto type = _column_types[column_id];
    auto nullable = _column_nullable[column_id];

    new_chunk->add_column(make_shared_by_data_type<BaseColumn, ValueColumn>(type, nullable));
  }
  _chunks.push_back(std::move(new_chunk));
}

uint16_t Table::column_count() const { return _column_types.size(); }

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk->size();
  }
  return ret;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (ColumnID column_id{0}; column_id < column_count(); ++column_id) {
    // TODO(Anyone): make more efficient
    if (_column_names[column_id] == column_name) {
      return column_id;
    }
  }
  Fail("Column " + column_name + " not found.");
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const {
  DebugAssert(column_id < _column_names.size(), "ColumnID " + std::to_string(column_id) + " out of range");
  return _column_names[column_id];
}

DataType Table::column_type(ColumnID column_id) const {
  DebugAssert(column_id < _column_names.size(), "ColumnID " + std::to_string(column_id) + " out of range");
  return _column_types[column_id];
}

bool Table::column_is_nullable(ColumnID column_id) const {
  DebugAssert(column_id < _column_names.size(), "ColumnID " + std::to_string(column_id) + " out of range");
  return _column_nullable[column_id];
}

const std::vector<DataType>& Table::column_types() const { return _column_types; }

const std::vector<bool>& Table::column_nullables() const { return _column_nullable; }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

const ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

void Table::emplace_chunk(const std::shared_ptr<Chunk>& chunk) {
  if (_chunks.size() == 1 && (_chunks.back()->column_count() == 0 || _chunks.back()->size() == 0)) {
    // the initial chunk was not used yet
    _chunks.clear();
  }
  DebugAssert(chunk->column_count() > 0, "Trying to add chunk without columns.");
  DebugAssert(chunk->column_count() == column_count(),
              std::string("adding chunk with ") + std::to_string(chunk->column_count()) + " columns to table with " +
                  std::to_string(column_count()) + " columns");
  _chunks.emplace_back(chunk);
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

TableType Table::get_type() const {
  // Cannot answer this if the table has no content
  Assert(!_chunks.empty() && column_count() > 0, "Table has no content, can't specify type");

  // We assume if one column is a reference column, all are.
  const auto column = _chunks[0]->get_column(ColumnID{0});
  const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);

  if (ref_column != nullptr) {
// In debug mode we're pedantic and check whether all columns in all chunks are Reference Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < column_count(); ++column_idx) {
        const auto column2 = _chunks[chunk_idx]->get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 != nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::References;
  } else {
// In debug mode we're pedantic and check whether all columns in all chunks are Value/Dict Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < column_count(); ++column_idx) {
        const auto column2 = _chunks[chunk_idx]->get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 == nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::Data;
  }
}

std::vector<IndexInfo> Table::get_indexes() const { return _indexes; }

size_t Table::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& chunk : _chunks) {
    bytes += chunk->estimate_memory_usage();
  }

  for (const auto& column_name : _column_names) {
    bytes += column_name.size();
  }

  bytes += _column_types.size() * sizeof(decltype(_column_types)::value_type);
  bytes += _column_nullable.size() * sizeof(decltype(_column_nullable)::value_type);

  // TODO(anybody) Statistics and Indices missing from Memory Usage Estimation

  return bytes;
}

}  // namespace opossum
