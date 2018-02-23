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

TableColumnDefinition::TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable):
  name(name), data_type(data_type), nullable(nullable) {}

Table::Table(const TableColumnDefinitions& column_definitions,
             const TableType type,
             const UseMvcc use_mvcc,
             const uint32_t max_chunk_size)
    : _column_definitions(column_definitions),
      _type(type),
      _use_mvcc(use_mvcc),
      _max_chunk_size(max_chunk_size),
      _append_mutex(std::make_unique<std::mutex>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

const TableColumnDefinitions& Table::column_definitions() const {
  return _column_definitions;
}

TableType Table::type() const {
  return _type;
}

size_t Table::column_count() const {
  return _column_definitions.size();
}

const std::string& Table::column_name(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].name;
}

std::vector<std::string> Table::column_names() const {
  std::vector<std::string> names;
  names.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    names.emplace_back(column_definition.name);
  }
  return names;
}

DataType Table::column_data_type(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].data_type;
}

std::vector<DataType> Table::column_data_types() const {
  std::vector<DataType> data_types;
  data_types.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    data_types.emplace_back(column_definition.data_type);
  }
  return data_types;
}

bool Table::column_is_nullable(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].nullable;
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.empty() || _chunks->back()->size() >= _max_chunk_size) {
    append_value_column_chunk();
  }

  _chunks.back()->append(values);
}

void Table::append_mutable_chunk() {
  std::vector<std::shared_ptr<BaseColumn>> columns;
  for (const auto& column_definition : _column_definitions) {
    resolve_data_type(column_definition.data_type, [&](auto type) {
      using ColumnDataType = decltype(type)::type;
      columns.emplace_back(std::make_shared<ValueColumn<ColumnDataType>>());
    });
  }
  add_chunk_new(columns)
}

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk->size();
  }
  return ret;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

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

std::shared_ptr<Chunk> Table::add_chunk_new(const std::vector<std::shared_ptr<BaseColumn>>& columns,
                                            const std::optional<PolymorphicAllocator<Chunk>>& alloc,
                                            const std::shared_ptr<Chunk::AccessCounter>& access_counter) {
  _chunks.emplace_back(std::make_shared<Chunk>(_column_definitions, _use_mvcc, alloc, access_counter));
  return _chunks.back();
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

std::vector<IndexInfo> Table::get_indexes() const { return _indexes; }

size_t Table::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& chunk : _chunks) {
    bytes += chunk->estimate_memory_usage();
  }

  for (const auto& column_definition : _column_definitions) {
    bytes += column_definition.name.size();
  }

  // TODO(anybody) Statistics and Indices missing from Memory Usage Estimation
  // TODO(anybody) TableLayout missing

  return bytes;
}

}  // namespace opossum
