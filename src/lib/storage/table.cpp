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
#include "value_segment.hpp"

namespace opossum {

std::shared_ptr<Table> Table::create_dummy_table(const TableCxlumnDefinitions& cxlumn_definitions) {
  return std::make_shared<Table>(cxlumn_definitions, TableType::Data);
}

Table::Table(const TableCxlumnDefinitions& cxlumn_definitions, const TableType type, const uint32_t max_chunk_size,
             const UseMvcc use_mvcc)
    : _cxlumn_definitions(cxlumn_definitions),
      _type(type),
      _use_mvcc(use_mvcc),
      _max_chunk_size(max_chunk_size),
      _append_mutex(std::make_unique<std::mutex>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

const TableCxlumnDefinitions& Table::cxlumn_definitions() const { return _cxlumn_definitions; }

TableType Table::type() const { return _type; }

UseMvcc Table::has_mvcc() const { return _use_mvcc; }

size_t Table::cxlumn_count() const { return _cxlumn_definitions.size(); }

const std::string& Table::cxlumn_name(const CxlumnID cxlumn_id) const {
  DebugAssert(cxlumn_id < _cxlumn_definitions.size(), "CxlumnID out of range");
  return _cxlumn_definitions[cxlumn_id].name;
}

std::vector<std::string> Table::cxlumn_names() const {
  std::vector<std::string> names;
  names.reserve(_cxlumn_definitions.size());
  for (const auto& column_definition : _cxlumn_definitions) {
    names.emplace_back(column_definition.name);
  }
  return names;
}

DataType Table::cxlumn_data_type(const CxlumnID cxlumn_id) const {
  DebugAssert(cxlumn_id < _cxlumn_definitions.size(), "CxlumnID out of range");
  return _cxlumn_definitions[cxlumn_id].data_type;
}

std::vector<DataType> Table::cxlumn_data_types() const {
  std::vector<DataType> data_types;
  data_types.reserve(_cxlumn_definitions.size());
  for (const auto& column_definition : _cxlumn_definitions) {
    data_types.emplace_back(column_definition.data_type);
  }
  return data_types;
}

bool Table::column_is_nullable(const CxlumnID cxlumn_id) const {
  DebugAssert(cxlumn_id < _cxlumn_definitions.size(), "CxlumnID out of range");
  return _cxlumn_definitions[cxlumn_id].nullable;
}

std::vector<bool> Table::columns_are_nullable() const {
  std::vector<bool> nullable(cxlumn_count());
  for (size_t cxlumn_idx = 0; cxlumn_idx < cxlumn_count(); ++cxlumn_idx) {
    nullable[cxlumn_idx] = _cxlumn_definitions[cxlumn_idx].nullable;
  }
  return nullable;
}

CxlumnID Table::cxlumn_id_by_name(const std::string& cxlumn_name) const {
  const auto iter = std::find_if(_cxlumn_definitions.begin(), _cxlumn_definitions.end(),
                                 [&](const auto& column_definition) { return column_definition.name == cxlumn_name; });
  Assert(iter != _cxlumn_definitions.end(), "Couldn't find column '" + cxlumn_name + "'");
  return static_cast<CxlumnID>(std::distance(_cxlumn_definitions.begin(), iter));
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.empty() || _chunks.back()->size() >= _max_chunk_size) {
    append_mutable_chunk();
  }

  _chunks.back()->append(values);
}

void Table::append_mutable_chunk() {
  ChunkSegments columns;
  for (const auto& column_definition : _cxlumn_definitions) {
    resolve_data_type(column_definition.data_type, [&](auto type) {
      using CxlumnDataType = typename decltype(type)::type;
      columns.push_back(std::make_shared<ValueSegment<CxlumnDataType>>(column_definition.nullable));
    });
  }
  append_chunk(columns);
}

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk->size();
  }
  return ret;
}

bool Table::empty() const { return row_count() == 0u; }

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

const std::vector<std::shared_ptr<Chunk>>& Table::chunks() const { return _chunks; }

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

void Table::append_chunk(const ChunkSegments& columns, const std::optional<PolymorphicAllocator<Chunk>>& alloc,
                         const std::shared_ptr<ChunkAccessCounter>& access_counter) {
  const auto chunk_size = columns.empty() ? 0u : columns[0]->size();

#if IS_DEBUG
  for (const auto& column : columns) {
    DebugAssert(column->size() == chunk_size, "Columns don't have the same length");
    const auto is_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(column) != nullptr;
    switch (_type) {
      case TableType::References:
        DebugAssert(is_reference_segment, "Invalid column type");
        break;
      case TableType::Data:
        DebugAssert(!is_reference_segment, "Invalid column type");
        break;
    }
  }
#endif

  std::shared_ptr<MvccData> mvcc_data;

  if (_use_mvcc == UseMvcc::Yes) {
    mvcc_data = std::make_shared<MvccData>(chunk_size);
  }

  _chunks.emplace_back(std::make_shared<Chunk>(columns, mvcc_data, alloc, access_counter));
}

void Table::append_chunk(const std::shared_ptr<Chunk>& chunk) {
#if IS_DEBUG
  for (const auto& column : chunk->columns()) {
    const auto is_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(column) != nullptr;
    switch (_type) {
      case TableType::References:
        DebugAssert(is_reference_segment, "Invalid column type");
        break;
      case TableType::Data:
        DebugAssert(!is_reference_segment, "Invalid column type");
        break;
    }
  }
#endif

  DebugAssert(chunk->has_mvcc_data() == (_use_mvcc == UseMvcc::Yes),
              "Chunk does not have the same MVCC setting as the table.");

  _chunks.emplace_back(chunk);
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

std::vector<IndexInfo> Table::get_indexes() const { return _indexes; }

size_t Table::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& chunk : _chunks) {
    bytes += chunk->estimate_memory_usage();
  }

  for (const auto& column_definition : _cxlumn_definitions) {
    bytes += column_definition.name.size();
  }

  // TODO(anybody) Statistics and Indices missing from Memory Usage Estimation
  // TODO(anybody) TableLayout missing

  return bytes;
}

}  // namespace opossum
