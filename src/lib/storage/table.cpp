#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

std::shared_ptr<Table> Table::create_dummy_table(const TableColumnDefinitions& column_definitions) {
  return std::make_shared<Table>(column_definitions, TableType::Data);
}

Table::Table(const TableColumnDefinitions& column_definitions, const TableType type,
             const std::optional<uint32_t> max_chunk_size, const UseMvcc use_mvcc)
    : _column_definitions(column_definitions),
      _type(type),
      _use_mvcc(use_mvcc),
      _max_chunk_size(type == TableType::Data ? max_chunk_size.value_or(Chunk::DEFAULT_SIZE) : Chunk::MAX_SIZE),
      _append_mutex(std::make_unique<std::mutex>()) {
  // _max_chunk_size has no meaning if the table is a reference table.
  DebugAssert(type == TableType::Data || !max_chunk_size, "Must not set max_chunk_size for reference tables");
  DebugAssert(!max_chunk_size || *max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

const TableColumnDefinitions& Table::column_definitions() const { return _column_definitions; }

TableType Table::type() const { return _type; }

UseMvcc Table::has_mvcc() const { return _use_mvcc; }

size_t Table::column_count() const { return _column_definitions.size(); }

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
  std::vector<DataType> types;
  types.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    types.emplace_back(column_definition.data_type);
  }
  return types;
}

bool Table::column_is_nullable(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].nullable;
}

std::vector<bool> Table::columns_are_nullable() const {
  std::vector<bool> nullable(column_count());
  for (size_t column_idx = 0; column_idx < column_count(); ++column_idx) {
    nullable[column_idx] = _column_definitions[column_idx].nullable;
  }
  return nullable;
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto iter = std::find_if(_column_definitions.begin(), _column_definitions.end(),
                                 [&](const auto& column_definition) { return column_definition.name == column_name; });
  Assert(iter != _column_definitions.end(), "Couldn't find column '" + column_name + "'");
  return ColumnID{static_cast<ColumnID::base_type>(std::distance(_column_definitions.begin(), iter))};
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.empty() || _chunks.back()->size() >= _max_chunk_size) {
    append_mutable_chunk();
  }

  _chunks.back()->append(values);
}

void Table::append_mutable_chunk() {
  Segments segments;
  for (const auto& column_definition : _column_definitions) {
    resolve_data_type(column_definition.data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(column_definition.nullable));
    });
  }

  std::shared_ptr<MvccData> mvcc_data;
  if (_use_mvcc == UseMvcc::Yes) {
    mvcc_data = std::make_shared<MvccData>(0, CommitID{0});
  }

  append_chunk(segments, mvcc_data);
}

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    if (chunk) ret += chunk->size();
  }
  return ret;
}

bool Table::empty() const { return row_count() == 0u; }

ChunkID Table::chunk_count() const { return ChunkID{static_cast<ChunkID::base_type>(_chunks.size())}; }

const tbb::concurrent_vector<std::shared_ptr<Chunk>>& Table::chunks() const { return _chunks; }

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

void Table::remove_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  DebugAssert(_chunks[chunk_id]->invalid_row_count() == _chunks[chunk_id]->size(),
              "Physical delete of chunk prevented: Chunk needs to be fully invalidated before.");
  if (_table_statistics) {
    auto invalidated_rows_count = _chunks[chunk_id]->size();
    _table_statistics->decrease_invalid_row_count(invalidated_rows_count);
  }
  _chunks[chunk_id] = nullptr;
}

void Table::append_chunk(const Segments& segments, std::shared_ptr<MvccData> mvcc_data,
                         const std::optional<PolymorphicAllocator<Chunk>>& alloc) {
  Assert(_type != TableType::References || !mvcc_data,
         "Setting explicit MvccData on a reference Table makes no sense. Reference Tables should use MvccData of "
         "referenced Table, if any");
  Assert(_type != TableType::Data || static_cast<bool>(mvcc_data) == (_use_mvcc == UseMvcc::Yes),
         "Supply MvccData to data Tables iff MVCC is enabled");

  const auto chunk_size = segments.empty() ? 0u : segments[0]->size();

  Assert(!mvcc_data || mvcc_data->size() == chunk_size, "Invalid MvccData size, needs to be the same as Chunk size");

#if HYRISE_DEBUG
  for (const auto& segment : segments) {
    DebugAssert(segment->size() == chunk_size, "Segments don't have the same length");
    const auto is_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr;
    switch (_type) {
      case TableType::References:
        DebugAssert(is_reference_segment, "Invalid segment type");
        break;
      case TableType::Data:
        DebugAssert(!is_reference_segment, "Invalid segment type");
        break;
    }
  }
#endif

  _chunks.push_back(std::make_shared<Chunk>(segments, mvcc_data, alloc));
}

void Table::append_chunk(const std::shared_ptr<Chunk>& chunk) {
#if HYRISE_DEBUG
  for (const auto& segment : chunk->segments()) {
    const auto is_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr;
    switch (_type) {
      case TableType::References:
        DebugAssert(is_reference_segment, "Invalid segment type");
        break;
      case TableType::Data:
        DebugAssert(!is_reference_segment, "Invalid segment type");
        break;
    }
  }
#endif

  DebugAssert(chunk->has_mvcc_data() == (_use_mvcc == UseMvcc::Yes),
              "Chunk does not have the same MVCC setting as the table.");

  _chunks.push_back(chunk);
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
