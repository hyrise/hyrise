#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

std::shared_ptr<Table> Table::create_dummy_table(const TableColumnDefinitions& column_definitions) {
  return std::make_shared<Table>(column_definitions, TableType::Data);
}

Table::Table(const TableColumnDefinitions& column_definitions, const TableType type,
             const std::optional<ChunkOffset> max_chunk_size, const UseMvcc use_mvcc)
    : _column_definitions(column_definitions),
      _type(type),
      _use_mvcc(use_mvcc),
      _max_chunk_size(type == TableType::Data ? max_chunk_size.value_or(Chunk::DEFAULT_SIZE) : Chunk::MAX_SIZE),
      _append_mutex(std::make_unique<std::mutex>()) {
  // _max_chunk_size has no meaning if the table is a reference table.
  DebugAssert(type == TableType::Data || !max_chunk_size, "Must not set max_chunk_size for reference tables");
  DebugAssert(!max_chunk_size || *max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

Table::Table(const TableColumnDefinitions& column_definitions, const TableType type,
             std::vector<std::shared_ptr<Chunk>>&& chunks, const UseMvcc use_mvcc)
    : Table(column_definitions, type, type == TableType::Data ? std::optional{Chunk::MAX_SIZE} : std::nullopt,
            use_mvcc) {
  _chunks = {chunks.begin(), chunks.end()};

#if HYRISE_DEBUG
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) continue;

    DebugAssert(chunk->has_mvcc_data() == (_use_mvcc == UseMvcc::Yes),
                "Supply MvccData for Chunks iff Table uses MVCC");
    DebugAssert(chunk->column_count() == column_count(), "Invalid Chunk column count");

    for (auto column_id = ColumnID{0}; column_id < column_count(); ++column_id) {
      DebugAssert(chunk->get_segment(column_id)->data_type() == column_data_type(column_id),
                  "Invalid Segment DataType");
    }
  }
#endif
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
  auto last_chunk = !_chunks.empty() ? get_chunk(ChunkID{chunk_count() - 1}) : nullptr;
  if (!last_chunk || last_chunk->size() >= _max_chunk_size) {
    append_mutable_chunk();
    last_chunk = get_chunk(ChunkID{chunk_count() - 1});
  }

  last_chunk->append(values);
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
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (chunk) ret += chunk->size();
  }
  return ret;
}

bool Table::empty() const { return row_count() == 0u; }

ChunkID Table::chunk_count() const { return ChunkID{static_cast<ChunkID::base_type>(_chunks.size())}; }

ChunkOffset Table::max_chunk_size() const { return _max_chunk_size; }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  if (_type == TableType::References) {
    // Not written concurrently, since reference tables are not modified anymore once they are written.
    return _chunks[chunk_id];
  } else {
    return std::atomic_load(&_chunks[chunk_id]);
  }
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  if (_type == TableType::References) {
    // see comment in non-const function
    return _chunks[chunk_id];
  } else {
    return std::atomic_load(&_chunks[chunk_id]);
  }
}

void Table::remove_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  DebugAssert(([this, chunk_id]() {
                const auto chunk = get_chunk(chunk_id);
                return (chunk->invalid_row_count() == chunk->size());
              }()),
              "Physical delete of chunk prevented: Chunk needs to be fully invalidated before.");
  Assert(_type == TableType::Data, "Removing chunks from other tables than data tables is not intended yet.");
  std::atomic_store(&_chunks[chunk_id], std::shared_ptr<Chunk>(nullptr));
}

void Table::append_chunk(const Segments& segments, std::shared_ptr<MvccData> mvcc_data,
                         const std::optional<PolymorphicAllocator<Chunk>>& alloc) {
  Assert(_type != TableType::Data || static_cast<bool>(mvcc_data) == (_use_mvcc == UseMvcc::Yes),
         "Supply MvccData to data Tables iff MVCC is enabled");

#if HYRISE_DEBUG
  for (const auto& segment : segments) {
    const auto is_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr;
    DebugAssert(is_reference_segment == (_type == TableType::References), "Invalid Segment type");
  }
#endif

  _chunks.push_back(std::make_shared<Chunk>(segments, mvcc_data, alloc));
}

std::vector<AllTypeVariant> Table::get_row(size_t row_idx) const {
  PerformanceWarning("get_row() used");
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) continue;

    if (row_idx < chunk->size()) {
      auto row = std::vector<AllTypeVariant>(column_count());

      for (ColumnID column_id{0}; column_id < column_count(); ++column_id) {
        row[column_id] = chunk->get_segment(column_id)->operator[](static_cast<ChunkOffset>(row_idx));
      }

      return row;
    } else {
      row_idx -= chunk->size();
    }
  }

  Fail("row_idx out of bounds");
}

std::vector<std::vector<AllTypeVariant>> Table::get_rows() const {
  PerformanceWarning("get_rows() used");

  // Allocate all rows
  auto rows = std::vector<std::vector<AllTypeVariant>>{row_count()};
  const auto num_columns = column_count();
  for (auto& row : rows) {
    row.resize(num_columns);
  }

  // Materialize the Chunks
  auto chunk_begin_row_idx = size_t{0};
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) continue;

    for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
      segment_iterate(*chunk->get_segment(column_id), [&](const auto& segment_position) {
        if (!segment_position.is_null()) {
          rows[chunk_begin_row_idx + segment_position.chunk_offset()][column_id] = segment_position.value();
        }
      });
    }

    chunk_begin_row_idx += chunk->size();
  }

  return rows;
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

std::shared_ptr<TableStatistics> Table::table_statistics() const { return _table_statistics; }

void Table::set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics) {
  _table_statistics = table_statistics;
}

std::vector<IndexStatistics> Table::indexes_statistics() const { return _indexes; }

size_t Table::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) continue;

    bytes += chunk->estimate_memory_usage();
  }

  for (const auto& column_definition : _column_definitions) {
    bytes += column_definition.name.size();
  }

  // TODO(anybody) Statistics and Indexes missing from Memory Usage Estimation
  // TODO(anybody) TableLayout missing

  return bytes;
}

}  // namespace opossum
