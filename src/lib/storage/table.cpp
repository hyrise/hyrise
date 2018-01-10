#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/partitioning/hash_partition_schema.hpp"
#include "storage/partitioning/null_partition_schema.hpp"
#include "storage/partitioning/range_partition_schema.hpp"
#include "storage/partitioning/round_robin_partition_schema.hpp"
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
    : _max_chunk_size(max_chunk_size),
      _append_mutex(std::make_unique<std::mutex>()),
      _partition_schema(std::make_shared<NullPartitionSchema>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

void Table::add_column_definition(const std::string& name, DataType data_type, bool nullable) {
  Assert((name.size() < std::numeric_limits<ColumnNameLength>::max()), "Cannot add column. Column name is too long.");

  _column_names.push_back(name);
  _column_types.push_back(data_type);
  _column_nullable.push_back(nullable);
}

void Table::add_column(const std::string& name, DataType data_type, bool nullable) {
  add_column_definition(name, data_type, nullable);

  _partition_schema->add_column(data_type, nullable);
}

void Table::append(std::vector<AllTypeVariant> values) {
  // TODO(Anyone): Chunks should be preallocated for chunk size
  _partition_schema->append(values, _max_chunk_size, _column_types, _column_nullable);
}

void Table::inc_invalid_row_count(uint64_t count) { _approx_invalid_row_count += count; }

void Table::create_new_chunk(PartitionID partition_id) {
  _partition_schema->create_new_chunk(_column_types, _column_nullable, partition_id);
}

uint16_t Table::column_count() const { return _column_types.size(); }

uint64_t Table::row_count() const { return _partition_schema->row_count(); }

uint64_t Table::approx_valid_row_count() const { return row_count() - _approx_invalid_row_count; }

ChunkID Table::chunk_count() const { return _partition_schema->chunk_count(); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (ColumnID column_id{0}; column_id < column_count(); ++column_id) {
    // TODO(Anyone): make more efficient
    if (_column_names[column_id] == column_name) {
      return column_id;
    }
  }
  Fail("Column " + column_name + " not found.");
  return {};
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

Chunk& Table::get_modifiable_chunk(ChunkID chunk_id, PartitionID partition_id) {
  return _partition_schema->get_modifiable_chunk(chunk_id, partition_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id, PartitionID partition_id) const {
  return _partition_schema->get_chunk(chunk_id, partition_id);
}

ProxyChunk Table::get_modifiable_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) {
  return _partition_schema->get_modifiable_chunk_with_access_counting(chunk_id, partition_id);
}

const ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) const {
  return _partition_schema->get_chunk_with_access_counting(chunk_id, partition_id);
}

void Table::emplace_chunk(Chunk chunk, PartitionID partition_id) {
  _partition_schema->emplace_chunk(chunk, this->column_count(), partition_id);
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

TableType Table::get_type() const { return _partition_schema->get_type(this->column_count()); }

void Table::create_hash_partitioning(const ColumnID column_id, const HashFunction hash_function,
                                     const size_t number_of_partitions) {
  _partition_schema = std::make_shared<HashPartitionSchema>(column_id, hash_function, number_of_partitions);
}

void Table::create_range_partitioning(const ColumnID column_id, const std::vector<AllTypeVariant> bounds) {
  _partition_schema = std::make_shared<RangePartitionSchema>(column_id, bounds);
}

void Table::create_round_robin_partitioning(const size_t number_of_partitions) {
  _partition_schema = std::make_shared<RoundRobinPartitionSchema>(number_of_partitions);
}

bool Table::is_partitioned() const { return _partition_schema->is_partitioned(); }

}  // namespace opossum
