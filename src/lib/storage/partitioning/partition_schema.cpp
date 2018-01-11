#include "storage/partitioning/partition_schema.hpp"

namespace opossum {

void PartitionSchema::add_column(DataType data_type, bool nullable) {
  for (std::shared_ptr<Partition> partition_ptr : _partitions) {
    partition_ptr->add_column(data_type, nullable);
  }
}

ChunkID PartitionSchema::chunk_count() const {
  ChunkID num_of_chunks = ChunkID{0};
  for (std::shared_ptr<Partition> partition_ptr : _partitions) {
    num_of_chunks += partition_ptr->chunk_count();
  }
  return num_of_chunks;
}

uint64_t PartitionSchema::row_count() const {
  uint64_t num_of_rows = 0;
  for (std::shared_ptr<Partition> partition_ptr : _partitions) {
    num_of_rows += partition_ptr->row_count();
  }
  return num_of_rows;
}

TableType PartitionSchema::get_type(uint16_t column_count) const { return _partitions.front()->get_type(column_count); }

AllTypeVariant PartitionSchema::get_value(const ColumnID column_id, const size_t row_number) const {
  size_t row_counter = 0u;
  for (std::shared_ptr<Partition> partition_ptr : _partitions) {
    size_t current_size = partition_ptr->row_count();
    row_counter += current_size;
    if (row_counter > row_number) {
      return (partition_ptr->get_value(column_id, row_number - (row_counter - current_size)));
    }
  }
  Fail("Row does not exist.");
  return {};
}

void PartitionSchema::create_new_chunk(const std::vector<DataType>& column_types,
                                       const std::vector<bool>& column_nullables, PartitionID partition_id) {
  _partitions.at(partition_id)->create_new_chunk(column_types, column_nullables);
}

void PartitionSchema::emplace_chunk(Chunk& chunk, uint16_t column_count, PartitionID partition_id) {
  _partitions.at(partition_id)->emplace_chunk(chunk, column_count);
}

Chunk& PartitionSchema::get_modifiable_chunk(ChunkID chunk_id, PartitionID partition_id) {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id,
              "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_modifiable_chunk(chunk_id);
}

const Chunk& PartitionSchema::get_chunk(ChunkID chunk_id, PartitionID partition_id) const {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id,
              "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_chunk(chunk_id);
}

ProxyChunk PartitionSchema::get_modifiable_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id,
              "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_modifiable_chunk_with_access_counting(chunk_id);
}

const ProxyChunk PartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) const {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id,
              "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_chunk_with_access_counting(chunk_id);
}

}  // namespace opossum
