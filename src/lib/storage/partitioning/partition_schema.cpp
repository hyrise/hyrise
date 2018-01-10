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

const std::shared_ptr<Partition> PartitionSchema::find_partition(PartitionID partition_to_find) {
  for (std::shared_ptr<Partition> partition_ptr : _partitions) {
    if (partition_ptr->get_partition_id() == partition_to_find) {
      return partition_ptr;
    }
  }
  throw "Cannot find specified partition!";
}

const PartitionID PartitionSchema::max_partition_id() {
  if (_partitions.size() == 0) {
    throw "The minimum number of partitions is 1!";
  }
  PartitionID max = PartitionID{0};
  for (std::shared_ptr<Partition> partition_ptr : _partitions) {
    if (partition_ptr->get_partition_id() > max) {
      max = partition_ptr->get_partition_id();
    }
  }
  return max;
}

virtual void PartitionSchema::create_new_chunk(const std::vector<DataType>& column_types,
                                               const std::vector<bool>& column_nullables) {
  throw "create_new_chunk can not be used on partitioned tables";
}

virtual void PartitionSchema::emplace_chunk(Chunk& chunk, uint16_t column_count) {
  throw "emplace_chunk can not be used on partitioned tables";
}

Chunk& PartitionSchema::get_modifiable_chunk(ChunkID chunk_id, PartitionID partition_id) {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id, "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_modifiable_chunk(chunk_id);
}

const Chunk& PartitionSchema::get_chunk(ChunkID chunk_id, PartitionID partition_id) const {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id, "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_chunk(chunk_id);
}

ProxyChunk PartitionSchema::get_modifiable_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id, "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_modifiable_chunk_with_access_counting(chunk_id);
}

const ProxyChunk PartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) const {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id, "No Partition with ID " + std::to_string(partition_id));
  return _partitions.at(partition_id)->get_chunk_with_access_counting(chunk_id);
}

}  // namespace opossum
