#include "storage/partitioning/partition_schema.hpp"

namespace opossum {

Chunk& PartitionSchema::get_modifiable_chunk(ChunkID chunk_id, PartitionID partition_id=0) {
  DebugAssert(_partitions.size() > partition_id, "No Partition with ID " + std::to_str(partition_id));
  return _partitions.at(partition_id)->get_modifiable_chunk(chunk_id);
}

const Chunk& PartitionSchema::get_chunk(ChunkID chunk_id, PartitionID partition_id=0) const {
  DebugAssert(_partitions.size() > partition_id, "No Partition with ID " + std::to_str(partition_id));
  return _partitions.at(partition_id)->get_chunk(chunk_id);
}

ProxyChunk PartitionSchema::get_modifiable_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id=0) {
  DebugAssert(_partitions.size() > partition_id, "No Partition with ID " + std::to_str(partition_id));
  return _partitions.at(partition_id)->get_modifiable_chunk_with_access_counting(chunk_id);
}

const ProxyChunk PartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id=0) const {
  DebugAssert(_partitions.size() > partition_id, "No Partition with ID " + std::to_str(partition_id));
  return _partitions.at(partition_id)->get_chunk_with_access_counting(chunk_id);
}

}  // namespace opossum
