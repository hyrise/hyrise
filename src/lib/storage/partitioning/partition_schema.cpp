#include "storage/partitioning/partition_schema.hpp"

namespace opossum {

void PartitionSchema::add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  _partitions[partition_id]->add_new_chunk(chunk);
}

void PartitionSchema::append(std::vector<AllTypeVariant> values, PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  _partitions[partition_id]->append(values);
}

std::shared_ptr<Partition> PartitionSchema::get_partition(PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  return _partitions[partition_id];
}

std::shared_ptr<Chunk> PartitionSchema::last_chunk(PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  return _partitions[partition_id]->last_chunk();
}

}  // namespace opossum
