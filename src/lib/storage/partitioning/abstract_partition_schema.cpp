#include "storage/partitioning/abstract_partition_schema.hpp"

namespace opossum {

uint16_t AbstractPartitionSchema::partition_count() const { return _partitions.size(); }

void AbstractPartitionSchema::clear() {
  for (auto partition : _partitions) {
    partition->clear();
  }
}

void AbstractPartitionSchema::add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  _partitions[partition_id]->add_new_chunk(chunk);
}

void AbstractPartitionSchema::append(std::vector<AllTypeVariant> values, PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  _partitions[partition_id]->append(values);
}

std::shared_ptr<Partition> AbstractPartitionSchema::get_partition(PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  return _partitions[partition_id];
}

std::shared_ptr<Chunk> AbstractPartitionSchema::last_chunk(PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  return _partitions[partition_id]->last_chunk();
}

}  // namespace opossum
