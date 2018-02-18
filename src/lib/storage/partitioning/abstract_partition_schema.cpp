#include "storage/partitioning/abstract_partition_schema.hpp"

namespace opossum {

PartitionID AbstractPartitionSchema::partition_count() const { return static_cast<PartitionID>(_partitions.size()); }

void AbstractPartitionSchema::clear() {
  for (auto partition : _partitions) {
    partition->clear();
  }
}

void AbstractPartitionSchema::add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  _partitions[partition_id]->add_new_chunk(chunk);
}

void AbstractPartitionSchema::append(const std::vector<AllTypeVariant>& values, PartitionID partition_id) {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  _partitions[partition_id]->append(values);
}

std::shared_ptr<const Partition> AbstractPartitionSchema::get_partition(PartitionID partition_id) const {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  return _partitions[partition_id];
}

std::shared_ptr<const Chunk> AbstractPartitionSchema::last_chunk(PartitionID partition_id) const {
  DebugAssert(partition_id < _partitions.size(), "Partition ID exceeds number of partitions");
  return _partitions[partition_id]->last_chunk();
}

std::vector<ChunkID> AbstractPartitionSchema::get_chunk_ids_to_exclude(PredicateCondition condition,
                                                                       const AllTypeVariant& value) const {
  return std::vector<ChunkID>();
}
}  // namespace opossum
