#include "storage/partitioning/partition_schema.hpp"

namespace opossum {

void PartitionSchema::add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id) {
  DebugAssert(static_cast<PartitionID>(_partitions.size()) > partition_id,
              "No Partition with ID " + std::to_string(partition_id));
  _partitions[partition_id]->add_new_chunk(chunk);
}

}  // namespace opossum
