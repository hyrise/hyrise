#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>(PartitionID{0})); }

void NullPartitionSchema::append(std::vector<AllTypeVariant> values) {
  PartitionSchema::append(values, PartitionID{0});
}

}  // namespace opossum
