#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>(PartitionID{0})); }

std::string NullPartitionSchema::name() const { return "NullPartition"; }

void NullPartitionSchema::append(std::vector<AllTypeVariant> values) {
  AbstractPartitionSchema::append(values, PartitionID{0});
}

}  // namespace opossum
