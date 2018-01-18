#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(size_t number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

void RoundRobinPartitionSchema::append(std::vector<AllTypeVariant> values) {
  PartitionSchema::append(values, _next_partition);
  if (_next_partition >= static_cast<PartitionID>(_partitions.size())) {
    _next_partition = 0;
  } else {
    _next_partition = static_cast<PartitionID>(static_cast<uint16_t>(_next_partition) + (uint16_t) 1);
  }
}

PartitionID RoundRobinPartitionSchema::get_matching_partition_for(std::vector<AllTypeVariant> values) {
  _next_partition = static_cast<PartitionID>(static_cast<uint16_t>(_next_partition) + (uint16_t) 1);
  return _next_partition;
}


}  // namespace opossum
