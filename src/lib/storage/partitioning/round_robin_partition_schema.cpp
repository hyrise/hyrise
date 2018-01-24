#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(size_t number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

std::string RoundRobinPartitionSchema::name() const { return "RoundRobinPartition"; }

void RoundRobinPartitionSchema::append(std::vector<AllTypeVariant> values) {
  AbstractPartitionSchema::append(values, _next_partition);
  _go_to_next_partition();
}

PartitionID RoundRobinPartitionSchema::get_matching_partition_for(std::vector<AllTypeVariant> values) {
  return get_next_partition();
}

PartitionID RoundRobinPartitionSchema::get_next_partition() {
  _go_to_next_partition();
  return _next_partition;
}

void RoundRobinPartitionSchema::_go_to_next_partition() {
  _next_partition = static_cast<PartitionID>(_next_partition + 1);
  if (_next_partition >= static_cast<PartitionID>(_partitions.size())) {
    _next_partition = 0;
  }
}

}  // namespace opossum
