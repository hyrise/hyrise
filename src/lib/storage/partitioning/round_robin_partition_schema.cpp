#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(PartitionID number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (PartitionID index{0}; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(index));
  }
}

std::string RoundRobinPartitionSchema::name() const { return "RoundRobinPartition"; }

PartitionSchemaType RoundRobinPartitionSchema::get_type() const { return PartitionSchemaType::RoundRobin; }

void RoundRobinPartitionSchema::append(std::vector<AllTypeVariant> values) {
  AbstractPartitionSchema::append(values, _next_partition);
  _go_to_next_partition();
}

PartitionID RoundRobinPartitionSchema::get_matching_partition_for(std::vector<AllTypeVariant> values) const {
  return get_next_partition();
}

PartitionID RoundRobinPartitionSchema::get_next_partition() const {
  _go_to_next_partition();
  return _next_partition;
}

void RoundRobinPartitionSchema::_go_to_next_partition() const {
  ++_next_partition;
  if (_next_partition >= _number_of_partitions) {
    _next_partition = PartitionID{0};
  }
}

}  // namespace opossum
