#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(size_t number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>());
  }
}

}  // namespace opossum
