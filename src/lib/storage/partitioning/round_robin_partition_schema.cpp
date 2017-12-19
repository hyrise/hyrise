#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(int number_of_partitions) : _number_of_partitions(number_of_partitions) {

	partitions.reserve(number_of_partitions);
	for (int i = 0; i < number_of_partitions; ++i) {
		partitions.emplace_back(std::make_shared<Partition>());
	}

}

}
