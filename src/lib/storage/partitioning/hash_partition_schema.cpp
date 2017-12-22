#include <storage/partitioning/hash_partition_schema.hpp>

namespace opossum {

HashPartitionSchema::HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions)
    : _column_id(column_id), _hash_function(hash_function), _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < _number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>());
  }
}

}  // namespace opossum
