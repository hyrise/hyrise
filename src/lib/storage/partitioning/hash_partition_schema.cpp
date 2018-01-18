#include <storage/partitioning/hash_partition_schema.hpp>

namespace opossum {

HashPartitionSchema::HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions)
    : _column_id(column_id), _hash_function(hash_function), _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < _number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

void HashPartitionSchema::append(std::vector<AllTypeVariant> values) {
  PartitionSchema::append(values, get_matching_partition_for(values));
}

PartitionID HashPartitionSchema::get_matching_partition_for(std::vector<AllTypeVariant> values) {
  DebugAssert(values.size() > static_cast<size_t>(_column_id), "Can not determine partition, too few values given");
  auto value = values[_column_id];
  const HashValue hash = _hash_function.calculate_hash(value);
  PartitionID matching_partition = static_cast<PartitionID>(hash % _number_of_partitions);
  return matching_partition;
}

}  // namespace opossum
