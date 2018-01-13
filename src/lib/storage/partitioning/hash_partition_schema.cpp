#include <storage/partitioning/hash_partition_schema.hpp>

namespace opossum {

HashPartitionSchema::HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions)
    : _column_id(column_id), _hash_function(hash_function), _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < _number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

void HashPartitionSchema::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                                 const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  AllTypeVariant value_to_hash = values.at(_column_id);
  PartitionID matching_partition = get_matching_partition_for(value_to_hash);
  std::shared_ptr<Partition> partition_to_append = _partitions.at(matching_partition);
  partition_to_append->append(values, max_chunk_size, column_types, column_nullables);
}

const PartitionID HashPartitionSchema::get_matching_partition_for(AllTypeVariant value) const {
  const HashValue hash = _hash_function.calculate_hash(value);
  PartitionID matching_partition = PartitionID{hash % _number_of_partitions};
  return matching_partition;
}

}  // namespace opossum
