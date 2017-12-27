#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(size_t number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>());
  }
}

void RoundRobinPartitionSchema::add_column(DataType data_type, bool nullable) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

void RoundRobinPartitionSchema::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                                       const std::vector<DataType>& column_types,
                                       const std::vector<bool>& column_nullables) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

ChunkID RoundRobinPartitionSchema::chunk_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

TableType RoundRobinPartitionSchema::get_type(uint16_t column_count) const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

AllTypeVariant RoundRobinPartitionSchema::get_value(const ColumnID column_id, const size_t row_number) const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

uint64_t RoundRobinPartitionSchema::row_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

}  // namespace opossum
