#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>(PartitionID{0})); }

void NullPartitionSchema::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                                 const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  _partitions.front()->append(values, max_chunk_size, column_types, column_nullables);
}

}  // namespace opossum
