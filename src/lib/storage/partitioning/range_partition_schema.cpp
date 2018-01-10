#include <storage/partitioning/range_partition_schema.hpp>

namespace opossum {

RangePartitionSchema::RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds)
    : _column_id(column_id), _bounds(bounds) {
  _partitions.reserve(bounds.size() + 1);

  for (size_t index = 0; index < bounds.size() + 1; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

void RangePartitionSchema::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                                  const std::vector<DataType>& column_types,
                                  const std::vector<bool>& column_nullables) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

TableType RangePartitionSchema::get_type(uint16_t column_count) const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

AllTypeVariant RangePartitionSchema::get_value(const ColumnID column_id, const size_t row_number) const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

}  // namespace opossum
