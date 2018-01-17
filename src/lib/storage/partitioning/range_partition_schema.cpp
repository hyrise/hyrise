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
                                  const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  PartitionID matching_partition = get_matching_partition_for(values);
  std::shared_ptr<Partition> partition_to_append = _partitions.at(matching_partition);
  partition_to_append->append(values, max_chunk_size, column_types, column_nullables);
}

PartitionID RangePartitionSchema::get_matching_partition_for(std::vector<AllTypeVariant> values) {
  DebugAssert(values.size() > static_cast<size_t>(_column_id), "Can not determine partition, too few values given");
  auto value = values[_column_id];
  for (size_t index = 0; index < _bounds.size(); ++index) {
    if (value <= _bounds.at(index)) {
      return static_cast<PartitionID>(index);
    }
  }
  return static_cast<PartitionID>(_bounds.size());
}

}  // namespace opossum
