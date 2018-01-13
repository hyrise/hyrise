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
  AllTypeVariant value_to_find_in_range = values.at(_column_id);
  PartitionID matching_partition = get_matching_partition_for(value_to_find_in_range);
  std::shared_ptr<Partition> partition_to_append = _partitions.at(matching_partition);
  partition_to_append->append(values, max_chunk_size, column_types, column_nullables);
}

const PartitionID RangePartitionSchema::get_matching_partition_for(AllTypeVariant value) const {
  for (size_t index = 0; index < _bounds.size(); ++index) {
    if (value <= _bounds.at(index)) {
      return PartitionID{index};
    }
  }
  return PartitionID{_bounds.size()};
};

}  // namespace opossum
