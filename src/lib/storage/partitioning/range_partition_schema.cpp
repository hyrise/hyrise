#include <storage/partitioning/range_partition_schema.hpp>

namespace opossum {

RangePartitionSchema::RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds)
    : _column_id(column_id), _bounds(bounds) {
  _partitions.reserve(bounds.size() + 1);

  for (size_t index = 0; index < bounds.size() + 1; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>());
  }
}

}  // namespace opossum
