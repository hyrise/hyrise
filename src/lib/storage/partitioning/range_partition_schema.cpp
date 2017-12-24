#include <storage/partitioning/range_partition_schema.hpp>

namespace opossum {

RangePartitionSchema::RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds)
    : _column_id(column_id), _bounds(bounds) {
  _partitions.reserve(bounds.size() + 1);

  for (size_t index = 0; index < bounds.size() + 1; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>());
  }
}

void RangePartitionSchema::append(std::vector<AllTypeVariant> values) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

ChunkID RangePartitionSchema::chunk_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

TableType RangePartitionSchema::get_type() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

uint32_t RangePartitionSchema::max_chunk_size() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

uint64_t RangePartitionSchema::row_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

}  // namespace opossum
