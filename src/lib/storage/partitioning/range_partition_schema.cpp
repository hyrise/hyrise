#include <storage/partitioning/range_partition_schema.hpp>

namespace opossum {

RangePartitionSchema::RangePartitionSchema(Table& table, ColumnID column_id, std::vector<AllTypeVariant> bounds)
    : PartitionSchema(table), _column_id(column_id), _bounds(bounds) {
  _partitions.reserve(bounds.size() + 1);

  for (size_t index = 0; index < bounds.size() + 1; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(table));
  }
}

void RangePartitionSchema::add_column(DataType data_type, bool nullable) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
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

uint64_t RangePartitionSchema::row_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

}  // namespace opossum
