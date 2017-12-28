#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>(PartitionID{0})); }

void NullPartitionSchema::add_column(DataType data_type, bool nullable) {
  _partitions.front()->add_column(data_type, nullable);
}

void NullPartitionSchema::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                                 const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  _partitions.front()->append(values, max_chunk_size, column_types, column_nullables);
}

ChunkID NullPartitionSchema::chunk_count() const { return _partitions.front()->chunk_count(); }

TableType NullPartitionSchema::get_type(uint16_t column_count) const {
  return _partitions.front()->get_type(column_count);
}

AllTypeVariant NullPartitionSchema::get_value(const ColumnID column_id, const size_t row_number) const {
  return _partitions.front()->get_value(column_id, row_number);
}

uint64_t NullPartitionSchema::row_count() const { return _partitions.front()->row_count(); }

void NullPartitionSchema::create_new_chunk(const std::vector<DataType>& column_types,
                                           const std::vector<bool>& column_nullables) {
  _partitions.front()->create_new_chunk(column_types, column_nullables);
}

void NullPartitionSchema::emplace_chunk(Chunk& chunk, uint16_t column_count) {
  _partitions.front()->emplace_chunk(chunk, column_count);
}

Chunk& NullPartitionSchema::get_chunk(ChunkID chunk_id) { return _partitions.front()->get_chunk(chunk_id); }

const Chunk& NullPartitionSchema::get_chunk(ChunkID chunk_id) const { return _partitions.front()->get_chunk(chunk_id); }

ProxyChunk NullPartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id) {
  return _partitions.front()->get_chunk_with_access_counting(chunk_id);
}

const ProxyChunk NullPartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id) const {
  return _partitions.front()->get_chunk_with_access_counting(chunk_id);
}

}  // namespace opossum
