#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema(Table& table) : PartitionSchema(table) {
  _partitions.emplace_back(std::make_shared<Partition>(table));
}

void NullPartitionSchema::append(std::vector<AllTypeVariant> values) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

ChunkID NullPartitionSchema::chunk_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

TableType NullPartitionSchema::get_type() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

uint64_t NullPartitionSchema::row_count() const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

void NullPartitionSchema::create_new_chunk() {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

void NullPartitionSchema::emplace_chunk(Chunk chunk) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

Chunk& NullPartitionSchema::get_chunk(ChunkID chunk_id) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

const Chunk& NullPartitionSchema::get_chunk(ChunkID chunk_id) const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

ProxyChunk NullPartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id) {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

const ProxyChunk NullPartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id) const {
  // TODO(partitioning group): Implement
  throw "Not implemented";
}

}  // namespace opossum
