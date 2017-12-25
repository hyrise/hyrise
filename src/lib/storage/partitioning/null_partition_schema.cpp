#include <storage/partitioning/null_partition_schema.hpp>

namespace opossum {

NullPartitionSchema::NullPartitionSchema(Table& table) : PartitionSchema(table) {
  _partitions.emplace_back(std::make_shared<Partition>(table));
}

void NullPartitionSchema::append(std::vector<AllTypeVariant> values) { _partitions.front()->append(values); }

ChunkID NullPartitionSchema::chunk_count() const { return _partitions.front()->chunk_count(); }

TableType NullPartitionSchema::get_type() const { return _partitions.front()->get_type(); }

uint64_t NullPartitionSchema::row_count() const { return _partitions.front()->row_count(); }

void NullPartitionSchema::create_new_chunk() { _partitions.front()->create_new_chunk(); }

void NullPartitionSchema::emplace_chunk(Chunk& chunk) { _partitions.front()->emplace_chunk(chunk); }

Chunk& NullPartitionSchema::get_chunk(ChunkID chunk_id) {
  Chunk& chunk = _partitions.front()->get_chunk(chunk_id);
  return chunk;
}

const Chunk& NullPartitionSchema::get_chunk(ChunkID chunk_id) const {
  const Chunk& chunk = _partitions.front()->get_chunk(chunk_id);
  return chunk;
}

ProxyChunk NullPartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id) {
  ProxyChunk proxy_chunk = _partitions.front()->get_chunk_with_access_counting(chunk_id);
  return proxy_chunk;
}

const ProxyChunk NullPartitionSchema::get_chunk_with_access_counting(ChunkID chunk_id) const {
  const ProxyChunk proxy_chunk = _partitions.front()->get_chunk_with_access_counting(chunk_id);
  return proxy_chunk;
}

}  // namespace opossum
