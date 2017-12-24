#pragma once

#include "storage/chunk.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "storage/proxy_chunk.hpp"

namespace opossum {

class NullPartitionSchema : public PartitionSchema {
 public:
  NullPartitionSchema();

  // Return true, since NullPartitioningSchema is only a list of chunks.
  // Indicates that NullPartitionSchema can handle:
  // create_new_chunk, emplace_chunk, get_chunk, get_chunk_with_access_counting
  bool is_continous() const override { return true; }

  // from abstract class PartitionSchema
  void append(std::vector<AllTypeVariant> values) override;
  ChunkID chunk_count() const override;
  TableType get_type() const override;
  uint32_t max_chunk_size() const override;
  uint64_t row_count() const override;

  // specific for NullPartitionSchema
  void create_new_chunk();
  void emplace_chunk(Chunk chunk);
  Chunk& get_chunk(ChunkID chunk_id);
  const Chunk& get_chunk(ChunkID chunk_id) const;
  ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id);
  const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const; 
};

}  // namespace opossum
