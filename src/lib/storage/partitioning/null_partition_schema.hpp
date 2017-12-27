#pragma once

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "storage/proxy_chunk.hpp"

namespace opossum {

class NullPartitionSchema : public PartitionSchema {
 public:
  explicit NullPartitionSchema();

  // Return false, since NullPartitioningSchema is only a list of chunks.
  // Indicates that NullPartitionSchema can handle:
  // create_new_chunk, emplace_chunk, get_chunk, get_chunk_with_access_counting
  bool is_partitioned() const { return false; }

  // from abstract class PartitionSchema
  void add_column(DataType data_type, bool nullable);
  void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
              const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables);
  ChunkID chunk_count() const;
  TableType get_type(uint16_t column_count) const;
  AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const;
  uint64_t row_count() const;

  // specific for NullPartitionSchema
  void create_new_chunk(const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables);
  void emplace_chunk(Chunk& chunk, uint16_t column_count);
  Chunk& get_chunk(ChunkID chunk_id);
  const Chunk& get_chunk(ChunkID chunk_id) const;
  ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id);
  const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const;
};

}  // namespace opossum
