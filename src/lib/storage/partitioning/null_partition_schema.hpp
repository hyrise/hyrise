#pragma once

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "storage/proxy_chunk.hpp"

namespace opossum {

class Table;

class NullPartitionSchema : public PartitionSchema {
 public:
  explicit NullPartitionSchema(Table& table);

  // Return false, since NullPartitioningSchema is only a list of chunks.
  // Indicates that NullPartitionSchema can handle:
  // create_new_chunk, emplace_chunk, get_chunk, get_chunk_with_access_counting
  bool is_partitioned() const { return false; }

  // from abstract class PartitionSchema
  void add_column(DataType data_type, bool nullable);
  void append(std::vector<AllTypeVariant> values);
  ChunkID chunk_count() const;
  TableType get_type() const;
  uint64_t row_count() const;

  template <typename T>
  T get_value(const ColumnID column_id, const size_t row_number) const {
    return _partitions.front()->get_value<T>(column_id, row_number);
  }

  // specific for NullPartitionSchema
  void create_new_chunk();
  void emplace_chunk(Chunk& chunk);
  Chunk& get_chunk(ChunkID chunk_id);
  const Chunk& get_chunk(ChunkID chunk_id) const;
  ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id);
  const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const;
};

}  // namespace opossum
