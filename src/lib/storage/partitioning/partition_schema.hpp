#pragma once

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/partitioning/partition.hpp"
#include "storage/proxy_chunk.hpp"
#include "types.hpp"

namespace opossum {

class PartitionSchema {
 public:
  PartitionSchema() = default;
  virtual ~PartitionSchema() = default;

  PartitionSchema(PartitionSchema&&) = default;
  PartitionSchema& operator=(PartitionSchema&&) = default;

  void add_column(DataType data_type, bool nullable);
  ChunkID chunk_count() const;
  uint64_t row_count() const;

  // These function have to be implemented by non-abstract PartitionSchemas.
  virtual void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                      const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) = 0;
  virtual TableType get_type(uint16_t column_count) const = 0;
  virtual AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const = 0;

  // These function me be overriden to check if chunk modifications obey partitioning rules.
  virtual void create_new_chunk(const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables,
                                PartitionID partition_id);
  virtual void emplace_chunk(Chunk& chunk, uint16_t column_count, PartitionID partition_id);
  virtual Chunk& get_modifiable_chunk(ChunkID chunk_id, PartitionID partition_id);
  virtual const Chunk& get_chunk(ChunkID chunk_id, PartitionID partition_id) const;
  virtual ProxyChunk get_modifiable_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id);
  virtual const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id, PartitionID partition_id) const;

  virtual bool is_partitioned() const { return true; }

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;

};

}  // namespace opossum
