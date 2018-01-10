#pragma once

#include "all_type_variant.hpp"
#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "storage/proxy_chunk.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class Partition {
 public:
  Partition(PartitionID partition_id);

  const PartitionID get_partition_id();

  void add_column(DataType data_type, bool nullable);
  void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
              const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables);
  ChunkID chunk_count() const;
  void create_new_chunk(const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables);
  void emplace_chunk(Chunk& chunk, uint16_t column_count);
  Chunk& get_modifiable_chunk(ChunkID chunk_id);
  const Chunk& get_chunk(ChunkID chunk_id) const;
  ProxyChunk get_modifiable_chunk_with_access_counting(ChunkID chunk_id);
  const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const;
  TableType get_type(uint16_t column_count) const;
  AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const;
  uint64_t row_count() const;

  Partition(Partition&&) = default;
  Partition& operator=(Partition&&) = default;

 protected:
  std::vector<Chunk> _chunks;
  const PartitionID _partition_id;
};

}  // namespace opossum
