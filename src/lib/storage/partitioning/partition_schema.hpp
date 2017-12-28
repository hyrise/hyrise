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

  virtual void add_column(DataType data_type, bool nullable) = 0;
  virtual void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                      const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) = 0;
  virtual ChunkID chunk_count() const = 0;
  virtual TableType get_type(uint16_t column_count) const = 0;
  virtual AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const = 0;
  virtual uint64_t row_count() const = 0;

  // The following functions can be overriden,
  // when a partition schema is continous and hence can make sense of them.
  // For example, the NullPartitionSchema implements them.
  // This results in speed-up of some operators (e.g. insert).
  virtual void create_new_chunk(const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
    throw "create_new_chunk can not be used on partitioned tables";
  }
  virtual void emplace_chunk(Chunk& chunk, uint16_t column_count) {
    throw "emplace_chunk can not be used on partitioned tables";
  }
  virtual Chunk& get_chunk(ChunkID chunk_id) { throw "get_chunk can not be used on partitioned tables"; }
  virtual const Chunk& get_chunk(ChunkID chunk_id) const { throw "get_chunk can not be used on partitioned tables"; }
  virtual ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) {
    throw "get_chunk_with_access_counting can not be used on partitioned tables";
  }
  virtual const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const {
    throw "get_chunk_with_access_counting can not be used on partitioned tables";
  }

  const std::shared_ptr<Partition> find_partition(PartitionID partition_to_find) {
    for (std::shared_ptr<Partition> partition_ptr : _partitions) {
      if (partition_ptr->get_partition_id() == partition_to_find) {
        return partition_ptr;
      }
    }
    throw "Cannot find specified partition!";
  }

  const PartitionID max_partition_id() {
    if (_partitions.size() == 0) {
      throw "The minimum number of partitions is 1!";
    }
    PartitionID max = PartitionID{0};
    for (std::shared_ptr<Partition> partition_ptr : _partitions) {
      if (partition_ptr->get_partition_id() > max) {
        max = partition_ptr->get_partition_id();
      }
    }
    return max;
  }

  // Indicates that the functions above are
  //   1. not meaningfully implemented if true is returned (default case)
  //   2. meaningfully implemented if false is returned
  virtual bool is_partitioned() const { return true; }

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
};

}  // namespace opossum
