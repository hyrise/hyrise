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

  virtual void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                      const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) = 0;
  virtual PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) = 0;

  virtual void add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id);
  virtual bool is_partitioned() const { return true; }

  std::shared_ptr<Partition> get_partition(PartitionID partition_id);

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
};

}  // namespace opossum
