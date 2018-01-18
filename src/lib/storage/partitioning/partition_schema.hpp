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

  virtual void append(std::vector<AllTypeVariant> values) = 0;
  void append(std::vector<AllTypeVariant> values, PartitionID partition_id);

  virtual PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) = 0;

  virtual void add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id);
  virtual bool is_partitioned() const { return true; }

  std::shared_ptr<Partition> get_partition(PartitionID partition_id);
  std::shared_ptr<Chunk> last_chunk(PartitionID partition_id);

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
};

}  // namespace opossum
