#pragma once

#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>

namespace opossum {

class RoundRobinPartitionSchema : public PartitionSchema {
 public:
  explicit RoundRobinPartitionSchema(size_t number_of_partitions);

  void append(std::vector<AllTypeVariant> values) override;
  ChunkID chunk_count() const override;
  TableType get_type() const override;
  uint32_t max_chunk_size() const override;
  uint64_t row_count() const override;

 protected:
  int _number_of_partitions;
  PartitionID _next_partition;
};

}  // namespace opossum
