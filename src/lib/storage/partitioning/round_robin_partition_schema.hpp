#pragma once

#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>

namespace opossum {

class RoundRobinPartitionSchema : public PartitionSchema {
 public:
  explicit RoundRobinPartitionSchema(size_t number_of_partitions);

 protected:
  int _number_of_partitions;
  PartitionID _next_partition;
};

}  // namespace opossum
