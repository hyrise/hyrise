#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class RoundRobinPartitionSchema : public PartitionSchema {
 public:
  explicit RoundRobinPartitionSchema(size_t number_of_partitions);

  void append(std::vector<AllTypeVariant> values) override;

  RoundRobinPartitionSchema(RoundRobinPartitionSchema&&) = default;
  RoundRobinPartitionSchema& operator=(RoundRobinPartitionSchema&&) = default;

  PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) override;

 protected:
  int _number_of_partitions;
  PartitionID _next_partition;
};

}  // namespace opossum
