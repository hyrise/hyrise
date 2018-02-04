#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "types.hpp"

namespace opossum {

/*
 * PartitionSchema distributing tuples over a number of Partitions (defined in the constructor)
 * using round-robin scheduling. This way all Partitions are always almost equally filled.
 */

class RoundRobinPartitionSchema : public AbstractPartitionSchema {
 public:
  explicit RoundRobinPartitionSchema(size_t number_of_partitions);

  std::string name() const override;
  PartitionSchemaType get_type() const override;

  void append(std::vector<AllTypeVariant> values) override;

  RoundRobinPartitionSchema(RoundRobinPartitionSchema&&) = default;
  RoundRobinPartitionSchema& operator=(RoundRobinPartitionSchema&&) = default;

  PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) override;
  PartitionID get_next_partition();

 protected:
  int _number_of_partitions;
  PartitionID _next_partition;

  void _go_to_next_partition();
};

}  // namespace opossum
