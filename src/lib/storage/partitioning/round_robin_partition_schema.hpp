#pragma once

#include <map>

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
  explicit RoundRobinPartitionSchema(PartitionID number_of_partitions);

  std::string name() const override;
  PartitionSchemaType get_type() const override;

  void append(const std::vector<AllTypeVariant>& values) override;

  PartitionID get_matching_partition_for(const std::vector<AllTypeVariant>& values) const override;
  std::map<RowID, PartitionID> get_mapping_to_partitions(std::shared_ptr<const Table> table) const override;
  PartitionID get_next_partition() const;

 protected:
  PartitionID _number_of_partitions;
  mutable PartitionID _next_partition;

  void _go_to_next_partition() const;
};

}  // namespace opossum
