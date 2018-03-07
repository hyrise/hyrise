#pragma once

#include <map>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "storage/proxy_chunk.hpp"

namespace opossum {

/*
 * This is a null object for the PartitionSchema strategy.
 * If there should not be any partitioning, NullPartitionSchema is used.
 * It holds exactly one Partition where all Chunks of a Table are referenced in.
 */

class NullPartitionSchema : public AbstractPartitionSchema {
 public:
  NullPartitionSchema();

  std::string name() const override;
  PartitionSchemaType get_type() const override;

  // Return false, since NullPartitioningSchema is only a list of chunks.
  bool is_partitioned() const override { return false; }

  PartitionID get_matching_partition_for(const std::vector<AllTypeVariant>& values) const override {
    return PartitionID{0};
  };
  std::map<RowID, PartitionID> get_mapping_to_partitions(std::shared_ptr<const Table> table) const override;
};

}  // namespace opossum
