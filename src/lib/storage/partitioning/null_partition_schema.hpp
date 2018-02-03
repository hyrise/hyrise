#pragma once

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

  void append(std::vector<AllTypeVariant> values) override;

  // Return false, since NullPartitioningSchema is only a list of chunks.
  bool is_partitioned() const override { return false; }

  PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) override { return PartitionID{0}; };
};

}  // namespace opossum
