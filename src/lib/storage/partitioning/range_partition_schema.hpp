#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "types.hpp"

namespace opossum {

/*
 * This PartitionSchema assigns tuples to a Partition by the tuples value
 * in a specific column (specified by column_id).
 * Bounds are used for definining the Partitions.
 * Example:
 * Bounds [20, 50] creates three Partitions.
 * Partition 1 holds values <= 20
 * Partition 2 holds 20 < values <= 50
 * Partition 3 holds values > 50
 */

class RangePartitionSchema : public AbstractPartitionSchema {
 public:
  RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds);

  std::string name() const override;

  void append(std::vector<AllTypeVariant> values) override;

  RangePartitionSchema(RangePartitionSchema&&) = default;
  RangePartitionSchema& operator=(RangePartitionSchema&&) = default;

  PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) override;
  PartitionID get_matching_partition_for(AllTypeVariant value);

  const ColumnID get_column_id();

 protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;
};

}  // namespace opossum
