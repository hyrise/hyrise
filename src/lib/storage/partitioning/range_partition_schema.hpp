#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class RangePartitionSchema : public PartitionSchema {
 public:
  RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds);

  std::string name() const override;

  void append(std::vector<AllTypeVariant> values) override;

  RangePartitionSchema(RangePartitionSchema&&) = default;
  RangePartitionSchema& operator=(RangePartitionSchema&&) = default;

  PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) override;

 protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;
};

}  // namespace opossum
