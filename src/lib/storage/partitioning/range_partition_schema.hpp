#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "types.hpp"

namespace opossum {

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
