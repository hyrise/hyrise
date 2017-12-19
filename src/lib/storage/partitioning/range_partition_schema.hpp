#pragma once

#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>
#include <all_type_variant.hpp>

namespace opossum {

class RangePartitionSchema : public PartitionSchema {

public:
  RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds);

protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;

};

}
