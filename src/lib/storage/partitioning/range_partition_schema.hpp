#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class RangePartitionSchema : public PartitionSchema {
 public:
  RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds);

  void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
              const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables);
  TableType get_type(uint16_t column_count) const;
  AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const;

  RangePartitionSchema(RangePartitionSchema&&) = default;
  RangePartitionSchema& operator=(RangePartitionSchema&&) = default;

 protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;
};

}  // namespace opossum
