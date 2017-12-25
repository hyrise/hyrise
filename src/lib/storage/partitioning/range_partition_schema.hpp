#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class RangePartitionSchema : public PartitionSchema {
 public:
  RangePartitionSchema(Table& table, ColumnID column_id, std::vector<AllTypeVariant> bounds);

  void append(std::vector<AllTypeVariant> values) override;
  ChunkID chunk_count() const override;
  TableType get_type() const override;
  uint64_t row_count() const override;

 protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;
};

}  // namespace opossum
