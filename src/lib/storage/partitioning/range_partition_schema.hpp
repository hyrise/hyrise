#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class RangePartitionSchema : public PartitionSchema {
 public:
  RangePartitionSchema(Table& table, ColumnID column_id, std::vector<AllTypeVariant> bounds);

  void add_column(DataType data_type, bool nullable);
  void append(std::vector<AllTypeVariant> values);
  ChunkID chunk_count() const;
  TableType get_type() const;
  AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const;
  uint64_t row_count() const;

 protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;
};

}  // namespace opossum
