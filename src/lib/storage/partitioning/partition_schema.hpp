#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition.hpp"
#include "types.hpp"

namespace opossum {

class Partition;
class Table;

class PartitionSchema {
 public:
  // Return true for all partition schemas except NullPartitonSchema.
  // This is for performance in insert operations, which are faster for unpartitioned tables.
  virtual bool is_partitioned() const { return true; }

  virtual void add_column(DataType data_type, bool nullable) = 0;
  virtual void append(std::vector<AllTypeVariant> values) = 0;
  virtual ChunkID chunk_count() const = 0;
  virtual TableType get_type() const = 0;
  virtual AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const = 0;
  virtual uint64_t row_count() const = 0;

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
  Table& _table;

  explicit PartitionSchema(Table& table) : _table(table) {}
};

}  // namespace opossum
