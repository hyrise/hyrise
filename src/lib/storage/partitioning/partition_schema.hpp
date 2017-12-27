#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition.hpp"
#include "types.hpp"

namespace opossum {

class PartitionSchema {
 public:
  // Return true for all partition schemas except NullPartitonSchema.
  // This is for performance in insert operations, which are faster for unpartitioned tables.
  virtual bool is_partitioned() const { return true; }

  virtual void add_column(DataType data_type, bool nullable) = 0;
  virtual void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                      const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) = 0;
  virtual ChunkID chunk_count() const = 0;
  virtual TableType get_type(uint16_t column_count) const = 0;
  virtual AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const = 0;
  virtual uint64_t row_count() const = 0;

  PartitionSchema(PartitionSchema&&) = default;
  PartitionSchema& operator=(PartitionSchema&&) = default;

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
  PartitionSchema() {}
};

}  // namespace opossum
