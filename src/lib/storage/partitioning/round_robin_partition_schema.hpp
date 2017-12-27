#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class RoundRobinPartitionSchema : public PartitionSchema {
 public:
  RoundRobinPartitionSchema(size_t number_of_partitions);

  void add_column(DataType data_type, bool nullable);
  void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
              const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables);
  ChunkID chunk_count() const;
  TableType get_type(uint16_t column_count) const;
  AllTypeVariant get_value(const ColumnID column_id, const size_t row_number) const;
  uint64_t row_count() const;

  RoundRobinPartitionSchema(RoundRobinPartitionSchema&&) = default;
  RoundRobinPartitionSchema& operator=(RoundRobinPartitionSchema&&) = default;

 protected:
  int _number_of_partitions;
  PartitionID _next_partition;
};

}  // namespace opossum
