#pragma once

#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class RoundRobinPartitionSchema : public PartitionSchema {
 public:
  RoundRobinPartitionSchema(Table& table, size_t number_of_partitions);

  void append(std::vector<AllTypeVariant> values) override;
  ChunkID chunk_count() const override;
  TableType get_type() const override;
  uint64_t row_count() const override;

 protected:
  int _number_of_partitions;
  PartitionID _next_partition;
};

}  // namespace opossum
