#pragma once

#include <hash_function.hpp>
#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>

namespace opossum {

class HashPartitionSchema : public PartitionSchema {
 public:
  HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions);

  void append(std::vector<AllTypeVariant> values) override;
  ChunkID chunk_count() const override;
  TableType get_type() const override;
  uint32_t max_chunk_size() const override;
  uint64_t row_count() const override;

 protected:
  ColumnID _column_id;
  HashFunction _hash_function;
  size_t _number_of_partitions;
};

}  // namespace opossum
