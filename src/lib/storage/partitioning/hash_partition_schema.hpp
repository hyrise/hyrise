#pragma once

#include "hash_function.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class HashPartitionSchema : public PartitionSchema {
 public:
  HashPartitionSchema(Table& table, ColumnID column_id, HashFunction hash_function, size_t number_of_partitions);

  void append(std::vector<AllTypeVariant> values);
  ChunkID chunk_count() const;
  TableType get_type() const;
  uint64_t row_count() const;

 protected:
  ColumnID _column_id;
  HashFunction _hash_function;
  size_t _number_of_partitions;
};

}  // namespace opossum
