#pragma once

#include <hash_function.hpp>
#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>

namespace opossum {

class HashPartitionSchema : public PartitionSchema {

public:
  HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions);

protected:
  ColumnID _column_id;
  HashFunction _hash_function;
  size_t _number_of_partitions;

};

}