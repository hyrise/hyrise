#pragma once

#include <hash_function.h>
#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>

class HashPartitionSchema : public PartitionSchema {

  ColumnID column_id;
  HashFunction hash_function;
  int number_of_partitions;

};
