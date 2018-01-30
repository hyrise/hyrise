#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "storage/partitioning/hash_function.hpp"
#include "types.hpp"

namespace opossum {

class HashPartitionSchema : public AbstractPartitionSchema {
 public:
  HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions);

  std::string name() const override;

  void append(std::vector<AllTypeVariant> values) override;

  HashPartitionSchema(HashPartitionSchema&&) = default;
  HashPartitionSchema& operator=(HashPartitionSchema&&) = default;

  PartitionID get_matching_partition_for(std::vector<AllTypeVariant> values) override;
  PartitionID get_matching_partition_for(AllTypeVariant value);

  const ColumnID get_column_id();

 protected:
  ColumnID _column_id;
  HashFunction _hash_function;
  size_t _number_of_partitions;
};

}  // namespace opossum
