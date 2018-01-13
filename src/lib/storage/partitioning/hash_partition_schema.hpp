#pragma once

#include "all_type_variant.hpp"
#include "hash_function.hpp"
#include "storage/partitioning/partition_schema.hpp"
#include "types.hpp"

namespace opossum {

class HashPartitionSchema : public PartitionSchema {
 public:
  HashPartitionSchema(ColumnID column_id, HashFunction hash_function, size_t number_of_partitions);

  void append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
              const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) override;

  HashPartitionSchema(HashPartitionSchema&&) = default;
  HashPartitionSchema& operator=(HashPartitionSchema&&) = default;

  const PartitionID get_matching_partition_for(AllTypeVariant value) const override;

 protected:
  ColumnID _column_id;
  HashFunction _hash_function;
  size_t _number_of_partitions;
};

}  // namespace opossum
