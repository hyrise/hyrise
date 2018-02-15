#pragma once

#include "all_type_variant.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "storage/partitioning/hash_function.hpp"
#include "types.hpp"

namespace opossum {

/*
 * PartitionSchema distributing tuples over a number of Partitions (defined in the constructor)
 * using the hash value of the tuple's value in the column referenced by column_id.
 * Hash values are calculated by hash_function.
 */

class HashPartitionSchema : public AbstractPartitionSchema {
 public:
  HashPartitionSchema(ColumnID column_id, HashFunction hash_function, PartitionID number_of_partitions);

  std::string name() const override;
  PartitionSchemaType get_type() const override;

  void append(const std::vector<AllTypeVariant>& values) override;

  PartitionID get_matching_partition_for(const std::vector<AllTypeVariant>& values) const override;
  PartitionID get_matching_partition_for(const AllTypeVariant& value) const;
  std::vector<ChunkID> get_chunk_ids_to_exclude(PredicateCondition condition, const AllTypeVariant& value) const override;

  ColumnID get_column_id() const;

 protected:
  ColumnID _column_id;
  HashFunction _hash_function;
  PartitionID _number_of_partitions;
};

}  // namespace opossum
