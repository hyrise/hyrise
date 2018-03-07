#pragma once

#include <map>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/partitioning/partition.hpp"
#include "storage/proxy_chunk.hpp"
#include "types.hpp"

namespace opossum {

class Table;

/*
 * PartitionSchema determine how to partition a table logically.
 * This is a strategy pattern with AbstractPartitionSchema defining the interface.
 * A PartitionSchema has a number of partitions referencing Chunks of the associated Table.
 * Chunks have to be created in the Table to be then passed by reference (i.e. shared_ptr)
 * to the PartitionSchema.
 */

class AbstractPartitionSchema : private Noncopyable {
 public:
  // string representation of the partition schema
  virtual std::string name() const = 0;

  // the partitions type
  virtual PartitionSchemaType get_type() const = 0;

  // number of partitions held by the schema
  PartitionID partition_count() const;

  // removes chunks from partitions
  void clear();

  // find partition the values belong in
  virtual PartitionID get_matching_partition_for(const std::vector<AllTypeVariant>& values) const = 0;

  // produces a map stating the partition each row is in
  virtual std::map<RowID, PartitionID> get_mapping_to_partitions(std::shared_ptr<const Table> table) const = 0;

  // returns ids of chunks not matching a given condition
  virtual std::vector<ChunkID> get_chunk_ids_to_exclude(PredicateCondition condition,
                                                        const AllTypeVariant& value) const;

  // adds a chunk to a partition
  virtual void add_new_chunk(std::shared_ptr<Chunk> chunk, PartitionID partition_id);

  // returns true, except for NullPartitionSchema
  virtual bool is_partitioned() const { return true; }

  // returns a partition referenced by id
  std::shared_ptr<const Partition> get_partition(PartitionID partition_id) const;

  // returns last chunk of a partition referenced by id
  std::shared_ptr<Chunk> last_chunk(PartitionID partition_id) const;

 protected:
  std::vector<std::shared_ptr<Partition>> _partitions;
};

}  // namespace opossum
