#pragma once

#include <map>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/partitioning/abstract_partition_schema.hpp"
#include "types.hpp"

namespace opossum {

/*
 * This PartitionSchema assigns tuples to a Partition by the tuples value
 * in a specific column (specified by column_id).
 * Bounds are used for definining the Partitions.
 * Example:
 * Bounds [20, 50] creates three Partitions.
 * Partition 1 holds values <= 20
 * Partition 2 holds 20 < values <= 50
 * Partition 3 holds values > 50
 */

class RangePartitionSchema : public AbstractPartitionSchema {
 public:
  RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds);

  std::string name() const override;
  PartitionSchemaType get_type() const override;

  PartitionID get_matching_partition_for(const std::vector<AllTypeVariant>& values) const override;
  PartitionID get_matching_partition_for(const AllTypeVariant& value) const;
  std::map<RowID, PartitionID> get_mapping_to_partitions(std::shared_ptr<const Table> table) const override;
  std::vector<ChunkID> get_chunk_ids_to_exclude(PredicateCondition condition,
                                                const AllTypeVariant& value) const override;

  ColumnID get_column_id() const;
  const std::vector<AllTypeVariant>& get_bounds() const;
  DataType get_bound_type() const;

 protected:
  ColumnID _column_id;
  std::vector<AllTypeVariant> _bounds;
  DataType _bound_type;

  bool _partition_matches_condition(PartitionID partition_id, PredicateCondition condition,
                                    PartitionID matching_partition_id) const;
};

}  // namespace opossum
