#include <storage/partitioning/range_partition_schema.hpp>

namespace opossum {

RangePartitionSchema::RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds)
    : _column_id(column_id), _bounds(bounds) {
  DebugAssert(std::all_of(bounds.cbegin(), bounds.cend(),
                          [&bounds](const AllTypeVariant& each) { return each.which() == bounds.front().which(); }),
              "All bounds have to be of the same type.");

  _bound_type = data_type_from_all_type_variant(bounds.front());
  _partitions.reserve(bounds.size() + 1);
  for (PartitionID index{0}; index < bounds.size() + 1; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>());
  }
}

std::string RangePartitionSchema::name() const { return "RangePartition"; }

PartitionSchemaType RangePartitionSchema::get_type() const { return PartitionSchemaType::Range; }

PartitionID RangePartitionSchema::get_matching_partition_for(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(values.size() > static_cast<size_t>(_column_id), "Can not determine partition, too few values given");
  const auto& value = values[_column_id];
  return get_matching_partition_for(value);
}

PartitionID RangePartitionSchema::get_matching_partition_for(const AllTypeVariant& value) const {
  auto bounds_iterator = std::lower_bound(_bounds.cbegin(), _bounds.cend(), value);
  return static_cast<PartitionID>(std::distance(_bounds.cbegin(), bounds_iterator));
}

std::map<RowID, PartitionID> RangePartitionSchema::get_mapping_to_partitions(std::shared_ptr<const Table> table) const {
  std::map<RowID, PartitionID> partition_mapping;
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    auto column_with_partitioning_values = source_chunk->get_column(get_column_id());
    for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
      partition_mapping[{chunk_id, rowID}] = get_matching_partition_for((*column_with_partitioning_values)[rowID]);
    }
  }
  return partition_mapping;
}

std::vector<ChunkID> RangePartitionSchema::get_chunk_ids_to_exclude(PredicateCondition condition,
                                                                    const AllTypeVariant& value) const {
  PartitionID matching_partition = get_matching_partition_for(value);
  std::vector<ChunkID> chunk_ids_to_exclude;
  for (PartitionID partition_id{0}; partition_id < partition_count(); partition_id++) {
    if (partition_id != matching_partition) {
      auto chunks_of_partition = get_partition(partition_id)->get_chunks();
      std::transform(chunks_of_partition.cbegin(), chunks_of_partition.cend(), std::back_inserter(chunk_ids_to_exclude),
                     [](auto chunk) { return chunk->id(); });
    }
  }
  return chunk_ids_to_exclude;
}

ColumnID RangePartitionSchema::get_column_id() const { return _column_id; }
const std::vector<AllTypeVariant>& RangePartitionSchema::get_bounds() const { return _bounds; }
DataType RangePartitionSchema::get_bound_type() const { return _bound_type; }

bool RangePartitionSchema::_partition_matches_condition(PartitionID partition_id, PredicateCondition condition,
                                                        PartitionID matching_partition_id) {
  switch (condition) {
    case PredicateCondition::Equals:
      return partition_id == matching_partition_id;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      return partition_id >= matching_partition_id;
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return partition_id <= matching_partition_id;
    default:
      // unable to exclude anything
      return true;
  }
}

}  // namespace opossum
