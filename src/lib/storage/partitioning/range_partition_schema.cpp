#include <storage/partitioning/range_partition_schema.hpp>

namespace opossum {

RangePartitionSchema::RangePartitionSchema(ColumnID column_id, std::vector<AllTypeVariant> bounds)
    : _column_id(column_id), _bounds(bounds) {
  Assert(std::all_of(bounds.cbegin(), bounds.cend(),
                     [&bounds](AllTypeVariant each){ return each.which() == bounds.front().which(); }),
         "All bounds have to be of the same type.");

  _bound_type = data_type_from_all_type_variant(bounds.front());
  _partitions.reserve(bounds.size() + 1);
  for (size_t index = 0; index < bounds.size() + 1; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

std::string RangePartitionSchema::name() const { return "RangePartition"; }

PartitionSchemaType RangePartitionSchema::get_type() const { return PartitionSchemaType::Range; }

void RangePartitionSchema::append(std::vector<AllTypeVariant> values) {
  AbstractPartitionSchema::append(values, get_matching_partition_for(values));
}

PartitionID RangePartitionSchema::get_matching_partition_for(std::vector<AllTypeVariant> values) {
  DebugAssert(values.size() > static_cast<size_t>(_column_id), "Can not determine partition, too few values given");
  auto value = values[_column_id];
  return get_matching_partition_for(value);
}

PartitionID RangePartitionSchema::get_matching_partition_for(AllTypeVariant value) {
  for (size_t index = 0; index < _bounds.size(); ++index) {
    if (value <= _bounds.at(index)) {
      return static_cast<PartitionID>(index);
    }
  }
  return static_cast<PartitionID>(_bounds.size());
}

const ColumnID RangePartitionSchema::get_column_id() const { return _column_id; }
const std::vector<AllTypeVariant> RangePartitionSchema::get_bounds() const { return _bounds; }
DataType RangePartitionSchema::get_bound_type() const { return _bound_type; }

}  // namespace opossum
