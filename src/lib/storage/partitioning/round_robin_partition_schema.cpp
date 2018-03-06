#include "storage/partitioning/round_robin_partition_schema.hpp"

#include "storage/table.hpp"

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(PartitionID number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (PartitionID index{0}; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(index));
  }
}

std::string RoundRobinPartitionSchema::name() const { return "RoundRobinPartition"; }

PartitionSchemaType RoundRobinPartitionSchema::get_type() const { return PartitionSchemaType::RoundRobin; }

void RoundRobinPartitionSchema::append(const std::vector<AllTypeVariant>& values) {
  AbstractPartitionSchema::append(values, _next_partition);
  _go_to_next_partition();
}

PartitionID RoundRobinPartitionSchema::get_matching_partition_for(const std::vector<AllTypeVariant>& values) const {
  return get_next_partition();
}

std::map<RowID, PartitionID> RoundRobinPartitionSchema::get_mapping_to_partitions(
    std::shared_ptr<const Table> table) const {
  std::map<RowID, PartitionID> partition_mapping;
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
      partition_mapping[{chunk_id, rowID}] = get_next_partition();
    }
  }
  return partition_mapping;
}

PartitionID RoundRobinPartitionSchema::get_next_partition() const {
  _go_to_next_partition();
  return _next_partition;
}

void RoundRobinPartitionSchema::_go_to_next_partition() const {
  _next_partition = ++_next_partition % _partitions.size();
}

}  // namespace opossum
