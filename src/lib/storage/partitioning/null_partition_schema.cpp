#include "storage/partitioning/null_partition_schema.hpp"

#include "storage/table.hpp"

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>()); }

std::string NullPartitionSchema::name() const { return "NullPartition"; }

PartitionSchemaType NullPartitionSchema::get_type() const { return PartitionSchemaType::Null; }

std::map<RowID, PartitionID> NullPartitionSchema::get_mapping_to_partitions(std::shared_ptr<const Table> table) const {
  std::map<RowID, PartitionID> partition_mapping;
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
      partition_mapping[{chunk_id, rowID}] = PartitionID{0};
    }
  }
  return partition_mapping;
}

}  // namespace opossum
