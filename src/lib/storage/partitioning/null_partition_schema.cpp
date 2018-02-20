#include "storage/partitioning/null_partition_schema.hpp"

#include "storage/table.hpp"

namespace opossum {

NullPartitionSchema::NullPartitionSchema() { _partitions.emplace_back(std::make_shared<Partition>(PartitionID{0})); }

std::string NullPartitionSchema::name() const { return "NullPartition"; }

PartitionSchemaType NullPartitionSchema::get_type() const { return PartitionSchemaType::Null; }

std::map<RowID, PartitionID> NullPartitionSchema::get_mapping_to_partitions(std::shared_ptr<const Table> table) const {
  std::map<RowID, PartitionID> partition_mapping;
  for (ChunkID chunkID = ChunkID{0}; chunkID < table->chunk_count(); ++chunkID) {
    const auto source_chunk = table->get_chunk(chunkID);
    for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
      partition_mapping[{chunkID, rowID}] = PartitionID{0};
    }
  }
  return partition_mapping;
}

void NullPartitionSchema::append(const std::vector<AllTypeVariant>& values) {
  AbstractPartitionSchema::append(values, PartitionID{0});
}

}  // namespace opossum
