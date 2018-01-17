#include "storage/partitioning/partition.hpp"
#include "resolve_type.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

Partition::Partition(PartitionID partition_id) : _partition_id(partition_id) {}

const PartitionID Partition::get_partition_id() { return _partition_id; }

void Partition::add_new_chunk(std::shared_ptr<Chunk> chunk) {
  _chunks.emplace_back(chunk);
}

void Partition::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                       const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  DebugAssert(_chunks.back()->size() < max_chunk_size, "Chunk is full");
  _chunks.back()->append(values);
}

std::shared_ptr<Chunk> Partition::last_chunk() {
  return _chunks.back();
}

}  // namespace opossum
