#include "storage/partitioning/partition.hpp"
#include "resolve_type.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

void Partition::clear() { _chunks.clear(); }

void Partition::add_new_chunk(std::shared_ptr<Chunk> chunk) { _chunks.emplace_back(chunk); }

void Partition::append(const std::vector<AllTypeVariant>& values) { _chunks.back()->append(values); }

std::vector<std::shared_ptr<const Chunk>> Partition::get_chunks() const {
  std::vector<std::shared_ptr<const Chunk>> immutable_chunks;
  immutable_chunks.reserve(_chunks.size());
  std::copy(_chunks.begin(), _chunks.end(), std::back_inserter(immutable_chunks));
  return immutable_chunks;
}

std::shared_ptr<Chunk> Partition::last_chunk() const { return _chunks.back(); }

}  // namespace opossum
