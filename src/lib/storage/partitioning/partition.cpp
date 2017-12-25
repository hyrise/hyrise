#include "storage/partitioning/partition.hpp"
#include "resolve_type.hpp"

namespace opossum {

Partition::Partition(Table& table) : _table(table) { _chunks.push_back(Chunk{ChunkUseMvcc::Yes}); }

void Partition::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back().size() == _table.max_chunk_size()) create_new_chunk();

  _chunks.back().append(values);
}

ChunkID Partition::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

void Partition::create_new_chunk() {
  // Create chunk with mvcc columns
  Chunk new_chunk{ChunkUseMvcc::Yes};

  auto column_types = _table.column_types();
  auto column_nullable = _table.column_nullables();

  for (auto column_id = 0u; column_id < column_types.size(); ++column_id) {
    const auto type = column_types[column_id];
    auto nullable = column_nullable[column_id];

    new_chunk.add_column(make_shared_by_data_type<BaseColumn, ValueColumn>(type, nullable));
  }
  _chunks.push_back(std::move(new_chunk));
}

uint64_t Partition::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk.size();
  }
  return ret;
}

}  // namespace opossum
