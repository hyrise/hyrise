#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

namespace opossum {

Table::Table(const TableLayout& layout, const UseMvcc use_mvcc, const uint32_t max_chunk_size)
    : _layout(layout), _use_mvcc(use_mvcc), _max_chunk_size(max_chunk_size), _append_mutex(std::make_unique<std::mutex>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

void Table::append(std::vector<AllTypeVariant> values) {
  Fail("Todo");
//  // TODO(Anyone): Chunks should be preallocated for chunk size
//  if (_chunks.back()->size() == _max_chunk_size) emplace_chunk();

//  _chunks.back()->append(values);
}

const TableLayout& Table::layout() const {
  return _layout;
}

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk->size();
  }
  return ret;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

const ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

std::shared_ptr<Chunk> Table::emplace_chunk(const std::vector<std::shared_ptr<BaseColumn>>& columns,
                                            const std::optional<PolymorphicAllocator<Chunk>>& alloc,
                                            const std::shared_ptr<Chunk::AccessCounter>& access_counter) {
  _chunks.emplace_back(std::make_shared<Chunk>(_layout, _use_mvcc, alloc, access_counter));
  return _chunks.back();
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

std::vector<IndexInfo> Table::get_indexes() const { return _indexes; }

size_t Table::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& chunk : _chunks) {
    bytes += chunk->estimate_memory_usage();
  }

  for (const auto& column_definition : _layout.column_definitions) {
    bytes += column_definition.name.size();
  }

  // TODO(anybody) Statistics and Indices missing from Memory Usage Estimation
  // TODO(anybody) TableLayout missing

  return bytes;
}

}  // namespace opossum
