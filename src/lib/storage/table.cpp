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

Table::Table(const TableLayout& layout, const ChunkUseMvcc use_mvcc, const uint32_t max_chunk_size)
    : _layout(layout), _use_mvcc(use_mvcc), _max_chunk_size(max_chunk_size), _append_mutex(std::make_unique<std::mutex>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

void Table::append(std::vector<AllTypeVariant> values) {
  // TODO(Anyone): Chunks should be preallocated for chunk size
  if (_chunks.back()->size() == _max_chunk_size) emplace_chunk();

  _chunks.back()->append(values);
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

TableType Table::get_type() const {
  // Cannot answer this if the table has no content
  Assert(!_chunks.empty() && column_count() > 0, "Table has no content, can't specify type");

  // We assume if one column is a reference column, all are.
  const auto column = _chunks[0]->get_column(ColumnID{0});
  const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);

  if (ref_column != nullptr) {
// In debug mode we're pedantic and check whether all columns in all chunks are Reference Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < column_count(); ++column_idx) {
        const auto column2 = _chunks[chunk_idx]->get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 != nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::References;
  } else {
// In debug mode we're pedantic and check whether all columns in all chunks are Value/Dict Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < column_count(); ++column_idx) {
        const auto column2 = _chunks[chunk_idx]->get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 == nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::Data;
  }
}

std::vector<IndexInfo> Table::get_indexes() const { return _indexes; }

}  // namespace opossum
