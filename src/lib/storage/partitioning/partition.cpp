#include "storage/partitioning/partition.hpp"
#include "resolve_type.hpp"
#include "storage/reference_column.hpp"

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

TableType Partition::get_type() const {
  Assert(!_chunks.empty() && _table.column_count() > 0, "Table has no content, can't specify type");

  // We assume if one column is a reference column, all are.
  const auto column = _chunks[0].get_column(ColumnID{0});
  const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);

  if (ref_column != nullptr) {
// In debug mode we're pedantic and check whether all columns in all chunks are Reference Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < _table.column_count(); ++column_idx) {
        const auto column2 = _chunks[chunk_idx].get_column(ColumnID{column_idx});
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
      for (auto column_idx = ColumnID{0}; column_idx < _table.column_count(); ++column_idx) {
        const auto column2 = _chunks[chunk_idx].get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 == nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::Data;
  }
}

void Partition::emplace_chunk(Chunk& chunk) {
  if (_chunks.size() == 1 && (_chunks.back().column_count() == 0 || _chunks.back().size() == 0)) {
    // the initial chunk was not used yet
    _chunks.clear();
  }
  DebugAssert(chunk.column_count() > 0, "Trying to add chunk without columns.");
  DebugAssert(chunk.column_count() == _table.column_count(),
              std::string("adding chunk with ") + std::to_string(chunk.column_count()) + " columns to table with " +
                  std::to_string(_table.column_count()) + " columns");
  _chunks.emplace_back(std::move(chunk));
}

Chunk& Partition::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

const Chunk& Partition::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

ProxyChunk Partition::get_chunk_with_access_counting(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

const ProxyChunk Partition::get_chunk_with_access_counting(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

uint64_t Partition::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk.size();
  }
  return ret;
}

}  // namespace opossum
