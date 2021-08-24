#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace opossum {

size_t PartialHashIndex::estimate_memory_consumption(const ChunkOffset row_count, const ChunkOffset distinct_count,
                                                     const uint32_t value_bytes) {
  Fail("PartialHashIndex::estimate_memory_consumption() is not implemented yet");
}

PartialHashIndex::PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                                   const ColumnID column_id)
    : AbstractTableIndex{get_table_index_type_of<PartialHashIndex>()}, _column_id(column_id) {
  if (!chunks_to_index.empty()) {
    insert_entries(chunks_to_index);
  } else {
    /**
     * Because 'chunks_to_index' is empty, we cannot determine the data type of the column and therefore construct
     * an empty impl object. When chunks are added to this index, it is swapped out again.
     */
    _impl = std::make_shared<BasePartialHashIndexImpl>();
  }
}

size_t PartialHashIndex::insert_entries(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  if (!_is_initialized) {
    resolve_data_type(chunks_to_index.front().second->get_segment(_column_id)->data_type(),
                      [&](const auto column_data_type) {
                        using ColumnDataType = typename decltype(column_data_type)::type;
                        _impl = std::make_shared<PartialHashIndexImpl<ColumnDataType>>(chunks_to_index, _column_id);
                      });
    _is_initialized = true;
    return _impl->get_indexed_chunk_ids().size();
  } else {
    return _impl->insert_entries(chunks_to_index, _column_id);
  }
}

size_t PartialHashIndex::remove_entries(const std::vector<ChunkID>& chunks_to_remove) {
  if (!_is_initialized) {
    return 0;
  }
  return _impl->remove_entries(chunks_to_remove);
}

PartialHashIndex::IteratorPair PartialHashIndex::_range_equals(const AllTypeVariant& value) const {
  return _impl->range_equals(value);
}

std::pair<PartialHashIndex::IteratorPair, PartialHashIndex::IteratorPair> PartialHashIndex::_range_not_equals(
    const AllTypeVariant& value) const {
  return _impl->range_not_equals(value);
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const { return _impl->cbegin(); }

PartialHashIndex::Iterator PartialHashIndex::_cend() const { return _impl->cend(); }

PartialHashIndex::Iterator PartialHashIndex::_null_cbegin() const { return _impl->null_cbegin(); }

PartialHashIndex::Iterator PartialHashIndex::_null_cend() const { return _impl->null_cend(); }

size_t PartialHashIndex::_memory_consumption() const {
  size_t bytes{0u};
  bytes += sizeof(_impl);
  bytes += sizeof(_is_initialized);
  bytes += sizeof(_column_id);
  bytes += _impl->memory_consumption();
  return bytes;
}

bool PartialHashIndex::_is_index_for(const ColumnID column_id) const { return column_id == _column_id; }

std::set<ChunkID> PartialHashIndex::_get_indexed_chunk_ids() const { return _impl->get_indexed_chunk_ids(); }

}  // namespace opossum
