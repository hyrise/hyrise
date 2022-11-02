#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace hyrise {

PartialHashIndex::PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                                   const ColumnID column_id)
    : AbstractTableIndex{TableIndexType::PartialHash}, _column_id(column_id) {
  Assert(!chunks_to_index.empty(), "PartialHashIndex requires chunks_to_index not to be empty.");
  resolve_data_type(chunks_to_index.front().second->get_segment(_column_id)->data_type(),
                    [&](const auto column_data_type) {
                      using ColumnDataType = typename decltype(column_data_type)::type;
                      _impl = std::make_shared<PartialHashIndexImpl<ColumnDataType>>(chunks_to_index, _column_id);
                    });
  insert_entries(chunks_to_index);
}

PartialHashIndex::PartialHashIndex(const DataType data_type, const ColumnID column_id)
    : AbstractTableIndex{TableIndexType::PartialHash}, _column_id(column_id) {
  resolve_data_type(data_type, [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    _impl = std::make_shared<PartialHashIndexImpl<ColumnDataType>>(
        std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{}, _column_id);
  });
}

size_t PartialHashIndex::insert_entries(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  return _impl->insert_entries(chunks_to_index, _column_id);
}

PartialHashIndex::IteratorPair PartialHashIndex::_range_equals(const AllTypeVariant& value) const {
  return _impl->range_equals(value);
}

std::pair<PartialHashIndex::IteratorPair, PartialHashIndex::IteratorPair> PartialHashIndex::_range_not_equals(
    const AllTypeVariant& value) const {
  return _impl->range_not_equals(value);
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const {
  return _impl->cbegin();
}

PartialHashIndex::Iterator PartialHashIndex::_cend() const {
  return _impl->cend();
}

PartialHashIndex::Iterator PartialHashIndex::_null_cbegin() const {
  return _impl->null_cbegin();
}

PartialHashIndex::Iterator PartialHashIndex::_null_cend() const {
  return _impl->null_cend();
}

size_t PartialHashIndex::_estimate_memory_usage() const {
  auto bytes = size_t{0u};
  bytes += sizeof(_column_id);
  bytes += sizeof(_impl);
  bytes += _impl->estimate_memory_usage();
  return bytes;
}

bool PartialHashIndex::_is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

std::unordered_set<ChunkID> PartialHashIndex::_get_indexed_chunk_ids() const {
  return _impl->get_indexed_chunk_ids();
}

ColumnID PartialHashIndex::_get_indexed_column_id() const {
  return _column_id;
}

}  // namespace hyrise
