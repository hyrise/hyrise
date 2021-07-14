#include "partial_hash_index_impl.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

template <typename DataType>
PartialHashIndexImpl<DataType>::PartialHashIndexImpl(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id, std::vector<RowID>&null_positions)
    : BasePartialHashIndexImpl(), _column_id(column_id) {
  // ToDo(pi) check all have same data type by taking first and comparing rest
  // ToDo(pi) set null positions
  for (const auto& chunk : chunks_to_index) {
    if (_indexed_chunk_ids.contains(chunk.first)) continue;

    _indexed_chunk_ids.insert(chunk.first);
    auto indexed_segment = chunk.second->get_segment(column_id);
    segment_iterate<DataType>(*indexed_segment, [&](const auto& position) {
      auto row_id = RowID{chunk.first, position.chunk_offset()};
      if (position.is_null()) {
        null_positions.emplace_back(row_id);
      } else {
        if (!_map.contains(position.value())) {
          _map[position.value()] = std::vector<RowID>();  // ToDo(pi) size
        }
        _map[position.value()].push_back(row_id);
        _row_ids.push_back(row_id);  //TODO(pi) Rethink concept
      }
    });
    _indexed_segments.push_back(indexed_segment);
  }
}

//ToDO(pi) change index (add) chunks later after creation

// ToDo(pi) return from cbegin to cend instead of map vectors
template <typename DataType>
std::pair<typename PartialHashIndexImpl<DataType>::Iterator, typename PartialHashIndexImpl<DataType>::Iterator> PartialHashIndexImpl<DataType>::equals(
    const AllTypeVariant& value) const {
  auto result = _map.find(get<DataType>(value));
  if (result == _map.end()) {
    auto end_iter = cend();
    return std::make_pair(end_iter, end_iter);
  }
  return std::make_pair(result->second.begin(), result->second.end());
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cbegin() const {
  return _row_ids.begin();
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cend() const {
  return _row_ids.end();
}

template <typename DataType>
std::vector<std::shared_ptr<const AbstractSegment>> PartialHashIndexImpl<DataType>::get_indexed_segments() const {
  return _indexed_segments;
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::memory_consumption() const {
  return 0;
}

template <typename DataType>
bool PartialHashIndexImpl<DataType>::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

template <typename DataType>
std::set<ChunkID> PartialHashIndexImpl<DataType>::get_indexed_chunk_ids() const {
  return _indexed_chunk_ids;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PartialHashIndexImpl);


}  // namespace opossum
