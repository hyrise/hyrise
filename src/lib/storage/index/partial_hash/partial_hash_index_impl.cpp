#include "partial_hash_index_impl.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

template <typename DataType>
PartialHashIndexImpl<DataType>::PartialHashIndexImpl(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id)
    : BasePartialHashIndexImpl() {
  add(chunks_to_index, column_id);
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::add(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id) {
  // ToDo(pi) check all have same data type by taking first and comparing rest
  size_t size_before = _indexed_chunk_ids.size();
  for (const auto& chunk : chunks_to_index) {
    if (_indexed_chunk_ids.contains(chunk.first)) continue;

    _indexed_chunk_ids.insert(chunk.first);
    auto indexed_segment = chunk.second->get_segment(column_id);
    segment_iterate<DataType>(*indexed_segment, [&](const auto& position) {
      auto row_id = RowID{chunk.first, position.chunk_offset()};
      if (position.is_null()) {
        if (!_null_values.contains(true)) {
          _null_values[true] = std::vector<RowID>();  // ToDo(pi) size
        }
        _null_values[true].push_back(row_id);
      } else {
        if (!_map.contains(position.value())) {
          _map[position.value()] = std::vector<RowID>();  // ToDo(pi) size
        }
        _map[position.value()].push_back(row_id);
      }
    });
  }

  return _indexed_chunk_ids.size() - size_before;
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::remove(const std::vector<ChunkID>& chunks_to_remove) {
  size_t size_before = _indexed_chunk_ids.size();
  for (const auto chunk_id : chunks_to_remove) {
    if (!_indexed_chunk_ids.contains(chunk_id)) continue;

    _indexed_chunk_ids.erase(chunk_id);
    auto map_iter = _map.begin();
    while (map_iter != _map.end()) {
      auto& values = _map.at(map_iter->first);
      values.erase(std::remove_if(values.begin(), values.end(),
                                  [chunk_id](RowID& row_id) { return row_id.chunk_id == chunk_id; }),
                   values.end());
      if (values.empty()) {
        map_iter = _map.erase(map_iter);
      } else {
        ++map_iter;
      }
    }
    auto& _nulls = _null_values[true];
    _nulls.erase(
        std::remove_if(_nulls.begin(), _nulls.end(), [chunk_id](RowID& row_id) { return row_id.chunk_id == chunk_id; }),
        _nulls.end());
  }

  return size_before - _indexed_chunk_ids.size();
}

template <typename DataType>
PartialHashIndexImpl<DataType>::IteratorPair PartialHashIndexImpl<DataType>::equals(const AllTypeVariant& value) const {
  auto begin = _map.find(boost::get<DataType>(value));
  if (begin == _map.end()) {
    auto end_iter = cend();
    return std::make_pair(end_iter, end_iter);
  }
  auto end = begin;
  return std::make_pair(Iterator(std::make_shared<TableIndexIterator<DataType>>(begin)),
                        Iterator(std::make_shared<TableIndexIterator<DataType>>(++end)));
}

template <typename DataType>
std::pair<typename PartialHashIndexImpl<DataType>::IteratorPair, typename PartialHashIndexImpl<DataType>::IteratorPair>
PartialHashIndexImpl<DataType>::not_equals(const AllTypeVariant& value) const {
  auto eq_begin = _map.find(boost::get<DataType>(value));
  auto eq_end = eq_begin;
  if (eq_begin != _map.cend()) {
    ++eq_end;
  }
  // (cbegin -> eq_begin) + (eq_end -> cend)
  return std::make_pair(std::make_pair(cbegin(), Iterator(std::make_shared<TableIndexIterator<DataType>>(eq_begin))),
                        std::make_pair(Iterator(std::make_shared<TableIndexIterator<DataType>>(eq_end)), cend()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cbegin() const {
  return Iterator(std::make_shared<TableIndexIterator<DataType>>(_map.cbegin()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cend() const {
  return Iterator(std::make_shared<TableIndexIterator<DataType>>(_map.cend()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cbegin() const {
  return Iterator(std::make_shared<TableIndexIterator<bool>>(_null_values.cbegin()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cend() const {
  return Iterator(std::make_shared<TableIndexIterator<bool>>(_null_values.cend()));
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::memory_consumption() const {
  return 0;
}

template <typename DataType>
std::set<ChunkID> PartialHashIndexImpl<DataType>::get_indexed_chunk_ids() const {
  return _indexed_chunk_ids;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PartialHashIndexImpl);

}  // namespace opossum
