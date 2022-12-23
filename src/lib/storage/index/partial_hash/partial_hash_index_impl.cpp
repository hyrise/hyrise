#include "partial_hash_index_impl.hpp"
#include "storage/segment_iterate.hpp"

namespace hyrise {

template <typename DataType>
TableIndexIterator<DataType>::TableIndexIterator(MapIterator itr) : _map_iterator(itr), _vector_index(0) {}

template <typename DataType>
const RowID& TableIndexIterator<DataType>::operator*() const {
  return _map_iterator->second[_vector_index];
}

template <typename DataType>
TableIndexIterator<DataType>& TableIndexIterator<DataType>::operator++() {
  if (++_vector_index >= _map_iterator->second.size()) {
    _map_iterator++;
    _vector_index = 0;
  }
  return *this;
}

template <typename DataType>
bool TableIndexIterator<DataType>::operator==(const BaseTableIndexIterator& other) const {
  auto other_iterator = dynamic_cast<const TableIndexIterator*>(&other);
  return other_iterator && _map_iterator == other_iterator->_map_iterator &&
         _vector_index == other_iterator->_vector_index;
}

template <typename DataType>
bool TableIndexIterator<DataType>::operator!=(const BaseTableIndexIterator& other) const {
  auto other_iterator = dynamic_cast<const TableIndexIterator*>(&other);
  return !other_iterator || _map_iterator != other_iterator->_map_iterator ||
         _vector_index != other_iterator->_vector_index;
}

template <typename DataType>
std::shared_ptr<BaseTableIndexIterator> TableIndexIterator<DataType>::clone() const {
  return std::make_shared<TableIndexIterator<DataType>>(*this);
}

template <typename DataType>
IteratorWrapper TableIndexIterator<DataType>::wrap(MapIterator itr) {
  return IteratorWrapper(std::make_shared<TableIndexIterator<DataType>>(itr));
}

TableIndexNullValuesIterator::TableIndexNullValuesIterator(MapIterator itr) : _map_iterator(itr) {}

const RowID& TableIndexNullValuesIterator::operator*() const {
  return *_map_iterator;
}

TableIndexNullValuesIterator& TableIndexNullValuesIterator::operator++() {
  _map_iterator++;
  return *this;
}

bool TableIndexNullValuesIterator::operator==(const BaseTableIndexIterator& other) const {
  const auto* const other_iterator = dynamic_cast<const TableIndexNullValuesIterator*>(&other);
  return other_iterator && _map_iterator == other_iterator->_map_iterator;
}

bool TableIndexNullValuesIterator::operator!=(const BaseTableIndexIterator& other) const {
  const auto* const other_iterator = dynamic_cast<const TableIndexNullValuesIterator*>(&other);
  return !other_iterator || _map_iterator != other_iterator->_map_iterator;
}

std::shared_ptr<BaseTableIndexIterator> TableIndexNullValuesIterator::clone() const {
  return std::make_shared<TableIndexNullValuesIterator>(*this);
}

IteratorWrapper TableIndexNullValuesIterator::wrap(MapIterator itr) {
  return IteratorWrapper(std::make_shared<TableIndexNullValuesIterator>(itr));
}

template <typename DataType>
PartialHashIndexImpl<DataType>::PartialHashIndexImpl(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id)
    : BasePartialHashIndexImpl() {
  insert_entries(chunks_to_index, column_id);
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::insert_entries(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id) {
  auto indexed_chunks = size_t{0};
  for (const auto& chunk : chunks_to_index) {
    if (_indexed_chunk_ids.contains(chunk.first)) {
      // Index already contains entries for the given chunk.
      continue;
    }

    _indexed_chunk_ids.insert(chunk.first);
    ++indexed_chunks;

    // Iterate over the segment to index and populate the index.
    const auto& indexed_segment = chunk.second->get_segment(column_id);
    segment_iterate<DataType>(*indexed_segment, [&](const auto& position) {
      const auto row_id = RowID{chunk.first, position.chunk_offset()};
      // If value is NULL, add to NULL vector, otherwise add into value map.
      if (position.is_null()) {
        _null_values.emplace_back(row_id);
      } else {
        _map[position.value()].emplace_back(row_id);
      }
    });
  }

  return indexed_chunks;
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::remove_entries(const std::vector<ChunkID>& chunks_to_unindex) {
  const size_t size_before = _indexed_chunk_ids.size();

  auto indexed_chunks_to_unindex = std::unordered_set<ChunkID>{};
  for (const auto& chunk_id : chunks_to_unindex) {
    if (!_indexed_chunk_ids.contains(chunk_id)) {
      continue;
    }

    indexed_chunks_to_unindex.insert(chunk_id);
    _indexed_chunk_ids.erase(chunk_id);
  }

  // Checks whether a given RowID's ChunkID is in the set of ChunkIDs to be unindexed.
  auto is_to_unindex = [&indexed_chunks_to_unindex](const RowID& row_id) {
    const auto find_iter =
        std::find(indexed_chunks_to_unindex.cbegin(), indexed_chunks_to_unindex.cend(), row_id.chunk_id);
    return find_iter != indexed_chunks_to_unindex.cend();
  };

  // Iterate over all values stored in the index.
  auto map_iter = _map.begin();
  while (map_iter != _map.end()) {
    auto& row_ids = _map.at(map_iter->first);

    // Remove every RowID entry of the value that references one of the chunks.
    row_ids.erase(std::remove_if(row_ids.begin(), row_ids.end(), is_to_unindex), row_ids.end());

    map_iter = row_ids.empty() ? _map.erase(map_iter) : ++map_iter;
  }

  auto& nulls = _null_values;

  nulls.erase(std::remove_if(nulls.begin(), nulls.end(), is_to_unindex), nulls.end());

  return size_before - _indexed_chunk_ids.size();
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::IteratorPair PartialHashIndexImpl<DataType>::range_equals(
    const AllTypeVariant& value) const {
  const auto begin = _map.find(boost::get<DataType>(value));
  if (begin == _map.end()) {
    const auto end_iter = cend();
    return std::make_pair(end_iter, end_iter);
  }
  auto end = begin;
  return std::make_pair(TableIndexIterator<DataType>::wrap(begin), TableIndexIterator<DataType>::wrap(++end));
}

template <typename DataType>
std::pair<typename PartialHashIndexImpl<DataType>::IteratorPair, typename PartialHashIndexImpl<DataType>::IteratorPair>
PartialHashIndexImpl<DataType>::range_not_equals(const AllTypeVariant& value) const {
  const auto range_eq = range_equals(value);
  return std::make_pair(std::make_pair(cbegin(), range_eq.first), std::make_pair(range_eq.second, cend()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cbegin() const {
  return TableIndexIterator<DataType>::wrap(_map.cbegin());
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cend() const {
  return TableIndexIterator<DataType>::wrap(_map.cend());
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cbegin() const {
  return TableIndexNullValuesIterator::wrap(_null_values.cbegin());
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cend() const {
  return TableIndexNullValuesIterator::wrap(_null_values.cend());
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::estimate_memory_usage() const {
  auto bytes = size_t{0u};

  bytes += sizeof(_indexed_chunk_ids);  // NOLINT - Linter complains size() should be used, which would be wrong.
  bytes += sizeof(ChunkID) * _indexed_chunk_ids.size();

  bytes += sizeof(_map);
  bytes += sizeof(DataType) * _map.size();

  bytes += sizeof(std::vector<RowID>) * _map.size();
  bytes += sizeof(RowID) * std::distance(cbegin(), cend());

  bytes += sizeof(_null_values);  // NOLINT
  bytes += sizeof(RowID) * _null_values.size();

  return bytes;
}

template <typename DataType>
std::unordered_set<ChunkID> PartialHashIndexImpl<DataType>::get_indexed_chunk_ids() const {
  return _indexed_chunk_ids;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PartialHashIndexImpl);

}  // namespace hyrise
