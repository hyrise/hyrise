#include "partial_hash_index_impl.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

template <typename DataType>
TableIndexFlattenedSparseMapIterator<DataType>::TableIndexFlattenedSparseMapIterator(MapIteratorType itr)
    : _map_iterator(itr), _vector_index(0) {}

template <typename DataType>
const RowID& TableIndexFlattenedSparseMapIterator<DataType>::operator*() const {
  return _map_iterator->second[_vector_index];
}

template <typename DataType>
TableIndexFlattenedSparseMapIterator<DataType>& TableIndexFlattenedSparseMapIterator<DataType>::operator++() {
  if (++_vector_index >= _map_iterator->second.size()) {
    _map_iterator++;
    _vector_index = 0;
  }
  return *this;
}

template <typename DataType>
bool TableIndexFlattenedSparseMapIterator<DataType>::operator==(const BaseTableIndexIterator& other) const {
  auto obj = dynamic_cast<const TableIndexFlattenedSparseMapIterator*>(&other);
  return obj && _map_iterator == obj->_map_iterator && _vector_index == obj->_vector_index;
}

template <typename DataType>
bool TableIndexFlattenedSparseMapIterator<DataType>::operator!=(const BaseTableIndexIterator& other) const {
  auto obj = dynamic_cast<const TableIndexFlattenedSparseMapIterator*>(&other);
  return !obj || _map_iterator != obj->_map_iterator || _vector_index != obj->_vector_index;
}

template <typename DataType>
std::shared_ptr<BaseTableIndexIterator> TableIndexFlattenedSparseMapIterator<DataType>::clone() const {
  return std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(*this);
}

TableIndexVectorIterator::TableIndexVectorIterator(MapIteratorType itr) : _map_iterator(itr) {}

const RowID& TableIndexVectorIterator::operator*() const {
  return *_map_iterator;
}

TableIndexVectorIterator& TableIndexVectorIterator::operator++() {
  _map_iterator++;
  return *this;
}

bool TableIndexVectorIterator::operator==(const BaseTableIndexIterator& other) const {
  auto obj = dynamic_cast<const TableIndexVectorIterator*>(&other);
  return obj && _map_iterator == obj->_map_iterator;
}

bool TableIndexVectorIterator::operator!=(const BaseTableIndexIterator& other) const {
  auto obj = dynamic_cast<const TableIndexVectorIterator*>(&other);
  return !obj || _map_iterator != obj->_map_iterator;
}

std::shared_ptr<BaseTableIndexIterator> TableIndexVectorIterator::clone() const {
  return std::make_shared<TableIndexVectorIterator>(*this);
}

size_t BasePartialHashIndexImpl::insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&,
                                                const ColumnID) {
  return 0;
}

size_t BasePartialHashIndexImpl::remove_entries(const std::vector<ChunkID>&) {
  return 0;
}

BasePartialHashIndexImpl::Iterator BasePartialHashIndexImpl::cbegin() const {
  return Iterator(std::make_shared<BaseTableIndexIterator>());
}

BasePartialHashIndexImpl::Iterator BasePartialHashIndexImpl::cend() const {
  return Iterator(std::make_shared<BaseTableIndexIterator>());
}

BasePartialHashIndexImpl::Iterator BasePartialHashIndexImpl::null_cbegin() const {
  return Iterator(std::make_shared<BaseTableIndexIterator>());
}

BasePartialHashIndexImpl::Iterator BasePartialHashIndexImpl::null_cend() const {
  return Iterator(std::make_shared<BaseTableIndexIterator>());
}

size_t BasePartialHashIndexImpl::memory_consumption() const {
  return 0;
}

BasePartialHashIndexImpl::IteratorPair BasePartialHashIndexImpl::range_equals(const AllTypeVariant& value) const {
  return std::make_pair(Iterator(std::make_shared<BaseTableIndexIterator>()),
                        Iterator(std::make_shared<BaseTableIndexIterator>()));
}

std::pair<BasePartialHashIndexImpl::IteratorPair, BasePartialHashIndexImpl::IteratorPair>
BasePartialHashIndexImpl::range_not_equals(const AllTypeVariant& value) const {
  return std::make_pair(range_equals(value), range_equals(value));
}

bool BasePartialHashIndexImpl::is_index_for(const ColumnID column_id) const {
  return false;
}

std::set<ChunkID> BasePartialHashIndexImpl::get_indexed_chunk_ids() const {
  return std::set<ChunkID>();
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
  size_t size_before = _indexed_chunk_ids.size();
  for (const auto& chunk : chunks_to_index) {
    // We do not allow multiple indexing of one chunk.
    if (_indexed_chunk_ids.contains(chunk.first))
      continue;

    _indexed_chunk_ids.insert(chunk.first);
    // Iterate over the segment to index and populate the index.
    auto indexed_segment = chunk.second->get_segment(column_id);
    segment_iterate<DataType>(*indexed_segment, [&](const auto& position) {
      const auto row_id = RowID{chunk.first, position.chunk_offset()};
      // If value is NULL, add to NULL vector, otherwise add into value map.
      if (position.is_null()) {
        _null_values.push_back(row_id);
      } else {
        if (!_map.contains(position.value())) {
          _map[position.value()] = std::vector<RowID>();
        }
        _map[position.value()].push_back(row_id);
      }
    });
  }

  return _indexed_chunk_ids.size() - size_before;
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::remove_entries(const std::vector<ChunkID>& chunks_to_unindex) {
  const size_t size_before = _indexed_chunk_ids.size();

  auto chunks_to_unindex_filtered = std::set<ChunkID>{};
  for (const auto& chunk_id : chunks_to_unindex) {
    if (!_indexed_chunk_ids.contains(chunk_id))
      continue;

    chunks_to_unindex_filtered.insert(chunk_id);
    _indexed_chunk_ids.erase(chunk_id);
  }

  // Iterate over all values stored in the index.
  auto map_iter = _map.begin();
  while (map_iter != _map.end()) {
    // Remove every RowID entry of the value that references one of the chunks.
    auto& entries_for_value = _map.at(map_iter->first);
    entries_for_value.erase(std::remove_if(entries_for_value.begin(), entries_for_value.end(),
                                           [chunks_to_unindex_filtered](RowID& row_id) {
                                             return chunks_to_unindex_filtered.end() !=
                                                    std::find(chunks_to_unindex_filtered.begin(),
                                                              chunks_to_unindex_filtered.end(), row_id.chunk_id);
                                           }),
                            entries_for_value.end());
    if (entries_for_value.empty()) {
      map_iter = _map.erase(map_iter);
    } else {
      ++map_iter;
    }
  }
  auto& nulls = _null_values;
  nulls.erase(std::remove_if(nulls.begin(), nulls.end(),
                             [chunks_to_unindex_filtered](RowID& row_id) {
                               return chunks_to_unindex_filtered.end() != std::find(chunks_to_unindex_filtered.begin(),
                                                                                    chunks_to_unindex_filtered.end(),
                                                                                    row_id.chunk_id);
                             }),
              nulls.end());

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
  return std::make_pair(Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(begin)),
                        Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(++end)));
}

template <typename DataType>
std::pair<typename PartialHashIndexImpl<DataType>::IteratorPair, typename PartialHashIndexImpl<DataType>::IteratorPair>
PartialHashIndexImpl<DataType>::range_not_equals(const AllTypeVariant& value) const {
  const auto range_eq = range_equals(value);
  return std::make_pair(std::make_pair(cbegin(), range_eq.first), std::make_pair(range_eq.second, cend()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cbegin() const {
  return Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(_map.cbegin()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cend() const {
  return Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(_map.cend()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cbegin() const {
  return Iterator(std::make_shared<TableIndexVectorIterator>(_null_values.cbegin()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cend() const {
  return Iterator(std::make_shared<TableIndexVectorIterator>(_null_values.cend()));
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::memory_consumption() const {
  size_t bytes{0u};
  bytes += (sizeof(std::set<ChunkID>) + sizeof(ChunkID) * _indexed_chunk_ids.size());

  // TODO(anyone): Find a clever way to estimate the hash sizes in the maps. For now we estimate a hash size of 8 byte
  bytes += sizeof(_map);
  bytes += (8 /* hash size */ + sizeof(std::vector<RowID>)) * _map.size();
  bytes += sizeof(RowID) * std::distance(cbegin(), cend());

  bytes += sizeof(_null_values);
  bytes += sizeof(RowID) * _null_values.size();

  return bytes;
}

template <typename DataType>
std::set<ChunkID> PartialHashIndexImpl<DataType>::get_indexed_chunk_ids() const {
  return _indexed_chunk_ids;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PartialHashIndexImpl);

}  // namespace opossum
