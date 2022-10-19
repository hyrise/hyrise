#include "partial_hash_index_impl.hpp"
#include "storage/segment_iterate.hpp"

namespace hyrise {

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
  const auto* const obj = dynamic_cast<const TableIndexVectorIterator*>(&other);
  return obj && _map_iterator == obj->_map_iterator;
}

bool TableIndexVectorIterator::operator!=(const BaseTableIndexIterator& other) const {
  const auto* const obj = dynamic_cast<const TableIndexVectorIterator*>(&other);
  return !obj || _map_iterator != obj->_map_iterator;
}

std::shared_ptr<BaseTableIndexIterator> TableIndexVectorIterator::clone() const {
  return std::make_shared<TableIndexVectorIterator>(*this);
}

size_t BasePartialHashIndexImpl::insert_entries(const std::vector<std::pair<ChunkID,
    std::shared_ptr<Chunk>>>& /*chunk*/, const ColumnID /*column_id*/) {
  return 0;
}

size_t BasePartialHashIndexImpl::remove_entries(const std::vector<ChunkID>& /*chunk_ids*/) {
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

std::unordered_set<ChunkID> BasePartialHashIndexImpl::get_indexed_chunk_ids() const {
  return std::unordered_set<ChunkID>{};
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
  const size_t size_before = _indexed_chunk_ids.size();
  for (const auto& chunk : chunks_to_index) {
    // We do not allow multiple indexing of one chunk.
    if (_indexed_chunk_ids.contains(chunk.first))
      continue;

    _indexed_chunk_ids.insert(chunk.first);
    // Iterate over the segment to index and populate the index.
    const auto& indexed_segment = chunk.second->get_segment(column_id);
    segment_iterate<DataType>(*indexed_segment, [&](const auto& position) {
      const auto row_id = RowID{chunk.first, position.chunk_offset()};
      // If value is NULL, add to NULL vector, otherwise add into value map.
      if (position.is_null()) {
        _null_values.push_back(row_id);
      } else {
        // _map[position.value()].push_back(row_id);
        typename tbb::concurrent_hash_map<DataType, std::vector<RowID>>::accessor hash_map_accessor;
        _map.insert(hash_map_accessor, position.value());
        hash_map_accessor->second.push_back(row_id);
      }
    });
  }

  return _indexed_chunk_ids.size() - size_before;
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::remove_entries(const std::vector<ChunkID>& chunks_to_unindex) {
  const auto size_before = _indexed_chunk_ids.size();

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
    return indexed_chunks_to_unindex.contains(row_id.chunk_id);
  };

  // Iterate over all values stored in the index.
  auto map_iter = _map.begin();
  auto keys_to_delete = std::unordered_set<DataType>{};

  while (map_iter != _map.end()) {
    typename tbb::concurrent_hash_map<DataType, std::vector<RowID>>::accessor hash_map_accessor;
    _map.find(hash_map_accessor, map_iter->first);
    auto& row_ids = hash_map_accessor->second;

    // Remove every RowID entry of the value that references one of the chunks.
    row_ids.erase(std::remove_if(row_ids.begin(), row_ids.end(), is_to_unindex), row_ids.end());

    if (row_ids.empty()) {
      keys_to_delete.insert(hash_map_accessor->first);
    }
    ++map_iter;
  }

  auto& nulls = _null_values;
  nulls.erase(std::remove_if(nulls.begin(), nulls.end(), is_to_unindex), nulls.end());

  for (const auto& key_to_delete : keys_to_delete) {
    _map.erase(key_to_delete);
  }


  return size_before - _indexed_chunk_ids.size();
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::IteratorPair PartialHashIndexImpl<DataType>::range_equals(
    const AllTypeVariant& value) const {
  auto range = _map.equal_range(boost::get<DataType>(value));
  return std::make_pair(Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(range.first)),
                        Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(range.second)));
  // const auto begin = _map.find(boost::get<DataType>(value));
  // if (begin == _map.end()) {
  //   const auto end_iter = cend();
  //   return std::make_pair(end_iter, end_iter);
  // }
  // auto end = begin;
  // return std::make_pair(Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(begin)),
  //                       Iterator(std::make_shared<TableIndexFlattenedSparseMapIterator<DataType>>(++end)));
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
  return Iterator{std::make_shared<TableIndexVectorIterator>(_null_values.cbegin())};
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::null_cend() const {
  return Iterator{std::make_shared<TableIndexVectorIterator>(_null_values.cend())};
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::memory_usage() const {
  auto bytes = size_t{0u};

  bytes += sizeof(_indexed_chunk_ids);  // NOLINT - Linter complains size() should be used, which would be wrong.
  bytes += sizeof(ChunkID) * _indexed_chunk_ids.size();

  bytes += sizeof(_map);
  // Tessil's sparse_map uses std::hash as hash function, so the hash size equals the size of a size_t.
  bytes += sizeof(size_t) * _map.size();

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
