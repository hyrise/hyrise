#include "partial_hash_index_impl.hpp"
#include "storage/segment_iterate.hpp"

#include <mutex>

namespace hyrise {

template <typename DataType>
TableIndexTbbUnorderedMapIterator<DataType>::TableIndexTbbUnorderedMapIterator(MapIteratorType it)
    : _map_iterator(it), _vector_index(0) {}

template <typename DataType>
const RowID& TableIndexTbbUnorderedMapIterator<DataType>::operator*() const {
  return _map_iterator->second[_vector_index];
}

template <typename DataType>
TableIndexTbbUnorderedMapIterator<DataType>& TableIndexTbbUnorderedMapIterator<DataType>::operator++() {
  if (++_vector_index >= _map_iterator->second.size()) {
    _map_iterator++;
    _vector_index = 0;
  }
  return *this;
}

template <typename DataType>
bool TableIndexTbbUnorderedMapIterator<DataType>::operator==(const BaseTableIndexIterator& other) const {
  auto other_iterator = dynamic_cast<const TableIndexTbbUnorderedMapIterator*>(&other);
  return other_iterator && _map_iterator == other_iterator->_map_iterator &&
         _vector_index == other_iterator->_vector_index;
}

template <typename DataType>
bool TableIndexTbbUnorderedMapIterator<DataType>::operator!=(const BaseTableIndexIterator& other) const {
  auto other_iterator = dynamic_cast<const TableIndexTbbUnorderedMapIterator*>(&other);
  return !other_iterator || _map_iterator != other_iterator->_map_iterator ||
         _vector_index != other_iterator->_vector_index;
}

template <typename DataType>
std::shared_ptr<BaseTableIndexIterator> TableIndexTbbUnorderedMapIterator<DataType>::clone() const {
  return std::make_shared<TableIndexTbbUnorderedMapIterator<DataType>>(*this);
}

TableIndexVectorIterator::TableIndexVectorIterator(MapIteratorType it) : _map_iterator(it) {}

const RowID& TableIndexVectorIterator::operator*() const {
  return *_map_iterator;
}

TableIndexVectorIterator& TableIndexVectorIterator::operator++() {
  _map_iterator++;
  return *this;
}

bool TableIndexVectorIterator::operator==(const BaseTableIndexIterator& other) const {
  const auto* const other_iterator = dynamic_cast<const TableIndexVectorIterator*>(&other);
  return other_iterator && _map_iterator == other_iterator->_map_iterator;
}

bool TableIndexVectorIterator::operator!=(const BaseTableIndexIterator& other) const {
  const auto* const other_iterator = dynamic_cast<const TableIndexVectorIterator*>(&other);
  return !other_iterator || _map_iterator != other_iterator->_map_iterator;
}

std::shared_ptr<BaseTableIndexIterator> TableIndexVectorIterator::clone() const {
  return std::make_shared<TableIndexVectorIterator>(*this);
}

size_t BasePartialHashIndexImpl::insert_entries(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& /*chunk*/, const ColumnID /*column_id*/) {
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

tbb::concurrent_unordered_set<ChunkID> BasePartialHashIndexImpl::get_indexed_chunk_ids() const {
  return tbb::concurrent_unordered_set<ChunkID>{};
}

template <typename DataType>
PartialHashIndexImpl<DataType>::PartialHashIndexImpl(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id)
    : BasePartialHashIndexImpl() {
  insert_entries(chunks_to_index, column_id);
}

std::mutex insert_entries_mutex;

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::insert_entries(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id) {
  const auto previous_chunk_count = _indexed_chunk_ids.size();
  for (const auto& chunk : chunks_to_index) {

    // Prevents multiple threads from indexing the same chunk concurrently.
    {
      std::lock_guard<std::mutex> lock{insert_entries_mutex};
      
      if (_indexed_chunk_ids.contains(chunk.first)) {
      // Index already contains entries for the given chunk.
      continue;
      }

      _indexed_chunk_ids.insert(chunk.first);
    }


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

  return _indexed_chunk_ids.size() - previous_chunk_count;
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
  return std::make_pair(Iterator(std::make_shared<TableIndexTbbUnorderedMapIterator<DataType>>(begin)),
                        Iterator(std::make_shared<TableIndexTbbUnorderedMapIterator<DataType>>(++end)));
}

template <typename DataType>
std::pair<typename PartialHashIndexImpl<DataType>::IteratorPair, typename PartialHashIndexImpl<DataType>::IteratorPair>
PartialHashIndexImpl<DataType>::range_not_equals(const AllTypeVariant& value) const {
  const auto range_eq = range_equals(value);
  return std::make_pair(std::make_pair(cbegin(), range_eq.first), std::make_pair(range_eq.second, cend()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cbegin() const {
  return Iterator(std::make_shared<TableIndexTbbUnorderedMapIterator<DataType>>(_map.cbegin()));
}

template <typename DataType>
typename PartialHashIndexImpl<DataType>::Iterator PartialHashIndexImpl<DataType>::cend() const {
  return Iterator(std::make_shared<TableIndexTbbUnorderedMapIterator<DataType>>(_map.cend()));
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
size_t PartialHashIndexImpl<DataType>::estimate_memory_usage() const {
  auto bytes = size_t{0u};

  bytes += sizeof(_indexed_chunk_ids);  // NOLINT - Linter complains size() should be used, which would be wrong.
  bytes += sizeof(ChunkID) * _indexed_chunk_ids.size();

  bytes += sizeof(_map);
  // TBB's concurrent_unordered_map uses tbb_hasher as hash function, so the hash size equals the size of a size_t.
  bytes += sizeof(size_t) * _map.size();

  bytes += sizeof(tbb::concurrent_vector<RowID>) * _map.size();
  bytes += sizeof(RowID) * std::distance(cbegin(), cend());

  bytes += sizeof(_null_values);  // NOLINT
  bytes += sizeof(RowID) * _null_values.size();

  return bytes;
}

template <typename DataType>
tbb::concurrent_unordered_set<ChunkID> PartialHashIndexImpl<DataType>::get_indexed_chunk_ids() const {
  return _indexed_chunk_ids;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PartialHashIndexImpl);

}  // namespace hyrise
