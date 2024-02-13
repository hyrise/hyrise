#include "partial_hash_index_impl.hpp"

#include <tsl/sparse_set.h>

#include <cstddef>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/index/partial_hash/flat_map_iterator.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"

namespace hyrise {

template <typename DataType>
PartialHashIndexImpl<DataType>::PartialHashIndexImpl(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID column_id)
    : BasePartialHashIndexImpl() {
  insert(chunks_to_index, column_id);
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::insert(
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
      // If value is NULL, add to NULL vector, otherwise add into value map.
      if (position.is_null()) {
        _null_positions[DataType{}].emplace_back(chunk.first, position.chunk_offset());
      } else {
        _positions[position.value()].emplace_back(chunk.first, position.chunk_offset());
      }
    });
  }

  return indexed_chunks;
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::remove(const std::vector<ChunkID>& chunks) {
  const size_t size_before = _indexed_chunk_ids.size();

  auto indexed_chunks = std::unordered_set<ChunkID>{};
  for (const auto& chunk_id : chunks) {
    if (!_indexed_chunk_ids.contains(chunk_id)) {
      continue;
    }

    indexed_chunks.insert(chunk_id);
    _indexed_chunk_ids.erase(chunk_id);
  }

  // Checks whether a given RowID's ChunkID is in the set of ChunkIDs to be unindexed.
  auto is_to_remove = [&indexed_chunks](const RowID& row_id) { return indexed_chunks.contains(row_id.chunk_id); };

  // Iterate over all values stored in the index.
  auto map_iter = _positions.begin();
  while (map_iter != _positions.end()) {
    auto& row_ids = _positions.at(map_iter->first);

    // Remove every RowID entry of the value that references one of the chunks.
    row_ids.erase(std::remove_if(row_ids.begin(), row_ids.end(), is_to_remove), row_ids.end());

    map_iter = row_ids.empty() ? _positions.erase(map_iter) : ++map_iter;
  }

  if (_null_positions.contains(DataType{})) {
    auto& nulls = _null_positions[DataType{}];
    nulls.erase(std::remove_if(nulls.begin(), nulls.end(), is_to_remove), nulls.end());
  }

  return size_before - _indexed_chunk_ids.size();
}

template <typename DataType>
bool PartialHashIndexImpl<DataType>::indexed_null_values() const {
  return null_cbegin() != null_cend();
}

template <typename DataType>
BasePartialHashIndexImpl::IteratorRange PartialHashIndexImpl<DataType>::range_equals(
    const AllTypeVariant& value) const {
  const auto& begin = _positions.find(boost::get<DataType>(value));
  if (begin == _positions.end()) {
    const auto& end_iter = cend();
    return std::make_pair(end_iter, end_iter);
  }
  auto end = begin;
  return std::make_pair(CreateFlatMapIterator<DataType>::from_map_iterator(begin),
                        CreateFlatMapIterator<DataType>::from_map_iterator(++end));
}

template <typename DataType>
BasePartialHashIndexImpl::IteratorRangePair PartialHashIndexImpl<DataType>::range_not_equals(
    const AllTypeVariant& value) const {
  const auto& range_eq = range_equals(value);
  return std::make_pair(std::make_pair(cbegin(), range_eq.first), std::make_pair(range_eq.second, cend()));
}

template <typename DataType>
BasePartialHashIndexImpl::Iterator PartialHashIndexImpl<DataType>::cbegin() const {
  return CreateFlatMapIterator<DataType>::from_map_iterator(_positions.cbegin());
}

template <typename DataType>
BasePartialHashIndexImpl::Iterator PartialHashIndexImpl<DataType>::cend() const {
  return CreateFlatMapIterator<DataType>::from_map_iterator(_positions.cend());
}

template <typename DataType>
BasePartialHashIndexImpl::Iterator PartialHashIndexImpl<DataType>::null_cbegin() const {
  return CreateFlatMapIterator<DataType>::from_map_iterator(_null_positions.cbegin());
}

template <typename DataType>
BasePartialHashIndexImpl::Iterator PartialHashIndexImpl<DataType>::null_cend() const {
  return CreateFlatMapIterator<DataType>::from_map_iterator(_null_positions.cend());
}

template <typename DataType>
size_t PartialHashIndexImpl<DataType>::estimate_memory_usage() const {
  auto bytes = size_t{0u};
  bytes += sizeof(_indexed_chunk_ids);

  // It is not trivial to estimate the memory consumption of a hash set. We use a tsl::sparse_set, so that the result
  // of size() estimates the used capacity sufficiently. Sparse sets/maps usually require only a few bits of overhead
  // per entry. Further details about sparse sets/maps: https://smerity.com/articles/2015/google_sparsehash.html
  bytes += sizeof(ChunkID) * _indexed_chunk_ids.size();

  bytes += sizeof(_positions);
  bytes += sizeof(_null_positions);

  // Size of the keys.
  bytes += sizeof(std::pair<DataType, std::vector<RowID>>) * _positions.size();

  bytes += sizeof(std::vector<RowID>) * _positions.size();

  // Capacity for indexed RowIDs.
  for (const auto& [_, row_ids] : _positions) {
    bytes += sizeof(RowID) * row_ids.capacity();
  }

  if (_null_positions.contains(DataType{})) {
    bytes += sizeof(std::vector<RowID>);
    bytes += sizeof(std::pair<DataType, std::vector<RowID>>);

    for (const auto& [_, row_ids] : _null_positions) {
      bytes += sizeof(RowID) * row_ids.capacity();
    }
  }

  return bytes;
}

template <typename DataType>
tsl::sparse_set<ChunkID> PartialHashIndexImpl<DataType>::get_indexed_chunk_ids() const {
  return _indexed_chunk_ids;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PartialHashIndexImpl);

}  // namespace hyrise
