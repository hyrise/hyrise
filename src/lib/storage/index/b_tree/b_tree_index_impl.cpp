#include "b_tree_index_impl.hpp"

#include "storage/index/abstract_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

template <typename DataType>
BTreeIndexImpl<DataType>::BTreeIndexImpl(const std::shared_ptr<const BaseSegment>& segments_to_index,
                                         std::vector<ChunkOffset>& null_positions)
    : _heap_bytes_used{0} {
  _bulk_insert(segments_to_index, null_positions);
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::lower_bound(const std::vector<AllTypeVariant>& values) const {
  return lower_bound(boost::get<DataType>(values[0]));
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::upper_bound(const std::vector<AllTypeVariant>& values) const {
  return upper_bound(boost::get<DataType>(values[0]));
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::cbegin() const {
  return _chunk_offsets.begin();
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::cend() const {
  return _chunk_offsets.end();
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::lower_bound(DataType value) const {
  auto result = _btree.lower_bound(value);
  if (result == _btree.end()) {
    return _chunk_offsets.end();
  } else {
    return _chunk_offsets.begin() + result->second;
  }
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::upper_bound(DataType value) const {
  auto result = _btree.upper_bound(value);
  if (result == _btree.end()) {
    return _chunk_offsets.end();
  } else {
    return _chunk_offsets.begin() + result->second;
  }
}

template <typename DataType>
size_t BTreeIndexImpl<DataType>::memory_consumption() const {
  return sizeof(std::vector<ChunkOffset>) + sizeof(ChunkOffset) * _chunk_offsets.capacity() + _btree.bytes_used() +
         _heap_bytes_used;
}

template <typename DataType>
void BTreeIndexImpl<DataType>::_bulk_insert(const std::shared_ptr<const BaseSegment>& segment,
                                            std::vector<ChunkOffset>& null_positions) {
  std::vector<std::pair<ChunkOffset, DataType>> values;
  null_positions.reserve(values.size());

  // Materialize
  segment_iterate<DataType>(*segment, [&](const auto& position) {
    if (position.is_null()) {
      null_positions.emplace_back(position.chunk_offset());
    } else {
      values.push_back({position.chunk_offset(), position.value()});
    }
  });

  null_positions.shrink_to_fit();

  if (values.empty()) {
    return;
  }

  // Sort
  std::sort(values.begin(), values.end(), [](const auto& a, const auto& b) { return a.second < b.second; });
  _chunk_offsets.resize(values.size());
  for (size_t i = 0; i < values.size(); i++) {
    _chunk_offsets[i] = values[i].first;
  }

  // Build index
  DataType current_value = values[0].second;
  _btree[current_value] = 0;
  _add_to_heap_memory_usage(current_value);
  for (size_t i = 0; i < values.size(); i++) {
    if (values[i].second != current_value) {
      current_value = values[i].second;
      _btree[current_value] = i;
      _add_to_heap_memory_usage(current_value);
    }
  }
}

template <typename DataType>
void BTreeIndexImpl<DataType>::_add_to_heap_memory_usage(const DataType&) {
  // Except for pmr_string (see below), no supported data type uses heap allocations
}

template <>
void BTreeIndexImpl<pmr_string>::_add_to_heap_memory_usage(const pmr_string& value) {
  // Track only strings that are longer than the reserved stack space for short string optimization (SSO)
  _heap_bytes_used += string_heap_size(value);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(BTreeIndexImpl);

}  // namespace opossum
