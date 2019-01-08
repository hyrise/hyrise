#include "b_tree_index_impl.hpp"

#include "storage/index/base_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename DataType>
BTreeIndexImpl<DataType>::BTreeIndexImpl(const std::shared_ptr<const BaseSegment>& segments_to_index)
    : _heap_bytes_used{0} {
  _bulk_insert(segments_to_index);
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::lower_bound(const std::vector<AllTypeVariant>& values) const {
  return lower_bound(type_cast_variant<DataType>(values[0]));
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::upper_bound(const std::vector<AllTypeVariant>& values) const {
  return upper_bound(type_cast_variant<DataType>(values[0]));
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
  return sizeof(std::vector<ChunkOffset>) + sizeof(ChunkOffset) * _chunk_offsets.size() + _btree.bytes_used() +
         _heap_bytes_used;
}

template <typename DataType>
void BTreeIndexImpl<DataType>::_bulk_insert(const std::shared_ptr<const BaseSegment>& segment) {
  std::vector<std::pair<ChunkOffset, DataType>> values;

  // Materialize
  segment_iterate<DataType>(*segment, [&](const auto& position) {
    if (position.is_null()) return;
    values.push_back(std::make_pair(position.chunk_offset(), position.value()));
  });

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
  // Except for std::string (see below), no supported data type uses heap allocations
}

template <>
void BTreeIndexImpl<std::string>::_add_to_heap_memory_usage(const std::string& value) {
  // Track only strings that are longer than the reserved stack space for short string optimization (SSO)
  static const auto short_string_threshold = std::string("").capacity();
  if (value.size() > short_string_threshold) {
    _heap_bytes_used += value.size();
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(BTreeIndexImpl);

}  // namespace opossum
