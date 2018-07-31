#include "b_tree_index_impl.hpp"

#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename DataType>
BTreeIndexImpl<DataType>::BTreeIndexImpl(std::shared_ptr<const BaseColumn> index_column) {
  _bulk_insert(index_column);
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::lower_bound(const std::vector<AllTypeVariant>& values) const {
  return lower_bound(type_cast<DataType>(values[0]));
}

template <typename DataType>
BaseBTreeIndexImpl::Iterator BTreeIndexImpl<DataType>::upper_bound(const std::vector<AllTypeVariant>& values) const {
  return upper_bound(type_cast<DataType>(values[0]));
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
uint64_t BTreeIndexImpl<DataType>::memory_consumption() const {
  return sizeof(std::vector<ChunkOffset>) + sizeof(ChunkOffset) * _chunk_offsets.size() + _btree.bytes_used();
}

template <typename DataType>
void BTreeIndexImpl<DataType>::_bulk_insert(const std::shared_ptr<const BaseColumn> column) {
  std::vector<std::pair<ChunkOffset, DataType>> values;

  // Materialize
  resolve_column_type<DataType>(*column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<DataType>(typed_column);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      values.push_back(std::make_pair(value.chunk_offset(), value.value()));
    });
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
  for (size_t i = 0; i < values.size(); i++) {
    if (values[i].second != current_value) {
      current_value = values[i].second;
      _btree[current_value] = i;
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(BTreeIndexImpl);

}  // namespace opossum
