#include "b_tree_index.hpp"

#include "storage/base_column.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "storage/create_iterable_from_column.hpp"

namespace opossum {

template <typename DataType>
BTreeIndex<DataType>::BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns)
    : BaseBTreeIndex{index_columns} {
  _bulk_insert(_index_column);
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::_lower_bound(AllTypeVariant value) const {
  return lower_bound(type_cast<DataType>(value));
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::_upper_bound(AllTypeVariant value) const {
  return upper_bound(type_cast<DataType>(value));
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::lower_bound(DataType value) const {
  auto result = _btree.lower_bound(value);
  if (result == _btree.end()) {
    return _chunk_offsets.end();
  } else {
    return _chunk_offsets.begin() + result->second;
  }
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::upper_bound(DataType value) const {
  auto result = _btree.upper_bound(value);
  if (result == _btree.end()) {
    return _chunk_offsets.end();
  } else {
    return _chunk_offsets.begin() + result->second;
  }
}

template <typename DataType>
uint64_t BTreeIndex<DataType>::memory_consumption() const {
  return sizeof(std::vector<ChunkOffset>) +
         sizeof(ChunkOffset) * _chunk_offsets.size() +
         _btree.bytes_used();
}

template <typename DataType>
void BTreeIndex<DataType>::_bulk_insert(const std::shared_ptr<const BaseColumn)> column) {
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
  std::sort(values.begin(), values.end(), [](const auto& a, const auto& b){ return a.second < b.second; });
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

EXPLICITLY_INSTANTIATE_DATA_TYPES(BTreeIndex);

} // namespace opossum
