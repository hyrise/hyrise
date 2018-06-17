#include "b_tree_index.hpp"

#include "storage/base_column.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "storage/create_iterable_from_column.hpp"

namespace opossum {

template <typename DataType>
BTreeIndex<DataType>::BTreeIndex(const Table& table, const ColumnID column_id) : BaseBTreeIndex{table, column_id} {
  _bulk_insert(table, column_id);
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::lower_bound_all_type(AllTypeVariant value) const {
  return lower_bound(type_cast<DataType>(value));
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::upper_bound_all_type(AllTypeVariant value) const {
  return upper_bound(type_cast<DataType>(value));
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::lower_bound(DataType value) const {
  auto result = _btree.lower_bound(value);
  if (result == _btree.end()) {
    return _row_ids.end();
  } else {
    return _row_ids.begin() + result->second;
  }
}

template <typename DataType>
BaseBTreeIndex::Iterator BTreeIndex<DataType>::upper_bound(DataType value) const {
  auto result = _btree.upper_bound(value);
  if (result == _btree.end()) {
    return _row_ids.end();
  } else {
    return _row_ids.begin() + result->second;
  }
}

template <typename DataType>
uint64_t BTreeIndex<DataType>::memory_consumption() const {
  return sizeof(std::vector<RowID>) +
         sizeof(RowID) * _row_ids.size() +
         _btree.bytes_used();
}

template <typename DataType>
void BTreeIndex<DataType>::_bulk_insert(const Table& table, const ColumnID column_id) {
  std::vector<std::pair<RowID, DataType>> values;

  // Materialize
  for (auto chunk_id = ChunkID{0}; chunk_id < _table.chunk_count(); chunk_id++) {
    auto chunk = _table.get_chunk(chunk_id);
    auto column = chunk->get_column(_column_id);
    resolve_column_type<DataType>(*column, [&](const auto& typed_column) {
      auto iterable_left = create_iterable_from_column<DataType>(typed_column);
      iterable_left.for_each([&](const auto& value) {
        if (value.is_null()) return;
        values.push_back(std::make_pair(RowID{chunk_id , value.chunk_offset()}, value.value()));
      });
    });
  }

  // Sort
  std::sort(values.begin(), values.end(), [](const auto& a, const auto& b){ return a.second < b.second; });
  _row_ids.resize(values.size());
  for (size_t i = 0; i < values.size(); i++) {
    _row_ids[i] = values[i].first;
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
