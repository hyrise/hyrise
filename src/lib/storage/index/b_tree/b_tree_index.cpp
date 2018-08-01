#include "b_tree_index.hpp"

#include "resolve_type.hpp"
#include "storage/index/column_index_type.hpp"

namespace opossum {

BTreeIndex::BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns)
    : BaseIndex{get_index_type_of<BTreeIndex>()}, _index_column(index_columns[0]) {
  Assert((index_columns.size() == 1), "BTreeIndex only works with a single column.");
  _impl = make_shared_by_data_type<BaseBTreeIndexImpl, BTreeIndexImpl>(_index_column->data_type(), _index_column);
}

uint64_t BTreeIndex::memory_consumption() const { return _impl->memory_consumption(); }

BTreeIndex::Iterator BTreeIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  return _impl->lower_bound(values);
}

BTreeIndex::Iterator BTreeIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  return _impl->upper_bound(values);
}

BTreeIndex::Iterator BTreeIndex::_cbegin() const { return _impl->cbegin(); }

BTreeIndex::Iterator BTreeIndex::_cend() const { return _impl->cend(); }

std::vector<std::shared_ptr<const BaseColumn>> BTreeIndex::_get_index_columns() const { return {_index_column}; }

}  // namespace opossum
