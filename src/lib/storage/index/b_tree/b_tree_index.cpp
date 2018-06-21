#include "b_tree_index.hpp"

#include "storage/index/column_index_type.hpp"

namespace opossum {

  BTreeIndex::BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns)
      : BaseIndex{get_index_type_of<BTreeIndex>()}, _index_column(index_columns[0]) {
    DebugAssert((index_columns.size() == 1), "BTreeIndex only works with a single column.");
  }

  BTreeIndex::Iterator BTreeIndex::_lower_bound(const std::vector<AllTypeVariant>&) const {
  }

  BTreeIndex::Iterator BTreeIndex::_upper_bound(const std::vector<AllTypeVariant>&) const {
  }

  BTreeIndex::Iterator BTreeIndex::_cbegin() const {
  }

  BTreeIndex::Iterator BTreeIndex::_cend() const {
  }

  std::vector<std::shared_ptr<const BaseColumn>> BTreeIndex::_get_index_columns() const { return {_index_column}; }

} // namespace opossum
