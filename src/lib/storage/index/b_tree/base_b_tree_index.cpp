#include "base_b_tree_index.hpp"

#include "storage/index/column_index_type.hpp"

namespace opossum {

  BaseBTreeIndex::BaseBTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns)
      : BaseIndex{get_index_type_of<BTreeIndex>()}, _index_column(index_columns[0]) {
    DebugAssert((index_columns.size() == 1), "BTreeIndex only works with a single column.");

  std::vector<std::shared_ptr<const BaseColumn>> BaseBTreeIndex::_get_index_columns() const { return {_index_column}; }

} // namespace opossum
