#pragma once

#include "base_b_tree_index.hpp"
#include "types.hpp"

#include <btree_map.h>

namespace opossum {

/**
* Implementation: https://code.google.com/archive/p/cpp-btree/
* Note: does not support null values right now.
*/
template <typename DataType>
class BTreeIndex : public BaseBTreeIndex {
 public:
  BTreeIndex() = delete;
  explicit BTreeIndex(const Table& table, const ColumnID column_id);
  BTreeIndex(BTreeIndex&&) = default;
  BTreeIndex& operator=(BTreeIndex&&) = default;
  virtual ~BTreeIndex() = default;

  virtual Iterator lower_bound_all_type(AllTypeVariant value) const;
  virtual Iterator upper_bound_all_type(AllTypeVariant value) const;
  virtual uint64_t memory_consumption() const;

  Iterator lower_bound(DataType value) const;
  Iterator upper_bound(DataType value) const;

 private:
  void _bulk_insert(const Table& table, const ColumnID column_id);

  btree::btree_map<DataType, size_t> _btree;
  std::vector<RowID> _row_ids;
};

} // namespace opossum
