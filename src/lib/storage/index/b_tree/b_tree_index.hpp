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
  explicit BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns);

  BTreeIndex(const BTreeIndex&) = delete;
  BTreeIndex& operator=(const BTreeIndex&) = delete;

  BTreeIndex(BTreeIndex&&) = default;
  BTreeIndex& operator=(BTreeIndex&&) = default;

  virtual uint64_t memory_consumption() const;

  Iterator lower_bound(DataType value) const;
  Iterator upper_bound(DataType value) const;

protected:
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant>&) const;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant>&) const;
  virtual Iterator _cbegin() const;
  virtual Iterator _cend() const;
  void _bulk_insert(const std::shared_ptr<const BaseColumn>);

  btree::btree_map<DataType, size_t> _btree;
  std::vector<ChunkOffset> _chunk_offsets;
};

} // namespace opossum
