#pragma once

#include "types.hpp"
#include "all_type_variant.hpp"
#include "storage/index/base_index.hpp"
#include "storage/base_column.hpp"

namespace opossum {

class BTreeIndex : public BaseIndex {
 public:
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  BTreeIndex() = delete;
  explicit BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns);

  virtual uint64_t memory_consumption() const = 0;

 protected:
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant>&) const override;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant>&) const override;
  virtual Iterator _cbegin() const override;
  virtual Iterator _cend() const override;
  virtual std::vector<std::shared_ptr<const BaseColumn>> _get_index_columns() const override;

  std::shared_ptr<const BaseColumn> _index_column;
  std::shared_ptr<BaseIndex> _impl;
};

} // namespace opossum
