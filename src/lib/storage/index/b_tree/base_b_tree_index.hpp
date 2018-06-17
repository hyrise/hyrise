#pragma once

#include "types.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class Table;

class BaseBTreeIndex : private BaseIndex {
 public:
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  BaseBTreeIndex() = delete;
  explicit BaseBTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns);

  virtual uint64_t memory_consumption() const = 0;

 protected:
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual std::vector<std::shared_ptr<const BaseColumn>> _get_index_columns() const override;

  std::shared_ptr<const BaseColumn> _index_column;
};

} // namespace opossum
