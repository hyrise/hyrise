#pragma once

#include "types.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class Table;

class BaseBTreeIndex : private Noncopyable {
 public:
  using Iterator = std::vector<RowID>::const_iterator;

  BaseBTreeIndex() = delete;
  explicit BaseBTreeIndex(const Table& table, const ColumnID column_id);
  BaseBTreeIndex(BaseBTreeIndex&&) = default;
  BaseBTreeIndex& operator=(BaseBTreeIndex&&) = default;
  virtual ~BaseBTreeIndex() = default;

  virtual Iterator lower_bound_all_type(AllTypeVariant value) const = 0;
  virtual Iterator upper_bound_all_type(AllTypeVariant value) const = 0;
  virtual uint64_t memory_consumption() const = 0;

 protected:
  const Table& _table;
  const ColumnID _column_id;
};

} // namespace opossum
