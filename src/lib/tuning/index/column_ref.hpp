#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

/**
 * A ColumnRef is used to reference a set of columns by a table name
 * and their column indices in this table
 */
struct ColumnRef {
  ColumnRef(std::string table_name, ColumnID column_id);
  ColumnRef(std::string table_name, const std::vector<ColumnID>& column_ids);

  std::string table_name;
  std::vector<ColumnID> column_ids;

  bool operator<(const ColumnRef& other) const;

  bool operator>(const ColumnRef& other) const;

  bool operator==(const ColumnRef& other) const;

  friend std::ostream& operator<<(std::ostream& output, const ColumnRef& column_ref);
};

}  // namespace opossum
