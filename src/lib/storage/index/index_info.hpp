#pragma once

#include <string>

#include "column_index_type.hpp"
#include "types.hpp"

namespace opossum {

struct IndexInfo {
  std::vector<ColumnID> column_ids;
  std::string name;
  ColumnIndexType type;

  bool operator==(const IndexInfo& other) const {
    return other.column_ids == column_ids && other.name == name && other.type == type;
  }

  bool operator!=(const IndexInfo& other) const { return !operator==(other); }
};

}  // namespace opossum
