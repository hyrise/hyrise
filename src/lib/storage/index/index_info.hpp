#pragma once

#include <string>

#include "column_index_type.hpp"
#include "types.hpp"

namespace opossum {

struct IndexInfo {
  std::vector<CxlumnID> cxlumn_ids;
  std::string name;
  ColumnIndexType type;
};

}  // namespace opossum
