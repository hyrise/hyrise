#pragma once

#include "segment_index_type.hpp" // NEEDEDINCLUDE
#include "types.hpp" // NEEDEDINCLUDE

namespace opossum {

struct IndexInfo {
  std::vector<ColumnID> column_ids;
  std::string name;
  SegmentIndexType type;
};

}  // namespace opossum
