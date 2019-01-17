#pragma once

#include "segment_index_type.hpp"
#include "types.hpp"

namespace opossum {

struct IndexInfo {
  std::vector<ColumnID> column_ids;
  std::string name;
  SegmentIndexType type;
};

}  // namespace opossum
