#pragma once

#include <algorithm>

#include "storage/pos_lists/row_id_pos_list.hpp"

namespace hyrise {

/**
 * Templated interface for sorting RowIDs.
 * The comparator type is provided as a template parameter.
 * 
 */

struct AggEntry {
  RowID row_id;               // used for key comparison
  std::vector<double> sums;   // aggregate values (can support multiple non-key columns)
};
template <typename Compare>
class AbstractRowIDSorter {
 public:
  virtual ~AbstractRowIDSorter() = default;

  // Sort the given RowIDs in-place using the comparator
  virtual void sort(RowIDPosList& rows, Compare comp) = 0;

  virtual void sort_with_agg(std::vector<AggEntry>& entries, Compare comp) = 0;
};

}  // namespace hyrise