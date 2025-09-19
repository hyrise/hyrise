#pragma once

#include <algorithm>

#include "storage/pos_lists/row_id_pos_list.hpp"

namespace hyrise {

/**
 * Templated interface for sorting RowIDs.
 * The comparator type is provided as a template parameter.
 */
template <typename Compare>
class AbstractRowIDSorter {
 public:
  virtual ~AbstractRowIDSorter() = default;

  // Sort the given RowIDs in-place using the comparator
  virtual void sort(RowIDPosList& rows, Compare comp) = 0;
};

}  // namespace hyrise
