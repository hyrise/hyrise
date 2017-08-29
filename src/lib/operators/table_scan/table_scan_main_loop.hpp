#pragma once

#include "types.hpp"

namespace opossum {

/**
 * @brief The hot loop of the table scan
 */
class TableScanMainLoop {
 public:
  template <typename Comparator, typename LeftIterator, typename RightIterator>
  void operator()(const Comparator &comparator, LeftIterator left_it, LeftIterator left_end, RightIterator right_it,
                  const ChunkID chunk_id, PosList &matches_out) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (comparator(left.value(), right.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }
};

}  // namespace opossum
