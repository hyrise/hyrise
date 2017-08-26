#pragma once

#include "types.hpp"

namespace opossum {

/**
 * @brief The hot loop of the table scan
 */
class TableScanMainLoop {
 public:
  TableScanMainLoop(const ChunkID chunk_id, PosList &matches_out) : _chunk_id{chunk_id}, _matches_out{matches_out} {}

  template <typename LeftIterator, typename RightIterator, typename Comparator>
  void operator()(const Comparator &comparator, LeftIterator left_it, LeftIterator left_end, RightIterator right_it) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (comparator(left.value(), right.value())) {
        _matches_out.push_back(RowID{_chunk_id, left.chunk_offset()});
      }
    }
  }

 private:
  const ChunkID _chunk_id;
  PosList &_matches_out;
};

}  // namespace opossum
