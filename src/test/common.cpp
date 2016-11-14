#include "common.hpp"

#include <algorithm>
#include <memory>
#include <vector>

#include "../lib/storage/table.hpp"
#include "../lib/types.hpp"


bool compareTables(const opossum::Table &tleft, const opossum::Table &tright, bool sorted) {
  if (tleft.col_count() != tright.col_count()) {
    return false;
  }
  for (size_t col_id = 0; col_id < tright.col_count(); ++col_id) {
    if (tleft.column_type(col_id) != tright.column_type(col_id) ||
        tleft.column_name(col_id) != tright.column_name(col_id)) {
      return false;
    }
  }

  if (tleft.row_count() != tright.row_count()) {
    return false;
  }
  // initialize tables with sizes
  std::vector<std::vector<opossum::AllTypeVariant>> left;
  std::vector<std::vector<opossum::AllTypeVariant>> right;

  left.resize(tleft.row_count(), std::vector<opossum::AllTypeVariant>(tleft.col_count()));
  right.resize(tright.row_count(), std::vector<opossum::AllTypeVariant>(tright.col_count()));

  // initialize table values
  for (size_t chunk_id = 0; chunk_id < tleft.chunk_count(); chunk_id++) {
    opossum::Chunk &chunk = tleft.get_chunk(chunk_id);

    for (size_t col_id = 0; col_id < tleft.col_count(); ++col_id) {
      std::shared_ptr<opossum::BaseColumn> column = chunk.get_column(col_id);

      for (size_t row = 0; row < chunk.size(); ++row) {
        left[row][col_id] = (*column)[row];
      }
    }
  }

  for (size_t chunk_id = 0; chunk_id < tright.chunk_count(); chunk_id++) {
    opossum::Chunk &chunk = tright.get_chunk(chunk_id);

    for (size_t col_id = 0; col_id < tright.col_count(); ++col_id) {
      std::shared_ptr<opossum::BaseColumn> column = chunk.get_column(col_id);

      for (size_t row = 0; row < chunk.size(); ++row) {
        right[row][col_id] = (*column)[row];
      }
    }
  }

  // sort if order matters
  if (!sorted) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
  }

  if (left == right) {
    return true;
  } else {
    return false;
  }
}
