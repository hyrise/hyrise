#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

/**
 * A IndexableColumnSet is used to reference a set of columns by a table name
 * and their column indexes in this table.
 * This is used by the IndexTuningEvaluator, IndexTuningOptions and IndexTuningOperations.
 */
struct IndexableColumnSet {
  IndexableColumnSet(std::string table_name, ColumnID column_id);
  IndexableColumnSet(std::string table_name, const std::vector<ColumnID>& column_ids);

  std::string table_name;
  std::vector<ColumnID> column_ids;

  bool operator<(const IndexableColumnSet& other) const;

  bool operator==(const IndexableColumnSet& other) const;

  friend std::ostream& operator<<(std::ostream& output, const IndexableColumnSet& indexable_column_set);
};

}  // namespace opossum
