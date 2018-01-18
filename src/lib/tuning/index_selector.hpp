#pragma once

#include <string>

#include "tuning/index_evaluator.hpp"
#include "types.hpp"

namespace opossum {

class IndexOperation {
 public:
  IndexOperation(const std::string& table_name, ColumnID column_id, bool create)
      : table_name{table_name}, column_id{column_id}, create{create} {}

  std::string table_name;
  ColumnID column_id;

  /**
   * What operation should be performed
   * true: create index, false: remove index
   */
  bool create;
};

/**
 */
class IndexSelector {
 public:
  /**
   * Determine Index creations and removals from list of proposals.
   *
   * The operation list is prioritized by expected performance impact on the system,
   * so that the most beneficial operations (and their dependencies) come before less
   * useful operations.
   */
  const std::vector<IndexOperation> select_indices(std::vector<IndexProposal> proposals, float memory_budget);

 protected:
  std::vector<IndexOperation> _operations;
};

}  // namespace opossum
