#pragma once

#include <memory>

#include "operators/abstract_operator.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

class IndexProposal {
 public:
  std::string table_name;
  ColumnID column_id;
};

/**
 * The IndexSelectionHeuristic takes information about the current system
 * (e.g. query plan cache, table statistics) and proposes indices to be created
 * or removed.
 */
class IndexSelectionHeuristic {
 public:
  IndexSelectionHeuristic();

  // Runs the heuristic to analyze the SystemStatistics object and return
  // recommended changes to be made to the live system.
  // TODO(group01) determine return value of this operation
  const std::vector<IndexProposal>& recommend_changes(const SystemStatistics& statistics);

 protected:
  void _inspect_operator(const std::shared_ptr<const AbstractOperator>& op);

  std::vector<IndexProposal> _index_proposals;
};

}  // namespace opossum
