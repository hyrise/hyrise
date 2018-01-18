#include <algorithm>
#include <iostream>

#include "index_selector.hpp"

namespace opossum {

std::vector<IndexOperation> IndexSelector::select_indices(std::vector<IndexEvaluation> proposals, float memory_budget) {
  std::sort(proposals.begin(), proposals.end());
  std::reverse(proposals.begin(), proposals.end());

  _operations.clear();
  _operations.reserve(proposals.size());
  for (const auto& proposal : proposals) {
    if (proposal.desirablility < 0) {
      _operations.emplace_back(proposal.table_name, proposal.column_id, false);
    } else {
      _operations.emplace_back(proposal.table_name, proposal.column_id, true);
    }
  }
  return _operations;
}

}  // namespace opossum
