#pragma once

#include <string>

#include "tuning/abstract_index_selector.hpp"
#include "tuning/index_evaluation.hpp"
#include "tuning/index_operation.hpp"

namespace opossum {

/**
 * ToDo(group01): describe specific mechanism to generate operation sequence
 */
class IndexSelector : public AbstractIndexSelector {
 public:
  std::vector<IndexOperation> select_indices(std::vector<IndexEvaluation> evaluations, float memory_budget) override;

 protected:
  std::vector<IndexOperation> _operations;
};

}  // namespace opossum
