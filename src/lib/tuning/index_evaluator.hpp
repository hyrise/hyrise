#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>

#include "tuning/base_index_evaluator.hpp"
#include "tuning/index_evaluation.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * ToDo(group01): describe specific mechanism to determine desirability and memory_cost
 */
class IndexEvaluator : public BaseIndexEvaluator {
 public:
  IndexEvaluator();

 protected:
  void _setup() final;
  void _process_access_record(const AccessRecord& record) final;
  ColumnIndexType _propose_index_type(const IndexEvaluation& index_evaluation) const final;
  float _predict_memory_cost(const IndexEvaluation& index_evaluation) const final;
  float _calculate_desirability(const IndexEvaluation& index_evaluation) const final;

  std::map<ColumnRef, float> _saved_work;
};

}  // namespace opossum
