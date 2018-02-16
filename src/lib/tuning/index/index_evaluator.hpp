#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "tuning/index/base_index_evaluator.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/index/index_choice.hpp"

namespace opossum {

/**
 * ToDo(group01): describe specific mechanism to determine desirability and memory_cost
 */
class IndexEvaluator : public BaseIndexEvaluator {
  friend class IndexEvaluatorTest;

 public:
  explicit IndexEvaluator();

 protected:
  void _setup() final;
  void _process_access_record(const AccessRecord& record) final;
  ColumnIndexType _propose_index_type(const IndexChoice& index_evaluation) const final;
  float _predict_memory_cost(const IndexChoice& index_evaluation) const final;
  float _calculate_saved_work(const IndexChoice& index_evaluation) const final;

  std::map<ColumnRef, float> _saved_work;
};

}  // namespace opossum
