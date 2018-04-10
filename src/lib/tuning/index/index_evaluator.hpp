#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "tuning/index/abstract_index_evaluator.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/index/index_choice.hpp"

namespace opossum {

/**
 * This is the default implementation of an AbstractIndexEvaluator.
 *
 * It only considers single-column indexes for now.
 *
 * Desirability is determined by the amount of "saved work" in terms on
 * "unscanned rows", i.e. the number of rows that don't have to be read when
 * using an index on a given column compared to a full column/table scan.
 * This assumes that columns are always scanned completely, meaning that
 * it does not take previous operators into account.
 *
 * Cost is determined by the memory footprint of a specific index. It is either
 * read directly from an existing index or estimated for a non-existing index.
 */
class IndexEvaluator : public AbstractIndexEvaluator {
  friend class IndexEvaluatorTest;

 public:
  IndexEvaluator();

 protected:
  void _setup() final;
  void _process_access_record(const AccessRecord& record) final;
  ColumnIndexType _propose_index_type(const IndexChoice& index_evaluation) const final;
  uintptr_t _predict_memory_cost(const IndexChoice& index_evaluation) const final;
  float _get_saved_work(const IndexChoice& index_evaluation) const final;

  std::map<ColumnRef, float> _saved_work;
};

}  // namespace opossum
