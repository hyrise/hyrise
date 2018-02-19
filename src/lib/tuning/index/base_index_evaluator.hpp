#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>

#include "all_type_variant.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "tuning/abstract_evaluator.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/index/index_choice.hpp"

namespace opossum {

/**
 * The BaseIndexEvaluator is a base class with helper functions for various index evaluators that differ
 * in the concrete algorithms used to determine index desirability and memory cost.
 *
 * It encapsulates the common behaviour of analyzing the systems query cache for
 * operations that might benefit from an index on a specific column
 * and of searching for already existing indices.
 */
class BaseIndexEvaluator : public AbstractEvaluator {
  friend class IndexEvaluatorTest;

 protected:
  /**
   * Data class representing a node in the query plan where an index could be used.
   * Contains:
   *  * The column the index would be useful on
   *  * The frequency of the respective query's occurrence in the query cache
   * Currently only predicate nodes are considered, so the respective condition
   * and compare value are saved directly.
   */
  struct AccessRecord {
    AccessRecord(const std::string& table_name, ColumnID column_id, size_t number_of_usages)
        : column_ref{table_name, column_id}, query_frequency{number_of_usages} {}
    ColumnRef column_ref;
    size_t query_frequency;

    PredicateCondition condition;
    AllTypeVariant compare_value;
  };

 public:
  BaseIndexEvaluator();

  void evaluate(std::vector<std::shared_ptr<TuningChoice>>& choices) final;

 protected:
  /**
   * This method is called at the very beginning of the evaluation process.
   *
   * It may be used to setup data structures of the concrete algorithms.
   *
   * The default implementation does nothing.
   */
  virtual void _setup();
  /**
   * This method is called for every access record that is aggregated into the new index set.
   *
   * It may be used to aggregate information concerning individual uses of the candidate index.
   *
   * The default implementation does nothing.
   */
  virtual void _process_access_record(const AccessRecord& record);
  /**
   * This method is called for every non-existing index to determine the best
   * index type to create.
   */
  virtual ColumnIndexType _propose_index_type(const IndexChoice& index_choice) const = 0;
  /**
   * This method is called on an existing index to determine its memory cost in MiB
   *
   * The existing implementation simply accumulates the individual index costs
   * as reported by the specific index object over all chunks of a column.
   */
  virtual float _existing_memory_cost(const IndexChoice& index_choice) const;
  /**
   * This method is called for every non-existing index to predict its memory cost.
   */
  virtual float _predict_memory_cost(const IndexChoice& index_choice) const = 0;
  /**
   * This method is called for every index to calculate its final desirability metric.
   */
  virtual float _calculate_saved_work(const IndexChoice& index_choice) const = 0;

 protected:
  void _inspect_query_cache();
  void _inspect_pqp_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency);
  void _inspect_lqp_operator(const std::shared_ptr<const AbstractLQPNode>& op, size_t query_frequency);
  void _aggregate_access_records();
  void _add_existing_indices();
  void _add_new_indices();

  std::vector<AccessRecord> _access_records;
  std::set<ColumnRef> _new_indices;

  std::vector<IndexChoice> _choices;
};

}  // namespace opossum
