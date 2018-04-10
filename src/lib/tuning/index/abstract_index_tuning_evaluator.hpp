#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>

#include "all_type_variant.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "tuning/abstract_tuning_evaluator.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/index/index_tuning_choice.hpp"

namespace opossum {

/**
 * The AbstractIndexTuningEvaluator is a base class with helper functions for various index evaluators that differ
 * in the concrete algorithms used to determine index desirability and memory cost.
 *
 * It encapsulates the common behaviour of analyzing the systems query cache for
 * operations that might benefit from an index on a specific column
 * and of searching for already existing indices.
 */
class AbstractIndexTuningEvaluator : public AbstractTuningEvaluator {
  friend class IndexTuningEvaluatorTest;

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
  AbstractIndexTuningEvaluator();

  void evaluate(std::vector<std::shared_ptr<TuningChoice>>& choices) final;

 protected:
  /**
   * This method is called at the very beginning of the evaluation process.
   * It may be used to setup data structures of the concrete algorithms.
   * The default implementation does nothing.
   */
  virtual void _setup();
  /**
   * This method is called for every access record that is aggregated into the new index set.
   * It may be used to aggregate information concerning individual uses of the candidate index.
   * The default implementation does nothing.
   */
  virtual void _process_access_record(const AccessRecord& record);
  /**
   * This method is called for every non-existing index to determine the best
   * index type to create.
   */
  virtual ColumnIndexType _propose_index_type(const IndexTuningChoice& index_choice) const = 0;
  /**
   * This method is called on an existing index to determine its memory cost in bytes
   * The existing implementation simply accumulates the individual index costs
   * as reported by the specific index object over all chunks of a column.
   */
  virtual uintptr_t _existing_memory_cost(const IndexTuningChoice& index_choice) const;
  /**
   * This method is called for every non-existing index to predict its memory cost.
   */
  virtual uintptr_t _predict_memory_cost(const IndexTuningChoice& index_choice) const = 0;
  /**
   * This method is called for every index to calculate its final desirability metric.
   */
  virtual float _get_saved_work(const IndexTuningChoice& index_choice) const = 0;

 protected:
  /**
   * This method iterates over all queries in the cache, gets their logical
   * query plans and generates AccessRecord objects by
   * calling _inspect_lqp_node().
   */
  std::vector<AccessRecord> _inspect_query_cache_and_generate_access_records();
  /**
   * This method traverses a logical query plan and generates AccessRecord objects
   * whenever it encounters an operation that could benefit from an index.
   */
  void _inspect_lqp_node(const std::shared_ptr<const AbstractLQPNode>& op, size_t query_frequency,
                         std::vector<AccessRecord>& access_records);
  /**
   * This method takes AccessRecords and creates a set of indexes for the referred columns.
   * It will call _process_access_record() of the derived class in order to let
   * the concrete implementation calculate e.g. usage and desirability metrics.
   */
  std::set<ColumnRef> _aggregate_access_records(const std::vector<AccessRecord>& access_records);
  /**
   * This method adds IndexTuningChoices (marked as already present) for every index that already exists.
   * It will delete entries from the passed new_indexes set that represent indexes that are already created.
   */
  void _add_choices_for_existing_indexes(std::vector<IndexTuningChoice>& choices, std::set<ColumnRef>& new_indexes);
  /**
   * This method adds IndexTuningChoices for every index that was proposed.
   */
  void _add_choices_for_new_indexes(std::vector<IndexTuningChoice>& choices, const std::set<ColumnRef>& new_indexes);

  std::vector<AccessRecord> _access_records;
  std::set<ColumnRef> _new_indexes;

  std::vector<IndexTuningChoice> _choices;
};

}  // namespace opossum
