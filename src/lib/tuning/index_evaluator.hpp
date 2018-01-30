#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>

#include "tuning/abstract_index_evaluator.hpp"
#include "tuning/index_evaluation.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * ToDo(group01): describe specific mechanism to determine desirability and memory_cost
 */
class IndexEvaluator : public AbstractIndexEvaluator {
  /**
   * Data class to reference one specific column by the name of its table
   * and its index in that table
   */
  struct ColumnRef {
    ColumnRef(std::string table_name, ColumnID column_id) : table_name{table_name}, column_id{column_id} {}

    std::string table_name;
    ColumnID column_id;

    bool operator<(const ColumnRef& other) const {
      return (table_name == other.table_name) ? column_id < other.column_id : table_name < other.table_name;
    }
    bool operator>(const ColumnRef& other) const {
      return (table_name == other.table_name) ? column_id > other.column_id : table_name > other.table_name;
    }
  };

  /**
   * Data class containing a single operator tree belonging to a query in the query cache
   * together with the frequency of that query.
   */
  struct Operation {
    Operation(std::shared_ptr<AbstractOperator> operator_tree, size_t frequency)
        : operator_tree{operator_tree}, frequency{frequency} {}
    std::shared_ptr<AbstractOperator> operator_tree;
    size_t frequency;
  };

  /**
   * Data class containing information on
   * *what* data is read by a query (table + column),
   * *how often* that data is accessed (query frequency / # of occurrences),
   * *how much* of the data is retrieved (query selectivity).
   */
  struct AccessRecord {
    AccessRecord(std::string table_name, ColumnID column_id, size_t number_of_usages, float selectivity)
        : table_name{table_name}, column_id{column_id}, number_of_usages{number_of_usages}, selectivity{selectivity} {}
    std::string table_name;
    ColumnID column_id;
    size_t number_of_usages;
    float selectivity;
  };

 public:
  IndexEvaluator();

  std::vector<IndexEvaluation> evaluate_indices(const SystemStatistics& statistics) override;

 protected:
  void _find_useful_indices(const SQLQueryCache<std::shared_ptr<SQLQueryPlan> >& cache);
  void _add_existing_indices();
  void _add_new_indices();
  void _calculate_memory_cost();
  void _calculate_desirability();

  void _inspect_query_cache(const SQLQueryCache<std::shared_ptr<SQLQueryPlan> >& cache);
  void _inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency);
  void _aggregate_access_records();

  ColumnIndexType _propose_index_type(const IndexEvaluation& index_evaluation) const;
  float _predict_memory_cost(const IndexEvaluation& index_evaluation) const;
  float _report_memory_cost(const IndexEvaluation& index_evaluation) const;

  std::set<ColumnRef> _new_indices;
  std::vector<Operation> _operations;
  std::vector<AccessRecord> _access_records;
  std::map<ColumnRef, float> _saved_work;

  std::vector<IndexEvaluation> _indices;
};

}  // namespace opossum
