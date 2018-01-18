#pragma once

#include <memory>

#include "tuning/abstract_index_evaluator.hpp"
#include "tuning/index_evaluation.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * Data class containing information on
 * *what* data is read by a query (table + column),
 * *how often* that data is accessed (query frequency / # of occurrences),
 * *how much* of the data is retrieved (query selectivity).
 */
class AccessRecord {
 public:
  std::string table_name;
  ColumnID column_id;
  size_t number_of_usages;
  float selectivity;
};

/**
 * An IndexEvaluation is an IndexEvaluators internal representation of a candidate index.
 *
 * Once multiple IndexEvaluators exist, they will use specific IndexEvaluations.
 * In a final evaluation step IndexProposals are created by calculating the desirability metric
 * from the data collected in an IndexEvaluation.
 */
class IndexEvaluatorData {
 public:
  IndexEvaluatorData(const std::string& table_name, ColumnID column_id)
      : table_name{table_name}, column_id{column_id}, saved_work{0.0f}, number_of_usages{0}, cost{0} {}

  /**
   * The column the index would be created on
   */
  std::string table_name;
  ColumnID column_id;

  // Estimated amount of work that can be saved with this index
  // (sum of inverted selectivites multiplied with query occurrence)
  // Note that this is a *relative* measure, not an absolute measure of work (like touched elements)!
  float saved_work;

  // Detailed benefit / cost values
  // How often this table+column was accessed
  int number_of_usages;
  // Value representing the estimated cost of an index creation operation
  int cost;

  static bool compare_number_of_usages(const IndexEvaluatorData& a, const IndexEvaluatorData& b) {
    return (a.number_of_usages < b.number_of_usages);
  }
  static bool compare_cost(const IndexEvaluatorData& a, const IndexEvaluatorData& b) { return (a.cost < b.cost); }
};

/**
 * ToDo(group01): describe specific mechanism to determine desirability and memory_cost
 */
class IndexEvaluator : public AbstractIndexEvaluator {
 public:
  IndexEvaluator();

  std::vector<IndexEvaluation> evaluate_indices(const SystemStatistics& statistics) override;

 protected:
  // Looks for table scans and extracts index proposals
  void _inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency);
  // Sums up multiple index proposals to one
  void _aggregate_access_records();

  // Estimates the cost of each index proposal
  void _estimate_cost();

  // Calculate the overall desirablity of each proposal.
  std::vector<IndexEvaluation> _calculate_desirability();

  std::vector<AccessRecord> _access_recods;
  std::vector<IndexEvaluatorData> _index_evaluations;
};

}  // namespace opossum
