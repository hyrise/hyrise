#pragma once

#include <map>
#include <memory>
#include <utility>

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
  IndexEvaluatorData() : saved_work{0.0f}, memory_cost{0.0f}, exists{false} {}

  // Estimated amount of work that can be saved with this index
  // (sum of inverted selectivites multiplied with query occurrence)
  // Note that this is a *relative* measure, not an absolute measure of work (like touched elements)!
  float saved_work;

  // Estimated amount of memory consumed by the index
  float memory_cost;

  // Does this index already exist?
  bool exists;
};

/**
 * ToDo(group01): describe specific mechanism to determine desirability and memory_cost
 */
class IndexEvaluator : public AbstractIndexEvaluator {
  using IndexSpec = std::pair<std::string, ColumnID>;

 public:
  IndexEvaluator();

  std::vector<IndexEvaluation> evaluate_indices(const SystemStatistics& statistics) override;

 protected:
  // Looks for table scans and extracts index proposals
  void _inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency);
  // Combines Access Records referring to the same index
  void _aggregate_access_records();

  // Estimates the cost of each index proposal
  void _estimate_cost();

  // Find existing indices to assess their usefulness
  void _find_existing_indices();

  // Calculate the overall desirablity of each proposal.
  std::vector<IndexEvaluation> _calculate_desirability();

  std::vector<AccessRecord> _access_recods;
  std::map<IndexSpec, IndexEvaluatorData> _indices;
};

}  // namespace opossum
