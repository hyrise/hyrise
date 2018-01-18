#pragma once

#include <memory>

#include "operators/abstract_operator.hpp"
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

// ToDo(group01): extract into own file, add proper constructor and accessor methods
class IndexProposal {
 public:
  IndexProposal(const std::string& table_name, ColumnID column_id, float desirability)
      : table_name{table_name}, column_id{column_id}, desirablility{desirability} {}

  /**
   * The column the this index would be created on
   */
  std::string table_name;
  ColumnID column_id;

  /**
   * An IndexEvaluator specific, signed value that indicates
   * how this index will affect the overall system performance
   *
   * desirability values are relative and only comparable if estimated
   * by the same IndexEvaluator
   */
  float desirablility;

  /**
   * Operators to allow comparison based on desirability
   */
  bool operator<(const IndexProposal& other) const { return (desirablility < other.desirablility); }
  bool operator>(const IndexProposal& other) const { return (desirablility > other.desirablility); }
};

/**
 * An IndexEvaluation is an IndexEvaluators internal representation of a candidate index.
 *
 * Once multiple IndexEvaluators exist, they will use specific IndexEvaluations.
 * In a final evaluation step IndexProposals are created by calculating the desirability metric
 * from the data collected in an IndexEvaluation.
 */
class IndexEvaluation {
 public:
  IndexEvaluation(const std::string& table_name, ColumnID column_id)
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

  static bool compare_number_of_usages(const IndexEvaluation& a, const IndexEvaluation& b) {
    return (a.number_of_usages < b.number_of_usages);
  }
  static bool compare_cost(const IndexEvaluation& a, const IndexEvaluation& b) { return (a.cost < b.cost); }
};

/**
 * The IndexEvaluator takes information about the current system
 * (e.g. query plan cache, table statistics) and proposes indices to be created
 * or removed.
 */
class IndexEvaluator {
 public:
  IndexEvaluator();

  // Runs the heuristic to analyze the SystemStatistics object and returns
  // recommended changes to be made to the live system. The changes are sorted
  // by desirability, most-desirable ones first.
  const std::vector<IndexProposal>& recommend_changes(const SystemStatistics& statistics);

 protected:
  // Looks for table scans and extracts index proposals
  void _inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency);
  // Sums up multiple index proposals to one
  void _aggregate_access_records();

  // Estimates the cost of each index proposal
  void _estimate_cost();

  // Calculate the overall desirablity of each proposal.
  void _calculate_desirability();

  std::vector<AccessRecord> _access_recods;
  std::vector<IndexEvaluation> _index_evaluations;
  std::vector<IndexProposal> _index_proposals;
};

}  // namespace opossum
