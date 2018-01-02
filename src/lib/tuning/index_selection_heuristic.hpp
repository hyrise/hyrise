#pragma once

#include <memory>

#include "operators/abstract_operator.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

// ToDo(group01): extract into own file, add proper constructor and accessor methods
class IndexProposal {
 public:
  // The index is defined by table_name + column_id
  std::string table_name;
  ColumnID column_id;
  // A percentage (0.0 - 1.0) that defines how desirable (cost VS benefit ratio) a creation of this index is.
  // ToDo(group01): discuss how this is to be interpreted. this could be done
  // 1. Relatively across all returned proposals (there will always be a proposal with 100% and one with 0% desirab.)
  //    --> calculate desirability value by comparing absolute values across calculated IndexProposals
  //    (current-ish implementation)
  // 2. Absolutely based on some well(?)-defined bounds, e.g. if the creation costs <100ms,
  //    then it has a desirability of at least 50%.
  float desirablility;

  // Detailed benefit / cost values
  // How often this table+column was accessed
  int number_of_usages;
  // Value representing the estimated cost of an index creation operation
  int cost;

  // Greater/Less than operators to allow comparison based on desirability
  bool operator<(const IndexProposal& other) const { return (desirablility < other.desirablility); }
  bool operator>(const IndexProposal& other) const { return (desirablility > other.desirablility); }

  static bool compare_number_of_usages(const IndexProposal& a, const IndexProposal& b){
      return (a.number_of_usages < b.number_of_usages);
  }
static bool compare_cost(const IndexProposal& a, const IndexProposal& b){
    return (a.cost < b.cost);
}
};

/**
 * The IndexSelectionHeuristic takes information about the current system
 * (e.g. query plan cache, table statistics) and proposes indices to be created
 * or removed.
 */
class IndexSelectionHeuristic {
 public:
  IndexSelectionHeuristic();

  // Runs the heuristic to analyze the SystemStatistics object and returns
  // recommended changes to be made to the live system. The changes are sorted
  // by desirability, most-desirable ones first.
  const std::vector<IndexProposal>& recommend_changes(const SystemStatistics& statistics);

 protected:
    // Looks for table scans and extracts index proposals
  void _inspect_operator(const std::shared_ptr<const AbstractOperator>& op);
    // Sums up multiple index proposals to one
    void _aggregate_usages();

    // Estimates the cost of each index proposal
    void _estimate_cost();

    // Calculate the overall desirablity of each proposal.
    void _calculate_desirability();

  std::vector<IndexProposal> _index_proposals;
};

}  // namespace opossum
