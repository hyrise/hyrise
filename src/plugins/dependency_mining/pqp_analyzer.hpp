#pragma once

#include "dependency_mining/util.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class PQPAnalyzer {
 protected:
  friend class DependencyMiningPlugin;
  PQPAnalyzer(const std::shared_ptr<DependencyCandidateQueue>& queue);
  //void set_queue(const DependencyCandidateQueue& queue);
  void run();

 private:
  TableColumnID _resolve_column_expression(const std::shared_ptr<AbstractExpression>& column_expression) const;
  TableColumnIDs _find_od_candidate(const std::shared_ptr<const AbstractOperator>& op,
                                                const std::shared_ptr<LQPColumnExpression>& dependent) const;
  void _add_if_new(DependencyCandidate& candidate);
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::vector<DependencyCandidate> _known_candidates;
};

}  // namespace opossum
