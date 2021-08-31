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

  void run();

 private:
  TableColumnID _resolve_column_expression(const std::shared_ptr<AbstractExpression>& column_expression) const;
  void _add_if_new(DependencyCandidate& candidate);
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::vector<DependencyCandidate> _known_candidates;

  // switches for mining specific optimizations
  constexpr static bool _enable_groupby_reduction = true;
  constexpr static bool _enable_join_to_semi = false;
  constexpr static bool _enable_join_to_predicate = false;
};

}  // namespace opossum
