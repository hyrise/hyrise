#pragma once

#include "candidate_strategy/abstract_dependency_candidate_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "util.hpp"

namespace opossum {

class PQPAnalyzer {
 protected:
  friend class DependencyMiningPlugin;

  PQPAnalyzer(const std::shared_ptr<DependencyCandidateQueue>& queue);

  void add_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule);

  void run();

 private:
  void _add_if_new(DependencyCandidate& candidate);
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::vector<DependencyCandidate> _known_candidates;
  //std::unordered_set<DependencyCandidate> _known_candidates;
  std::unordered_map<LQPNodeType, std::vector<std::unique_ptr<AbstractDependencyCandidateRule>>> _rules;

  // switches for mining specific optimizations
  constexpr static bool _enable_groupby_reduction = true;
  constexpr static bool _enable_join_to_semi = true;
  constexpr static bool _enable_join_to_predicate = true;
  constexpr static bool _enable_join_elimination = true;
};

}  // namespace opossum
