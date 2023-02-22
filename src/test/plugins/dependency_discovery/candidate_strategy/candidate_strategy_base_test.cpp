#include "candidate_strategy_base_test.hpp"

namespace hyrise {

DependencyCandidates CandidateStrategyBaseTest::_apply_rule(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node) const {
  auto dependency_candidates = DependencyCandidates{};
  _rule->apply_to_node(lqp_node, dependency_candidates);
  return dependency_candidates;
}

}  // namespace hyrise
