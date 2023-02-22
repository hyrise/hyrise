#pragma once

#include "../../../../plugins/dependency_discovery/candidate_strategy/abstract_dependency_candidate_rule.hpp"
#include "base_test.hpp"

namespace hyrise {

class CandidateStrategyBaseTest : public BaseTest {
 protected:
  DependencyCandidates _apply_rule(const std::shared_ptr<const AbstractLQPNode>& lqp_node) const;

  std::unique_ptr<AbstractDependencyCandidateRule> _rule;
};

}  // namespace hyrise
