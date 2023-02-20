#pragma once

#include "abstract_dependency_candidate_rule.hpp"

namespace hyrise {

class DependentGroupByReductionCandidateRule : public AbstractDependencyCandidateRule {
 public:
  DependentGroupByReductionCandidateRule();

  void apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                     DependencyCandidates& candidates) const final;
};

}  // namespace hyrise
