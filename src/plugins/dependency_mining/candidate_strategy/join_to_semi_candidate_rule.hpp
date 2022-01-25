#pragma once

#include "abstract_dependency_candidate_rule.hpp"

namespace opossum {

class JoinToSemiCandidateRule : public AbstractDependencyCandidateRule {
 public:
  JoinToSemiCandidateRule();
  std::vector<DependencyCandidate> apply_to_node(
      const std::shared_ptr<const AbstractLQPNode>& lqp_node, const std::shared_ptr<const AbstractOperator>& op, const size_t priority,
      const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>&
          required_expressions_by_node) const final;
};

}  // namespace opossum
