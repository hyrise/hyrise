#pragma once

#include "dependency_mining/util.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class AbstractDependencyCandidateRule {
 public:
  AbstractDependencyCandidateRule(const LQPNodeType node_type);
  virtual ~AbstractDependencyCandidateRule() = default;
  virtual std::vector<DependencyCandidate> apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                                         const size_t priority) const = 0;
  const LQPNodeType target_node_type;
};

}  // namespace opossum
