#pragma once

#include "dependency_discovery/dependency_candidates.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

class AbstractDependencyCandidateRule {
 public:
  AbstractDependencyCandidateRule(const LQPNodeType init_target_node_type) : target_node_type{init_target_node_type} {}

  AbstractDependencyCandidateRule() = delete;
  virtual ~AbstractDependencyCandidateRule() = default;

  virtual void apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                             DependencyCandidates& candidates) const = 0;

  const LQPNodeType target_node_type;
};

}  // namespace hyrise
