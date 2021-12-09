#pragma once

#include "dependency_mining/util.hpp"
#include "logical_query_plan/join_node.hpp"

namespace opossum {

class AbstractDependencyCandidateRule {
 public:
  explicit AbstractDependencyCandidateRule(const LQPNodeType node_type);
  virtual ~AbstractDependencyCandidateRule() = default;
  virtual std::vector<DependencyCandidate> apply_to_node(
      const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority,
      const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>&
          required_expressions_by_node) const = 0;
  const LQPNodeType target_node_type;

 protected:
  std::vector<std::shared_ptr<const AbstractLQPNode>> _inputs_to_visit(
      const std::shared_ptr<const JoinNode>& join_node,
      const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>&
          required_expressions_by_node) const;
};

}  // namespace opossum
