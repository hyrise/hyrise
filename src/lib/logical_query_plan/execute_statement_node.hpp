#pragma once

#include "logical_query_plan/base_non_query_node.hpp"

namespace opossum {

class AbstractExpression;
class LQPPreparedStatement;

/**
 * LQP equivalent to the ExecuteStatement operator.
 */
class ExecuteStatementNode : public EnableMakeForLQPNode<ExecuteStatementNode>, public BaseNonQueryNode {
 public:
  ExecuteStatementNode(const std::string& name, const std::vector<std::shared_ptr<AbstractExpression>>& parameters);

  std::string description() const override;

  std::string name;
  std::vector<std::shared_ptr<AbstractExpression>> parameters;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
