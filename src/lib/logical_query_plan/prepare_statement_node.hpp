#pragma once

#include "logical_query_plan/base_non_query_node.hpp"

namespace opossum {

class LQPPreparedStatement;

/**
 * LQP equivalent to the PrepareStatement operator.
 */
class PrepareStatementNode : public EnableMakeForLQPNode<PrepareStatementNode>, public BaseNonQueryNode {
 public:
  PrepareStatementNode(const std::string& name, const std::shared_ptr<LQPPreparedStatement>& prepared_statement);

  std::string description() const override;

  std::string name;
  std::shared_ptr<LQPPreparedStatement> prepared_statement;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
