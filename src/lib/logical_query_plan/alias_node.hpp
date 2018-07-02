#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Assign column names to expressions
 */
class AliasNode : public EnableMakeForLQPNode<AliasNode>, public AbstractLQPNode {
 public:
  AliasNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
            const std::vector<std::string>& aliases);

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;

  const std::vector<std::string> aliases;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping& node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  const std::vector<std::shared_ptr<AbstractExpression>> _expressions;
};

}  // namespace opossum
