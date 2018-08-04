#pragma once

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Base class for LQP nodes that do not query data and therefore do not output columns
 */
class BaseNonQueryNode : public AbstractLQPNode {
 public:
  using AbstractLQPNode::AbstractLQPNode;

  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;

 private:
  const std::vector<std::shared_ptr<AbstractExpression>> _column_expressions_dummy; // always empty
};

}  // namespace opossum
