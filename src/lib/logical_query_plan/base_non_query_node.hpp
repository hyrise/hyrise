#pragma once

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Base class for LQP nodes that do not query data (e.g, DML and DDL nodes) and therefore do not output columns.
 *
 * Helper class that provides a cxlumn_expressions() override and contains an empty dummy expression vector
 */
class BaseNonQueryNode : public AbstractLQPNode {
 public:
  using AbstractLQPNode::AbstractLQPNode;

  const std::vector<std::shared_ptr<AbstractExpression>>& cxlumn_expressions() const override;

 private:
  const std::vector<std::shared_ptr<AbstractExpression>> _cxlumn_expressions_dummy;  // always empty
};

}  // namespace opossum
