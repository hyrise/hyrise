#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public AbstractLQPNode {
 public:
  ValidateNode();

  std::string description() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _clone_impl() const override;
};

}  // namespace opossum
