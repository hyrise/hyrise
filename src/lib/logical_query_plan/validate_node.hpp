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
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const override;
};

}  // namespace opossum
