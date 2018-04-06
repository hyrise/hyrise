#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public EnableMakeForLQPNode<ValidateNode>, public AbstractLQPNode {
 public:
  ValidateNode();

  std::string description() const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  AbstractLQPNodeSPtr _deep_copy_impl(
      const AbstractLQPNodeSPtr& copied_left_input,
      const AbstractLQPNodeSPtr& copied_right_input) const override;
};

}  // namespace opossum
