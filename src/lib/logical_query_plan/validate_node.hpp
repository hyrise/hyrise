#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace hyrise {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public EnableMakeForLQPNode<ValidateNode>, public AbstractLQPNode {
 public:
  ValidateNode();

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  // Forwards unique column combinations from the left input node.
  UniqueColumnCombinations unique_column_combinations() const override;

  OrderDependencies order_dependencies() const override;

  std::shared_ptr<InclusionDependencies> inclusion_dependencies() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
