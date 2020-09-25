#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type is used to represent the except set operation.
 */
class ExceptNode : public EnableMakeForLQPNode<ExceptNode>, public AbstractLQPNode {
 public:
  explicit ExceptNode(const SetOperationMode init_operation_mode);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  // Forwards unique constraints from the left input node
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;
  // Passes FDs from the left input node
  std::vector<FunctionalDependency> non_trivial_functional_dependencies() const override;

  const SetOperationMode set_operation_mode;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};
}  // namespace opossum
