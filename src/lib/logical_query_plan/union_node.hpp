#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type is used to (1) represent the intersect set operation with the modes Unique and All, and (2) to
 * intersect disjuncitve PosLists (using the Positions mode). For example, `a = 1 OR b = 2` can be split up into
 * and later united by a UnionNode in the Positions mode.
 *
 */

class UnionNode : public EnableMakeForLQPNode<UnionNode>, public AbstractLQPNode {
 public:
  explicit UnionNode(const SetOperationMode init_set_operation_mode);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> column_expressions() const override;
  const std::shared_ptr<ExpressionsConstraintDefinitions> constraints() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  const SetOperationMode set_operation_mode;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};
}  // namespace opossum
