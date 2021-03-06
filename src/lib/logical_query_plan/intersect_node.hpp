#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type is used to represent the intersect set operation.
 *
 * Please note the following about the cardinality for an implementation:
 *
 * "By default these operators remove duplicates, which can occur if there are duplicates in the inputs.
 * If ALL is specified then duplicates are not removed.
 * If t1 has m copies of row R and t2 has n copies then t1 INTERSECT ALL t2 returns min(m,n) copies of R,
 * and t1 EXCEPT ALL t2 returns max( 0, m-n) copies of R." 
 *
 * Source: https://db.apache.org/derby/papers/Intersect-design.html 
 */
class IntersectNode : public EnableMakeForLQPNode<IntersectNode>, public AbstractLQPNode {
 public:
  explicit IntersectNode(const SetOperationMode init_operation_mode);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  // Forwards unique constraints from the left input node
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;
  std::vector<FunctionalDependency> non_trivial_functional_dependencies() const override;

  const SetOperationMode set_operation_mode;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};
}  // namespace opossum
