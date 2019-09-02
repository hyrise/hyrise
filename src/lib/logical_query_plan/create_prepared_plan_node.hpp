#pragma once

#include "logical_query_plan/base_non_query_node.hpp"

namespace opossum {

class PreparedPlan;

/**
 * LQP equivalent to the PrepareStatement operator.
 */
class CreatePreparedPlanNode : public EnableMakeForLQPNode<CreatePreparedPlanNode>, public BaseNonQueryNode {
 public:
  CreatePreparedPlanNode(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);

  std::string description() const override;

  std::string name;
  std::shared_ptr<PreparedPlan> prepared_plan;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
