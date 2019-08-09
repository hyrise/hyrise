#include "create_prepared_plan_node.hpp"

#include <sstream>

#include "storage/prepared_plan.hpp"

namespace opossum {

CreatePreparedPlanNode::CreatePreparedPlanNode(const std::string& name,
                                               const std::shared_ptr<PreparedPlan>& prepared_plan)
    : BaseNonQueryNode(LQPNodeType::CreatePreparedPlan), name(name), prepared_plan(prepared_plan) {}

std::string CreatePreparedPlanNode::description() const {
  std::stringstream stream;
  stream << "[CreatePreparedPlan] '" << name << "' {\n";
  stream << *prepared_plan;
  stream << "}";

  return stream.str();
}

std::shared_ptr<AbstractLQPNode> CreatePreparedPlanNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreatePreparedPlanNode::make(name, prepared_plan);
}

bool CreatePreparedPlanNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_prepared_plan_node = static_cast<const CreatePreparedPlanNode&>(rhs);
  return name == create_prepared_plan_node.name && *prepared_plan == *create_prepared_plan_node.prepared_plan;
}

}  // namespace opossum
