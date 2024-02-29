#include "logical_plan_root_node.hpp"

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/functional_dependency.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "utils/assert.hpp"

namespace hyrise {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLQPNode(LQPNodeType::Root) {}

std::string LogicalPlanRootNode::description(const DescriptionMode /*mode*/) const {
  return "[LogicalPlanRootNode]";
}

std::shared_ptr<AbstractLQPNode> LogicalPlanRootNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return make();
}

UniqueColumnCombinations LogicalPlanRootNode::unique_column_combinations() const {
  Fail("LogicalPlanRootNode is not expected to be queried for unique column combinations.");
}

OrderDependencies LogicalPlanRootNode::order_dependencies() const {
  Fail("LogicalPlanRootNode is not expected to be queried for order depedencies.");
}

FunctionalDependencies LogicalPlanRootNode::non_trivial_functional_dependencies() const {
  Fail("LogicalPlanRootNode is not expected to be queried for functional dependencies.");
}

bool LogicalPlanRootNode::_on_shallow_equals(const AbstractLQPNode& /*rhs*/,
                                             const LQPNodeMapping& /*node_mapping*/) const {
  return true;
}

}  // namespace hyrise
