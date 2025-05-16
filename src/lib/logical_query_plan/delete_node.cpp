#include "delete_node.hpp"

#include <memory>
#include <string>

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"

namespace hyrise {

DeleteNode::DeleteNode() : AbstractNonQueryNode(LQPNodeType::Delete) {}

std::string DeleteNode::description(const DescriptionMode /*mode*/) const {
  return "[Delete]";
}

std::shared_ptr<AbstractLQPNode> DeleteNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return DeleteNode::make();
}

bool DeleteNode::_on_shallow_equals(const AbstractLQPNode& /*rhs*/, const LQPNodeMapping& /*node_mapping*/) const {
  return true;
}

}  // namespace hyrise
