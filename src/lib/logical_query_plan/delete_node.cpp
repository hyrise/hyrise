#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

DeleteNode::DeleteNode() : AbstractLQPNode(LQPNodeType::Delete) {}

std::string DeleteNode::description() const {
  std::ostringstream desc;

  desc << "[Delete]";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> DeleteNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DeleteNode::make();
}

bool DeleteNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
