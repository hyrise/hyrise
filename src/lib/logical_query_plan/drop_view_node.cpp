#include "drop_view_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

DropViewNode::DropViewNode(const std::string& view_name)
    : AbstractLQPNode(LQPNodeType::DropView), _view_name(view_name) {}

std::shared_ptr<AbstractLQPNode> DropViewNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<DropViewNode>(_view_name);
}

std::string DropViewNode::description() const {
  std::ostringstream desc;

  desc << "[Drop] View: '" << _view_name << "'";

  return desc.str();
}

bool DropViewNode::subtree_is_read_only() const { return false; }

const std::string& DropViewNode::view_name() const { return _view_name; }

}  // namespace opossum
