#include "drop_view_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

DropViewNode::DropViewNode(const std::string& view_name)
    : AbstractLQPNode(LQPNodeType::DropView), _view_name(view_name) {}

std::string DropViewNode::description() const { return "[Drop] View: '"s + _view_name + "'"; }

const std::string& DropViewNode::view_name() const { return _view_name; }

std::shared_ptr<AbstractLQPNode> DropViewNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DropViewNode::make(_view_name);
}

bool DropViewNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return static_cast<const DropViewNode&>(rhs)._view_name == _view_name;
}

}  // namespace opossum
