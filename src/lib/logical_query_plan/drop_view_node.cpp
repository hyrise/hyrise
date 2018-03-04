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
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return DropViewNode::make(_view_name);
}

std::string DropViewNode::description() const {
  std::ostringstream desc;

  desc << "[Drop] View: '" << _view_name << "'";

  return desc.str();
}

bool DropViewNode::subplan_is_read_only() const { return false; }

const std::vector<std::string>& DropViewNode::output_column_names() const {
  static std::vector<std::string> output_column_names_dummy;
  return output_column_names_dummy;
}

const std::string& DropViewNode::view_name() const { return _view_name; }

bool DropViewNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& drop_view_node = static_cast<const DropViewNode&>(rhs);

  return _view_name == drop_view_node._view_name;
}

}  // namespace opossum
