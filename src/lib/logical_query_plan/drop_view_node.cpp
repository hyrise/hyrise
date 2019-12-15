#include "drop_view_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

DropViewNode::DropViewNode(const std::string& view_name, const bool if_exists)
    : BaseNonQueryNode(LQPNodeType::DropView), view_name(view_name), if_exists(if_exists) {}

std::string DropViewNode::description(const DescriptionMode mode) const { return "[Drop] View: '"s + view_name + "'"; }

size_t DropViewNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(view_name);
  boost::hash_combine(hash, if_exists);
  return hash;
}

std::shared_ptr<AbstractLQPNode> DropViewNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DropViewNode::make(view_name, if_exists);
}

bool DropViewNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& drop_view_node = static_cast<const DropViewNode&>(rhs);
  return view_name == drop_view_node.view_name && if_exists == drop_view_node.if_exists;
}

}  // namespace opossum
