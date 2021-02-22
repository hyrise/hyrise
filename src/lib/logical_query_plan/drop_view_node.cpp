#include "drop_view_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

DropViewNode::DropViewNode(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env,
                           const std::string& init_view_name, const bool init_if_exists)
    : AbstractNonQueryNode(LQPNodeType::DropView),
      hyrise_env(init_hyrise_env),
      view_name(init_view_name),
      if_exists(init_if_exists) {}

std::string DropViewNode::description(const DescriptionMode mode) const { return "[Drop] View: '"s + view_name + "'"; }

size_t DropViewNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(hyrise_env);
  boost::hash_combine(hash, view_name);
  boost::hash_combine(hash, if_exists);
  return hash;
}

std::shared_ptr<AbstractLQPNode> DropViewNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DropViewNode::make(hyrise_env, view_name, if_exists);
}

bool DropViewNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& drop_view_node = static_cast<const DropViewNode&>(rhs);
  return hyrise_env == drop_view_node.hyrise_env && view_name == drop_view_node.view_name &&
         if_exists == drop_view_node.if_exists;
}

}  // namespace opossum
