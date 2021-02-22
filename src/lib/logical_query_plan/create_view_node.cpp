#include "create_view_node.hpp"

#include <sstream>
#include <string>

#include "lqp_utils.hpp"

namespace opossum {

CreateViewNode::CreateViewNode(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env,
                               const std::string& init_view_name, const std::shared_ptr<LQPView>& init_view,
                               const bool init_if_not_exists)
    : AbstractNonQueryNode(LQPNodeType::CreateView),
      hyrise_env(init_hyrise_env),
      view_name(init_view_name),
      view(init_view),
      if_not_exists(init_if_not_exists) {}

std::string CreateViewNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[CreateView] " << (if_not_exists ? "IfNotExists " : "");
  stream << "Name: " << view_name << ", Columns: ";

  for (const auto& [column_id, column_name] : view->column_names) {
    stream << column_name << " ";
  }

  stream << "FROM (\n" << *view->lqp << ")";

  return stream.str();
}

size_t CreateViewNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(hyrise_env);
  boost::hash_combine(hash, view_name);
  boost::hash_combine(hash, view);
  boost::hash_combine(hash, if_not_exists);
  return hash;
}

std::shared_ptr<AbstractLQPNode> CreateViewNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreateViewNode::make(hyrise_env, view_name, view->deep_copy(), if_not_exists);
}

bool CreateViewNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_view_node_rhs = static_cast<const CreateViewNode&>(rhs);

  return hyrise_env == create_view_node_rhs.hyrise_env && view_name == create_view_node_rhs.view_name &&
         view->deep_equals(*create_view_node_rhs.view) && if_not_exists == create_view_node_rhs.if_not_exists;
}

}  // namespace opossum
