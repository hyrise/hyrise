#include "create_view_node.hpp"

#include <sstream>
#include <string>

#include "lqp_utils.hpp"

namespace opossum {

CreateViewNode::CreateViewNode(const std::string& view_name, const std::shared_ptr<LQPView>& view)
    : BaseNonQueryNode(LQPNodeType::CreateView), _view_name(view_name), _view(view) {}

std::string CreateViewNode::view_name() const { return _view_name; }

std::shared_ptr<LQPView> CreateViewNode::view() const { return _view; }

std::string CreateViewNode::description() const {
  std::stringstream stream;
  stream << *_view->lqp;

  return "[CreateView] Name: '" + _view_name + "' (\n" + stream.str() + ")";
}

std::shared_ptr<AbstractLQPNode> CreateViewNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreateViewNode::make(_view_name, _view->deep_copy());
}

bool CreateViewNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_view_node_rhs = static_cast<const CreateViewNode&>(rhs);

  return _view_name == create_view_node_rhs._view_name && _view->deep_equals(*create_view_node_rhs._view);
}

}  // namespace opossum
