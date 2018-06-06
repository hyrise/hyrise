#include "create_view_node.hpp"

#include <sstream>
#include <string>

#include "lqp_utils.hpp"

namespace opossum {

CreateViewNode::CreateViewNode(const std::string& view_name, const std::shared_ptr<View>& view)
    : AbstractLQPNode(LQPNodeType::CreateView), _view_name(view_name), _view(view) {}

std::string CreateViewNode::view_name() const { return _view_name; }

std::shared_ptr<View> CreateViewNode::view() const { return _view; }

std::string CreateViewNode::description() const {
  std::stringstream stream;
  _view->lqp->print(stream);

  return "[CreateView] Name: '" + _view_name + "' (\n" + stream.str() + ")";
}

std::shared_ptr<AbstractLQPNode> CreateViewNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return std::make_shared<CreateViewNode>(_view_name, _view->deep_copy());
}

bool CreateViewNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto create_view_node_rhs = static_cast<const CreateViewNode&>(rhs);

  return _view_name == create_view_node_rhs._view_name &&
         _view->deep_equals(*create_view_node_rhs._view);
}

}  // namespace opossum
