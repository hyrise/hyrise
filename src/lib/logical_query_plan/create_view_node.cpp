#include "create_view_node.hpp"

#include <sstream>
#include <string>

#include "lqp_utils.hpp"

namespace opossum {

CreateViewNode::CreateViewNode(const std::string& view_name, const View& view)
    : AbstractLQPNode(LQPNodeType::CreateView), _view_name(view_name), _view(view) {}

std::string CreateViewNode::view_name() const { return _view_name; }

const View& CreateViewNode::view() const { return _view; }

std::string CreateViewNode::description() const {
  std::stringstream stream;
  _view.lqp->print(stream);

  return "[CreateView] Name: '" + _view_name + "' (\n" + stream.str() + ")";
}

std::shared_ptr<AbstractLQPNode> CreateViewNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  View copied_view{_view.lqp->deep_copy(), _view.column_names};
  return std::make_shared<CreateViewNode>(_view_name, copied_view);
}

bool CreateViewNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto create_view_node_rhs = static_cast<const CreateViewNode&>(rhs);

  return _view_name == create_view_node_rhs._view_name &&
         _view.column_names == create_view_node_rhs._view.column_names &&
         !lqp_find_subplan_mismatch(_view.lqp, create_view_node_rhs._view.lqp);
}

}  // namespace opossum
