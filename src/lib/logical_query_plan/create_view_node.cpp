#include "create_view_node.hpp"

#include <sstream>
#include <string>

#include "lqp_utils.hpp"

namespace opossum {

CreateViewNode::CreateViewNode(const std::string& view_name, const std::shared_ptr<AbstractLQPNode>& lqp)
    : AbstractLQPNode(LQPNodeType::CreateView), _view_name(view_name), _lqp(lqp) {}

std::string CreateViewNode::view_name() const { return _view_name; }

std::shared_ptr<AbstractLQPNode> CreateViewNode::lqp() const { return _lqp; }

std::string CreateViewNode::description() const {
  std::stringstream stream;
  _lqp->print(stream);

  return "[CreateView] Name: '" + _view_name + "' (\n" + stream.str() + ")";
}

std::shared_ptr<AbstractLQPNode> CreateViewNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return std::make_shared<CreateViewNode>(_view_name, _lqp->deep_copy());
}

bool CreateViewNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto create_view_node_rhs = static_cast<const CreateViewNode&>(rhs);

  return _view_name == create_view_node_rhs._view_name && !lqp_find_subplan_mismatch(_lqp, create_view_node_rhs._lqp);
}

}  // namespace opossum
