#include "create_view_node.hpp"

#include <string>

namespace opossum {

CreateViewNode::CreateViewNode(const std::string& view_name, std::shared_ptr<const AbstractLQPNode> lqp)
    : AbstractLQPNode(LQPNodeType::CreateView), _view_name(view_name), _lqp(lqp) {}

std::string CreateViewNode::view_name() const { return _view_name; }
std::shared_ptr<const AbstractLQPNode> CreateViewNode::lqp() const { return _lqp; }

std::shared_ptr<AbstractLQPNode> CreateViewNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  // no need to deep_copy the _lqp because it is const anyway
  return std::make_shared<CreateViewNode>(_view_name, _lqp);
}

std::string CreateViewNode::description() const { return "[CreateView]"; }

}  // namespace opossum
