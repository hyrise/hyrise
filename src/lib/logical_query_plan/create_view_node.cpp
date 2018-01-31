#include "create_view_node.hpp"

#include <string>
#include <sstream>

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

std::string CreateViewNode::description() const {
  std::stringstream stream;
  _lqp->print(stream);

  return "[CreateView] Name: '" + _view_name + "' (\n" + stream.str() + ")";
}

const std::vector<std::string>& CreateViewNode::output_column_names() const {
  return _output_column_names_dummy;
}

}  // namespace opossum
