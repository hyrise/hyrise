#include "create_view_node.hpp"

#include <sstream>
#include <string>

namespace opossum {

CreateViewNode::CreateViewNode(const std::string& view_name, const std::shared_ptr<const AbstractLQPNode>& lqp)
    : AbstractLQPNode(LQPNodeType::CreateView), _view_name(view_name), _lqp(lqp) {}

std::string CreateViewNode::view_name() const { return _view_name; }

std::shared_ptr<const AbstractLQPNode> CreateViewNode::lqp() const { return _lqp; }

std::shared_ptr<AbstractLQPNode> CreateViewNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  // no need to deep_copy the _lqp because it is const anyway
  return std::make_shared<CreateViewNode>(_view_name, _lqp);
}

std::string CreateViewNode::description() const {
  std::stringstream stream;
  _lqp->print(stream);

  return "[CreateView] Name: '" + _view_name + "' (\n" + stream.str() + ")";
}

const std::vector<std::string>& CreateViewNode::output_column_names() const {
  static std::vector<std::string> output_column_names_dummy;
  return output_column_names_dummy;
}

bool CreateViewNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& create_view_node = static_cast<const CreateViewNode&>(rhs);

  return _view_name == create_view_node._view_name && !_lqp->find_first_subplan_mismatch(create_view_node._lqp);
}

}  // namespace opossum
