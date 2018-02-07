#include "limit_node.hpp"

#include <string>

#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<LimitNode> LimitNode::make(const size_t num_rows, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto limit_node = std::make_shared<LimitNode>(num_rows);
  limit_node->set_left_child(child);
  return limit_node;
}

LimitNode::LimitNode(const size_t num_rows) : AbstractLQPNode(LQPNodeType::Limit), _num_rows(num_rows) {}

std::shared_ptr<AbstractLQPNode> LimitNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<LimitNode>(_num_rows);
}

std::string LimitNode::description() const { return "[Limit] " + std::to_string(_num_rows) + " rows"; }

size_t LimitNode::num_rows() const { return _num_rows; }

bool LimitNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& limit_node = dynamic_cast<const LimitNode&>(rhs);

  return _num_rows == limit_node._num_rows;
}

}  // namespace opossum
