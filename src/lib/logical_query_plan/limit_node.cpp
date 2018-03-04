#include "limit_node.hpp"

#include <string>

#include "utils/assert.hpp"

namespace opossum {

LimitNode::LimitNode(const size_t num_rows) : AbstractLQPNode(LQPNodeType::Limit), _num_rows(num_rows) {}

std::shared_ptr<AbstractLQPNode> LimitNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return LimitNode::make(_num_rows);
}

std::string LimitNode::description() const { return "[Limit] " + std::to_string(_num_rows) + " rows"; }

size_t LimitNode::num_rows() const { return _num_rows; }

bool LimitNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& limit_node = static_cast<const LimitNode&>(rhs);

  return _num_rows == limit_node._num_rows;
}

}  // namespace opossum
