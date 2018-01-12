#include "limit_node.hpp"

#include <string>

namespace opossum {

LimitNode::LimitNode(const size_t num_rows) : AbstractLQPNode(LQPNodeType::Limit), _num_rows(num_rows) {}

std::shared_ptr<AbstractLQPNode> LimitNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<LimitNode>(_num_rows);
}

std::string LimitNode::description() const { return "[Limit] " + std::to_string(_num_rows) + " rows"; }

size_t LimitNode::num_rows() const { return _num_rows; }

}  // namespace opossum
