#include "abstract_expression_node.hpp"

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

AbstractExpressionNode::AbstractExpressionNode(ExpressionType type) : _type(type) {}

const std::weak_ptr<AbstractExpressionNode> &AbstractExpressionNode::parent() const { return _parent; }

void AbstractExpressionNode::set_parent(const std::weak_ptr<AbstractExpressionNode> &parent) { _parent = parent; }

const std::shared_ptr<AbstractExpressionNode> &AbstractExpressionNode::left_child() const { return _left_child; }

void AbstractExpressionNode::set_left_child(const std::shared_ptr<AbstractExpressionNode> &left) {
  _left_child = left;
  left->_parent = shared_from_this();
}

const std::shared_ptr<AbstractExpressionNode> &AbstractExpressionNode::right_child() const { return _right_child; }

void AbstractExpressionNode::set_right_child(const std::shared_ptr<AbstractExpressionNode> &right) {
  _right_child = right;
  right->_parent = shared_from_this();
}

const ExpressionType AbstractExpressionNode::type() const { return _type; }

void AbstractExpressionNode::print(const uint8_t level) const {
  std::cout << std::setw(level) << " ";
  std::cout << description() << std::endl;

  if (_left_child) {
    _left_child->print(level + 2);
  }

  if (_right_child) {
    _right_child->print(level + 2);
  }
}
}  // namespace opossum
