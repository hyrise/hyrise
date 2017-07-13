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

const std::shared_ptr<AbstractExpressionNode> &AbstractExpressionNode::left() const { return _left; }

void AbstractExpressionNode::set_left(const std::shared_ptr<AbstractExpressionNode> &left) {
  _left = left;
  left->_parent = shared_from_this();
}

const std::shared_ptr<AbstractExpressionNode> &AbstractExpressionNode::right() const { return _right; }

void AbstractExpressionNode::set_right(const std::shared_ptr<AbstractExpressionNode> &right) {
  _right = right;
  right->_parent = shared_from_this();
}

const ExpressionType AbstractExpressionNode::type() const { return _type; }

void AbstractExpressionNode::print(const uint8_t indent) const {
  std::cout << std::setw(indent) << " ";
  std::cout << description() << std::endl;

  if (_left) {
    _left->print(indent + 2);
  }

  if (_right) {
    _right->print(indent + 2);
  }
}
}  // namespace opossum
