#include "abstract_node.hpp"

#include <iomanip>
#include <iostream>

namespace opossum {

const std::weak_ptr<AbstractNode> &AbstractNode::get_parent() const { return _parent; }

void AbstractNode::set_parent(const std::weak_ptr<AbstractNode> &parent) { _parent = parent; }

const std::shared_ptr<AbstractNode> &AbstractNode::get_left() const { return _left; }

void AbstractNode::set_left(const std::shared_ptr<AbstractNode> &left) { _left = left; }

const std::shared_ptr<AbstractNode> &AbstractNode::get_right() const { return _right; }

void AbstractNode::set_right(const std::shared_ptr<AbstractNode> &right) { _right = right; }

void AbstractNode::print(const uint8_t indent) const {
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
