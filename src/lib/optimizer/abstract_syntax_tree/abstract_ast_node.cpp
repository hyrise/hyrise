#include "abstract_ast_node.hpp"

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace opossum {

AbstractAstNode::AbstractAstNode(AstNodeType node_type) : _type(node_type) {}

std::shared_ptr<AbstractAstNode> AbstractAstNode::parent() const { return _parent.lock(); }

void AbstractAstNode::clear_parent() { _parent = {}; }

const std::shared_ptr<AbstractAstNode> &AbstractAstNode::left() const { return _left; }

void AbstractAstNode::set_left(const std::shared_ptr<AbstractAstNode> &left) {
  _left = left;
  if (left) left->_parent = shared_from_this();
}

const std::shared_ptr<AbstractAstNode> &AbstractAstNode::right() const { return _right; }

void AbstractAstNode::set_right(const std::shared_ptr<AbstractAstNode> &right) {
  _right = right;
  if (right) right->_parent = shared_from_this();
}

AstNodeType AbstractAstNode::type() const { return _type; }

const std::vector<std::string> &AbstractAstNode::output_columns() const {
  if (_left && !_right) return _left->output_columns();
  if (!_left && _right) return _right->output_columns();

  /**
   * Rebuild _output_columns when node has both children as there is no way to detect whether one of them has changed
   */
  if (_left) {
    auto left_output_columns = _left->output_columns();
    _output_columns.insert(_output_columns.end(), left_output_columns.begin(), left_output_columns.end());
  }

  if (_right) {
    auto right_output_columns = _right->output_columns();
    _output_columns.insert(_output_columns.end(), right_output_columns.begin(), right_output_columns.end());
  }

  return _output_columns;
}

void AbstractAstNode::print(const uint32_t indent, std::ostream &out) const {
  out << std::setw(indent) << " ";
  out << description() << std::endl;

  if (_left) {
    _left->print(indent + 2, out);
  }

  if (_right) {
    _right->print(indent + 2, out);
  }
}
}  // namespace opossum
