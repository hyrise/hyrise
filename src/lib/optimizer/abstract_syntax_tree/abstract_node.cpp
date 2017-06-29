#include "abstract_node.hpp"

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

const std::weak_ptr<AbstractNode> &AbstractNode::parent() const { return _parent; }

void AbstractNode::clear_parent() { _parent = {}; }

const std::shared_ptr<AbstractNode> &AbstractNode::left() const { return _left; }

void AbstractNode::set_left(const std::shared_ptr<AbstractNode> left) {
  _left = left;
  if (left) left->_parent = shared_from_this();
}

const std::shared_ptr<AbstractNode> &AbstractNode::right() const { return _right; }

void AbstractNode::set_right(const std::shared_ptr<AbstractNode> right) {
  _right = right;
  if (right) right->_parent = shared_from_this();
}

const NodeType AbstractNode::type() const { return _type; }

void AbstractNode::set_type(const NodeType type) { _type = type; }

const std::shared_ptr<TableStatistics> AbstractNode::statistics() const { return _statistics; };
void AbstractNode::set_statistics(const std::shared_ptr<TableStatistics> statistics) { _statistics = statistics; };

const std::vector<std::string> AbstractNode::output_columns() {
  std::vector<std::string> output_columns;

  if (_left) {
    auto left_output_columns = _left->output_columns();
    output_columns.insert(output_columns.end(), left_output_columns.begin(), left_output_columns.end());
  }

  if (_right) {
    auto right_output_columns = _right->output_columns();
    output_columns.insert(output_columns.end(), right_output_columns.begin(), right_output_columns.end());
  }

  return output_columns;
}

const std::shared_ptr<TableStatistics> AbstractNode::get_or_create_statistics() {
  if (_statistics) {
    return _statistics;
  }

  _statistics = create_statistics();
  return _statistics;
}

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

std::shared_ptr<TableStatistics> AbstractNode::create_statistics() const {
  Assert(static_cast<bool>(_left),
         "Default create_statistics()-impl requires children, override for special node types");
  return _left->get_or_create_statistics();
}

}  // namespace opossum
