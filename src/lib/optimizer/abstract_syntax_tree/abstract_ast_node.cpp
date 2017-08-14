#include "abstract_ast_node.hpp"

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractASTNode::AbstractASTNode(ASTNodeType node_type) : _type(node_type) {}

std::shared_ptr<AbstractASTNode> AbstractASTNode::parent() const { return _parent.lock(); }

void AbstractASTNode::clear_parent() {
  // _parent is a weak_ptr that we need to lock
  auto parent_ptr = parent();
  if (!parent_ptr) return;

  if (parent_ptr->_left_child.get() == this) {
    parent_ptr->set_left_child(nullptr);
  } else if (parent_ptr->_right_child.get() == this) {
    parent_ptr->set_right_child(nullptr);
  } else {
    Fail("Invalid AST: ASTNode is not child of his parent.");
  }
  _parent = {};
}

const std::shared_ptr<AbstractASTNode> &AbstractASTNode::left_child() const { return _left_child; }

void AbstractASTNode::set_left_child(const std::shared_ptr<AbstractASTNode> &left) {
  _left_child = left;
  if (left) left->_parent = shared_from_this();
}

const std::shared_ptr<AbstractASTNode> &AbstractASTNode::right_child() const { return _right_child; }

void AbstractASTNode::set_right_child(const std::shared_ptr<AbstractASTNode> &right) {
  DebugAssert(_left_child != nullptr, "Cannot set right child without having a left child.");

  _right_child = right;
  if (right) right->_parent = shared_from_this();
}

ASTNodeType AbstractASTNode::type() const { return _type; }

void AbstractASTNode::set_statistics(const std::shared_ptr<TableStatistics> &statistics) { _statistics = statistics; }

const std::shared_ptr<TableStatistics> AbstractASTNode::get_statistics() {
  if (!_statistics) {
    _statistics = _gather_statistics();
  }

  return _statistics;
}

const std::shared_ptr<TableStatistics> AbstractASTNode::get_statistics_from(
    const std::shared_ptr<AbstractASTNode> &other_node) const {
  return other_node->get_statistics();
}

// TODO(mp): This does not support Joins or Unions. Add support for nodes with two children later.
// This requires changes in the Statistics interface.
const std::shared_ptr<TableStatistics> AbstractASTNode::_gather_statistics() const {
  DebugAssert(static_cast<bool>(_left_child),
              "Default implementation of _gather_statistics() requires a left child, override in concrete node "
              "implementation for different behavior");
  DebugAssert(!static_cast<bool>(_right_child),
              "Default implementation of _gather_statistics() cannot have a right_child so far");
  return get_statistics_from(_left_child);
}

std::vector<ColumnID> AbstractASTNode::output_column_ids() const {
  if (_left_child && !_right_child) return _left_child->output_column_ids();
  if (!_left_child && _right_child) return _right_child->output_column_ids();

  /**
   * Rebuild _output_columns when node has both children as there is no way to detect whether one of them has changed
   */
  _output_column_ids.clear();

  if (_left_child) {
    const auto &left_output_columns = _left_child->output_column_ids();
    _output_column_ids.insert(_output_column_ids.end(), left_output_columns.begin(), left_output_columns.end());
  }

  if (_right_child) {
    const auto &right_output_columns = _right_child->output_column_ids();
    _output_column_ids.insert(_output_column_ids.end(), right_output_columns.begin(), right_output_columns.end());
  }

  return _output_column_ids;
}

bool AbstractASTNode::find_column_id_for_column_name(std::string & column_name, ColumnID &column_id) {
  // TODO(Sven): fail if column name is ambiguous
  if (_left_child) {
    if (_left_child->find_column_id_for_column_name(column_name, column_id)) {
      return true;
    }
    if (_right_child->find_column_id_for_column_name(column_name, column_id)) {
      column_id = ColumnID{column_id + _left_child->_output_column_ids.size()};
      return true;
    }
  }
  Fail("Did not find column_name");
  return false;
}

void AbstractASTNode::print(const uint32_t level, std::ostream &out) const {
  out << std::setw(level) << " ";
  out << description() << std::endl;

  if (_left_child) {
    _left_child->print(level + 2, out);
  }

  if (_right_child) {
    _right_child->print(level + 2, out);
  }
}

}  // namespace opossum
