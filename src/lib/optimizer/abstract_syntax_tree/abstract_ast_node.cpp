#include "abstract_ast_node.hpp"

#include <algorithm>
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

bool AbstractASTNode::is_optimizable() const { return true; }

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
  DebugAssert(left || !_right_child, "Node can't have right child and no left child");

  _left_child = left;
  if (left) left->_parent = shared_from_this();

  _on_child_changed();
}

const std::shared_ptr<AbstractASTNode> &AbstractASTNode::right_child() const { return _right_child; }

void AbstractASTNode::set_right_child(const std::shared_ptr<AbstractASTNode> &right) {
  DebugAssert(_left_child != nullptr, "Cannot set right child without having a left child.");

  _right_child = right;
  if (right) right->_parent = shared_from_this();

  _on_child_changed();
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

const std::vector<std::string> &AbstractASTNode::output_column_names() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->output_column_names();
}

const std::vector<ColumnID> &AbstractASTNode::output_column_id_to_input_column_id() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->output_column_id_to_input_column_id();
}

size_t AbstractASTNode::output_col_count() const { return output_column_names().size(); }

ColumnID AbstractASTNode::get_column_id_by_named_column_reference(
    const NamedColumnReference &named_column_reference) const {
  const auto column_id = find_column_id_by_named_column_reference(named_column_reference);
  DebugAssert(column_id,
              std::string("NamedColumnReference ") + named_column_reference.column_name + " could not be resolved.");
  return *column_id;
}

optional<ColumnID> AbstractASTNode::find_column_id_by_named_column_reference(
    const NamedColumnReference &named_column_reference) const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->find_column_id_by_named_column_reference(named_column_reference);
}

bool AbstractASTNode::knows_table(const std::string &table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->knows_table(table_name);
}

std::vector<ColumnID> AbstractASTNode::get_output_column_ids_for_table(const std::string &table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");

  if (!_left_child->knows_table(table_name)) {
    return {};
  }

  return _left_child->get_output_column_ids_for_table(table_name);
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
