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
   * This function has to be overwritten if columns or their order are in any way redefined.
   * Examples include Projections, Aggregates, and Joins.
   *
   * Having two children does not necessarily mean that this function has to be overwritten, though.
   * In case of UNION, this function is perfectly valid.
   */
  DebugAssert(!!_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->output_column_names();
}

bool AbstractASTNode::has_output_column(const std::string &column_name) const {
  const auto &column_names = output_column_names();
  return std::find(column_names.begin(), column_names.end(), column_name) != column_names.end();
}

const std::vector<ColumnID> &AbstractASTNode::output_column_ids() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined.
   * Examples include Projections, Aggregates, and Joins.
   *
   * Having two children does not necessarily mean that this function has to be overwritten, though.
   * In case of UNION, this function is perfectly valid.
   */
  DebugAssert(!!_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->output_column_ids();
}

std::string AbstractASTNode::get_column_name_for_column_id(ColumnID column_id) const {
  const auto column_ids = output_column_ids();
  auto iter = std::find(column_ids.cbegin(), column_ids.cend(), column_id);

  DebugAssert(iter != column_ids.cend(), "ColumnID not found.");

  auto index = std::distance(column_ids.cbegin(), iter);
  return output_column_names()[index];
}

ColumnID AbstractASTNode::get_column_id_for_column_identifier(const ColumnIdentifier &column_identifier) const {
  const auto column_id = find_column_id_for_column_identifier(column_identifier);
  DebugAssert(!!column_id, "ColumnIdentifier could not be resolved.");
  return *column_id;
}

optional<ColumnID> AbstractASTNode::find_column_id_for_column_identifier(
    const ColumnIdentifier &column_identifier) const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined.
   * Examples include Projections, Aggregates, and Joins.
   *
   * Having two children does not necessarily mean that this function has to be overwritten, though.
   * In case of UNION, this function is perfectly valid.
   */
  DebugAssert(!!_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->find_column_id_for_column_identifier(column_identifier);
}

bool AbstractASTNode::manages_table(const std::string &table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   *
   * Having two children does not necessarily mean that this function has to be overwritten, though.
   * In case of UNION, this function is perfectly valid,
   * because rows cannot be traced back to its original table after the operator.
   */
  DebugAssert(!!_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->manages_table(table_name);
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
