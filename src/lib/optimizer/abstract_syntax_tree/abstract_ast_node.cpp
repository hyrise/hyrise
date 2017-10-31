#include "abstract_ast_node.hpp"

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

class TableStatistics;

AbstractASTNode::AbstractASTNode(ASTNodeType node_type) : _type(node_type) {}

bool AbstractASTNode::is_optimizable() const { return true; }

ASTChildSide AbstractASTNode::get_child_side() const {
  auto parent = this->parent();
  Assert(parent, "get_child_side() can only be called on node with a parent");
  return parent->left_child() == shared_from_this() ? ASTChildSide::Left : ASTChildSide::Right;
}

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

const std::shared_ptr<AbstractASTNode>& AbstractASTNode::left_child() const { return _left_child; }

void AbstractASTNode::set_left_child(const std::shared_ptr<AbstractASTNode>& left) {
  if (left == _left_child) return;

  DebugAssert(left || !_right_child, "Node can't have right child and no left child");

  _left_child = left;
  if (left) left->_parent = shared_from_this();

  _on_child_changed();
}

const std::shared_ptr<AbstractASTNode>& AbstractASTNode::right_child() const { return _right_child; }

void AbstractASTNode::set_right_child(const std::shared_ptr<AbstractASTNode>& right) {
  if (right == _right_child) return;

  DebugAssert(_left_child != nullptr, "Cannot set right child without having a left child.");

  _right_child = right;
  if (right) right->_parent = shared_from_this();

  _on_child_changed();
}

void AbstractASTNode::set_children(const std::shared_ptr<AbstractASTNode>& left,
                                   const std::shared_ptr<AbstractASTNode>& right) {
  set_left_child(left);
  set_right_child(right);
}

void AbstractASTNode::set_child(ASTChildSide side, const std::shared_ptr<AbstractASTNode>& child) {
  if (side == ASTChildSide::Left) {
    set_left_child(child);
  } else {
    set_right_child(child);
  }
}

ASTNodeType AbstractASTNode::type() const { return _type; }

void AbstractASTNode::set_statistics(const std::shared_ptr<TableStatistics>& statistics) { _statistics = statistics; }

const std::shared_ptr<TableStatistics> AbstractASTNode::get_statistics() {
  if (!_statistics) {
    _statistics = derive_statistics_from(left_child(), right_child());
  }

  return _statistics;
}

std::shared_ptr<TableStatistics> AbstractASTNode::derive_statistics_from(
    const std::shared_ptr<AbstractASTNode>& left_child, const std::shared_ptr<AbstractASTNode>& right_child) const {
  DebugAssert(static_cast<bool>(_left_child),
              "Default implementation of derive_statistics_from() requires a left child, override in concrete node "
              "implementation for different behavior");
  DebugAssert(!static_cast<bool>(_right_child),
              "Default implementation of derive_statistics_from() cannot have a right_child");

  return left_child->get_statistics();
}

const std::vector<std::string>& AbstractASTNode::output_column_names() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->output_column_names();
}

const std::vector<ColumnID>& AbstractASTNode::output_column_id_to_input_column_id() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(_left_child, "Node has no left child and therefore must override this function.");
  return _left_child->output_column_id_to_input_column_id();
}

size_t AbstractASTNode::output_col_count() const { return output_column_names().size(); }

ColumnID AbstractASTNode::get_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  const auto column_id = find_column_id_by_named_column_reference(named_column_reference);
  DebugAssert(column_id,
              std::string("NamedColumnReference ") + named_column_reference.column_name + " could not be resolved.");
  return *column_id;
}

std::optional<ColumnID> AbstractASTNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(_left_child,
              "Node has no left child and therefore must override this function OR the function is not supported on "
              "this node type.");

  auto named_column_reference_without_local_alias = _resolve_local_alias(named_column_reference);
  if (!named_column_reference_without_local_alias) {
    return {};
  } else {
    return _left_child->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
  }
}

bool AbstractASTNode::knows_table(const std::string& table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   */
  DebugAssert(_left_child,
              "Node has no left child and therefore must override this function OR the function is not supported on "
              "this node type.");
  if (_table_alias) {
    return *_table_alias == table_name;
  } else {
    return _left_child->knows_table(table_name);
  }
}

std::vector<ColumnID> AbstractASTNode::get_output_column_ids() const {
  std::vector<ColumnID> column_ids(output_col_count());
  std::iota(column_ids.begin(), column_ids.end(), 0);
  return column_ids;
}

std::vector<ColumnID> AbstractASTNode::get_output_column_ids_for_table(const std::string& table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   */
  DebugAssert(_left_child,
              "Node has no left child and therefore must override this function OR the function is not supported on "
              "this node type.");

  if (!knows_table(table_name)) {
    return {};
  }

  if (_table_alias && *_table_alias == table_name) {
    return get_output_column_ids();
  }

  return _left_child->get_output_column_ids_for_table(table_name);
}

void AbstractASTNode::remove_from_tree() {
  Assert(!_right_child, "Can't remove a node with two children");

  auto parent = _parent.lock();

  if (parent) {
    parent->set_left_child(_left_child);  // Note: It's totally fine for _left_child to be a nullptr
  } else if (_left_child) {
    _left_child->clear_parent();
  }
}

void AbstractASTNode::replace_in_tree(const std::shared_ptr<AbstractASTNode>& node_to_replace) {
  Assert(!_left_child && !_right_child && !parent(),
         "Can't put a Node that's already part of a tree into another tree. Call remove_from_tree() first");

  set_left_child(node_to_replace->left_child());
  set_right_child(node_to_replace->right_child());

  auto parent = node_to_replace->parent();
  if (parent) {
    if (parent->left_child() == node_to_replace) {
      parent->set_left_child(shared_from_this());
    } else {
      Assert(parent->right_child() == shared_from_this(), "Invalid binary tree");
      parent->set_right_child(shared_from_this());
    }
  }
}

void AbstractASTNode::set_alias(const std::optional<std::string>& table_alias) { _table_alias = table_alias; }

ColumnOrigins AbstractASTNode::get_column_origins() const {
  ColumnOrigins column_origins(output_col_count());

  for (size_t column_idx = 0; column_idx < column_origins.size(); ++column_idx) {
    column_origins[column_idx] = get_column_origin(make_column_id(column_idx));
  }

  return column_origins;
}

ColumnOrigin AbstractASTNode::get_column_origin(ColumnID column_id) const {
  DebugAssert(column_id < output_column_id_to_input_column_id().size(), "ColumnID out of range");

  const auto input_column_id = output_column_id_to_input_column_id()[column_id];
  if (input_column_id == INVALID_COLUMN_ID) {
    return {shared_from_this(), column_id};
  }

  DebugAssert(left_child() && !right_child(), "Must have left child and no right child to determine column origin.");
  return left_child()->get_column_origin(input_column_id);
}

void AbstractASTNode::dispatch_column_id_mapping(const ColumnOrigins& prev_column_origins) {
  /**
   * Obtain the current column origins of this node's subtree and generate a mapping from the previous column order
   * defined by `prev_column_origins`.
   * Then, propagate this mapping to the parent.
   */

  const auto post_ordering_column_origins = get_column_origins();
  const auto column_id_mapping = ast_generate_column_id_mapping(prev_column_origins, get_column_origins());

  _propagate_column_id_mapping_to_parent(column_id_mapping);
}

void AbstractASTNode::map_column_ids(const ColumnIDMapping& column_id_mapping,
                                     ASTChildSide caller_child_side) {
  /**
   * By default, simply forward to parents.
   * Derived AST node types need to override this if they want to react on column order changes
   */

  DebugAssert(left_child() && !right_child(), "Need left child and no right child.");
  DebugAssert(column_id_mapping.size() == left_child()->output_col_count(), "Invalid column_id_mapping");

  _propagate_column_id_mapping_to_parent(column_id_mapping);
}

void AbstractASTNode::print(std::ostream& out, std::vector<bool> levels) const {
  const auto max_level = levels.empty() ? 0 : levels.size() - 1;
  for (size_t level = 0; level < max_level; ++level) {
    if (levels[level]) {
      out << " | ";
    } else {
      out << "   ";
    }
  }

  if (!levels.empty()) {
    out << " \\_";
  }
  out << description() << std::endl;

  levels.emplace_back(right_child() != nullptr);

  if (left_child()) {
    left_child()->print(out, levels);
  }
  if (right_child()) {
    levels.back() = false;
    right_child()->print(out, levels);
  }

  levels.pop_back();
}

std::string AbstractASTNode::get_verbose_column_name(ColumnID column_id) const {
  Assert(!_right_child, "Nodes with both children need to override get_verbose_column_name()");

  /**
   *  A AbstractASTNode without a left child should generally be a StoredTableNode, which overrides this function. But
   *  since get_verbose_column_name() is just a convenience function we don't want to force anyone to override this
   *  function when experimenting with nodes. Thus we handle the case of no left child here as well.
   */
  if (!_left_child) {
    if (_table_alias) {
      return *_table_alias + "." + output_column_names()[column_id];
    }

    return output_column_names()[column_id];
  }

  const auto verbose_name = _left_child->get_verbose_column_name(column_id);

  if (_table_alias) {
    return *_table_alias + "." + verbose_name;
  }

  return verbose_name;
}

std::vector<std::string> AbstractASTNode::get_verbose_column_names() const {
  std::vector<std::string> verbose_names(output_col_count());
  for (auto column_id = ColumnID{0}; column_id < output_col_count(); ++column_id) {
    verbose_names[column_id] = get_verbose_column_name(column_id);
  }
  return verbose_names;
}

std::optional<NamedColumnReference> AbstractASTNode::_resolve_local_alias(const NamedColumnReference& reference) const {
  if (reference.table_name && _table_alias) {
    if (*reference.table_name == *_table_alias) {
      // The used table name is the alias of this table. Remove id from the NamedColumnReference for further search
      auto reference_without_local_alias = reference;
      reference_without_local_alias.table_name = std::nullopt;
      return reference_without_local_alias;
    } else {
      return {};
    }
  }
  return reference;
}

void AbstractASTNode::_propagate_column_id_mapping_to_parent(const ColumnIDMapping& column_id_mapping) {
  auto parent = _parent.lock();
  if (parent) {
    parent->map_column_ids(column_id_mapping, get_child_side());
  }
}

}  // namespace opossum
