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

std::vector<std::shared_ptr<AbstractASTNode>> AbstractASTNode::parents() const {
  std::vector<std::shared_ptr<AbstractASTNode>> parents;
  parents.reserve(_parents.size());

  for (const auto& parent_weak_ptr : _parents) {
    const auto parent = parent_weak_ptr.lock();
    DebugAssert(parent, "Failed to lock parent");
    parents.emplace_back(parent);
  }

  return parents;
}

size_t AbstractASTNode::num_parents() const {
  return _parents.size();
}

void AbstractASTNode::remove_parent(const std::shared_ptr<AbstractASTNode>& parent) {
  const auto child_side = get_child_side(parent);
  parent->set_child(child_side, nullptr);
}

void AbstractASTNode::clear_parents() {
  // Don't use for-each loop here, as remove_parent manipulates the _parents vector
  while (!_parents.empty()) {
    auto parent = _parents.front().lock();
    DebugAssert(parent, "Failed to lock parent");
    remove_parent(parent);
  }
}

ASTChildSide AbstractASTNode::get_child_side(const std::shared_ptr<AbstractASTNode>& parent) const {
  if (parent->_children[0].get() == this) {
    return ASTChildSide::Left;
  } else if (parent->_children[1].get() == this) {
    return ASTChildSide::Right;
  } else {
    Fail("Specified parent node is not actually a parent node of this node.");
    return ASTChildSide::Left;  // Make compilers happy
  }
}

std::vector<ASTChildSide> AbstractASTNode::get_child_sides() const {
  std::vector<ASTChildSide> child_sides;
  child_sides.reserve(_parents.size());

  for (const auto& parent_weak_ptr : _parents) {
    const auto parent = parent_weak_ptr.lock();
    DebugAssert(parent, "Failed to lock parent");
    child_sides.emplace_back(get_child_side(parent));
  }

  return child_sides;
}

std::shared_ptr<AbstractASTNode> AbstractASTNode::left_child() const { return _children[0]; }

void AbstractASTNode::set_left_child(const std::shared_ptr<AbstractASTNode>& left) {
  set_child(ASTChildSide::Left, left);
}

std::shared_ptr<AbstractASTNode> AbstractASTNode::right_child() const { return _children[1]; }

void AbstractASTNode::set_right_child(const std::shared_ptr<AbstractASTNode>& right) {
  set_child(ASTChildSide::Right, right);
}

std::shared_ptr<AbstractASTNode> AbstractASTNode::child(ASTChildSide side) const {
  const auto child_index = static_cast<int>(side);
  return _children[child_index];
}

void AbstractASTNode::set_children(const std::shared_ptr<AbstractASTNode>& left,
                                   const std::shared_ptr<AbstractASTNode>& right) {
  set_left_child(left);
  set_right_child(right);
}

void AbstractASTNode::set_child(ASTChildSide side, const std::shared_ptr<AbstractASTNode>& child) {
  // We need a reference to _children[child_index], so not calling this->child(side)
  auto& current_child = _children[static_cast<int>(side)];

  if (current_child == child) {
    return;
  }

  // Untie from previous child
  if (current_child) {
    current_child->_remove_parent_pointer(shared_from_this());
  }

  /**
   * Tie in the new child
   */
  current_child = child;
  if (current_child) {
    current_child->_add_parent_pointer(shared_from_this());
  }

  for (auto& parent : parents()) {
    parent->_child_changed();
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
  DebugAssert(left_child,
              "Default implementation of derive_statistics_from() requires a left child, override in concrete node "
              "implementation for different behavior");
  DebugAssert(!right_child, "Default implementation of derive_statistics_from() cannot have a right_child");

  return left_child->get_statistics();
}

const std::vector<std::string>& AbstractASTNode::output_column_names() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function.");
  return left_child()->output_column_names();
}

const std::vector<ColumnID>& AbstractASTNode::output_column_ids_to_input_column_ids() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function.");

  if (!_output_column_ids_to_input_column_ids) {
    _output_column_ids_to_input_column_ids.emplace(output_column_count());
    std::iota(_output_column_ids_to_input_column_ids->begin(), _output_column_ids_to_input_column_ids->end(),
              ColumnID{0});
  }
  return *_output_column_ids_to_input_column_ids;
}

size_t AbstractASTNode::output_column_count() const { return output_column_names().size(); }

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
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function");

  auto named_column_reference_without_local_alias = _resolve_local_alias(named_column_reference);
  if (!named_column_reference_without_local_alias) {
    return {};
  } else {
    return left_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
  }
}

bool AbstractASTNode::knows_table(const std::string& table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   */
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function");
  if (_table_alias) {
    return *_table_alias == table_name;
  } else {
    return left_child()->knows_table(table_name);
  }
}

std::vector<ColumnID> AbstractASTNode::get_output_column_ids() const {
  std::vector<ColumnID> column_ids(output_column_count());
  std::iota(column_ids.begin(), column_ids.end(), 0);
  return column_ids;
}

std::vector<ColumnID> AbstractASTNode::get_output_column_ids_for_table(const std::string& table_name) const {
  /**
   * This function might have to be overwritten if a node can handle different input tables, e.g. a JOIN.
   */
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function.");

  if (!knows_table(table_name)) {
    return {};
  }

  if (_table_alias && *_table_alias == table_name) {
    return get_output_column_ids();
  }

  return left_child()->get_output_column_ids_for_table(table_name);
}

void AbstractASTNode::remove_from_tree() {
  Assert(!right_child(), "Can only remove nodes that only have a left child or no children");

  /**
   * Back up parents and in which child side they hold this node
   */
  auto parents = this->parents();
  auto child_sides = this->get_child_sides();

  /**
   * Hold left_child ptr in extra variable to keep the ref count up and untie it from this node.
   * left_child might be nullptr
   */
  auto left_child = this->left_child();
  set_left_child(nullptr);

  /**
   * Tie this nodes previous parents with this nodes previous left child
   * If left_child is nullptr, still call set_child so this node will get untied from the AST.
   */
  for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
    parents[parent_idx]->set_child(child_sides[parent_idx], left_child);
  }
}

void AbstractASTNode::replace_with(const std::shared_ptr<AbstractASTNode>& replacement_node) {
  DebugAssert(replacement_node->_parents.empty(), "Node can't have parents");
  DebugAssert(!replacement_node->left_child() && !replacement_node->right_child(), "Node can't have children");

  const auto parents = this->parents();
  const auto child_sides = this->get_child_sides();

  /**
   * Tie the replacement_node with this nodes children
   */
  replacement_node->set_left_child(left_child());
  replacement_node->set_right_child(right_child());

  /**
   * Tie the replacement_node with this nodes parents. This will effectively perform clear_parents() on this node.
   */
  for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
    parents[parent_idx]->set_child(child_sides[parent_idx], replacement_node);
  }

  /**
   * Untie this node from the AST
   */
  set_left_child(nullptr);
  set_right_child(nullptr);
}

void AbstractASTNode::set_alias(const std::optional<std::string>& table_alias) { _table_alias = table_alias; }

ColumnOrigins AbstractASTNode::get_column_origins() const {
  ColumnOrigins column_origins(output_column_count());

  for (size_t column_idx = 0; column_idx < column_origins.size(); ++column_idx) {
    column_origins[column_idx] = get_column_origin(make_column_id(column_idx));
  }

  return column_origins;
}

ColumnOrigin AbstractASTNode::get_column_origin(ColumnID column_id) const {
  DebugAssert(column_id < output_column_ids_to_input_column_ids().size(), "ColumnID out of range");

  const auto input_column_id = output_column_ids_to_input_column_ids()[column_id];
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

  _propagate_column_id_mapping_to_parents(column_id_mapping);
}

void AbstractASTNode::map_column_ids(const ColumnIDMapping& column_id_mapping, ASTChildSide caller_child_side) {
  /**
   * By default, simply forward to parents.
   * Derived AST node types need to override this if they want to react on column order changes
   */

  DebugAssert(left_child() && !right_child(), "Need left child and no right child.");
  DebugAssert(column_id_mapping.size() == left_child()->output_column_count(), "Invalid column_id_mapping");

  _propagate_column_id_mapping_to_parents(column_id_mapping);
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
  DebugAssert(!right_child(), "Node with right child needs to override this function.");

  /**
   *  A AbstractASTNode without a left child should generally be a StoredTableNode, which overrides this function. But
   *  since get_verbose_column_name() is just a convenience function we don't want to force anyone to override this
   *  function when experimenting with nodes. Thus we handle the case of no left child here as well.
   */
  if (!left_child()) {
    if (_table_alias) {
      return *_table_alias + "." + output_column_names()[column_id];
    }

    return output_column_names()[column_id];
  }

  const auto verbose_name = left_child()->get_verbose_column_name(column_id);

  if (_table_alias) {
    return *_table_alias + "." + verbose_name;
  }

  return verbose_name;
}

std::vector<std::string> AbstractASTNode::get_verbose_column_names() const {
  std::vector<std::string> verbose_names(output_column_count());
  for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
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

void AbstractASTNode::_propagate_column_id_mapping_to_parents(const ColumnIDMapping& column_id_mapping) {
  for (auto& parent : parents()) {
    parent->map_column_ids(column_id_mapping, get_child_side(parent));
  }
}

void AbstractASTNode::_child_changed() {
  _statistics.reset();
  _output_column_ids_to_input_column_ids.reset();

  _on_child_changed();
  for (auto& parent : parents()) {
    parent->_child_changed();
  }
}

void AbstractASTNode::_remove_parent_pointer(const std::shared_ptr<AbstractASTNode>& parent) {
  const auto iter =
      std::find_if(_parents.begin(), _parents.end(), [&](const auto& rhs) { return parent == rhs.lock(); });
  DebugAssert(iter != _parents.end(), "Specified parent node is not actually a parent node of this node.");

  /**
   * TODO(anybody) This is actually a O(n) operation, could be O(1) by just swapping the last element into the deleted
   * element.
   */
  _parents.erase(iter);
}

void AbstractASTNode::_add_parent_pointer(const std::shared_ptr<AbstractASTNode>& parent) {
#if IS_DEBUG
  const auto iter =
      std::find_if(_parents.begin(), _parents.end(), [&](const auto& rhs) { return parent == rhs.lock(); });
  DebugAssert(iter == _parents.end(), "Specified new parent node is already a parent node.");
#endif
  _parents.emplace_back(parent);
}

}  // namespace opossum
