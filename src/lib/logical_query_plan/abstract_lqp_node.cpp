#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "column_origin.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TableStatistics;

AbstractLQPNode::AbstractLQPNode(LQPNodeType node_type) : _type(node_type) {}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::deep_copy() const {
  auto left_child = this->left_child() ? this->left_child()->deep_copy() : std::shared_ptr<AbstractLQPNode>();
  auto right_child = this->right_child() ? this->right_child()->deep_copy() : std::shared_ptr<AbstractLQPNode>();

  // We cannot use the copy constructor here, because it does not work with shared_from_this()
  auto deep_copy = _deep_copy_impl(left_child, right_child);
  deep_copy->set_left_child(left_child);
  deep_copy->set_right_child(right_child);

  return deep_copy;
}

ColumnOrigin AbstractLQPNode::clone_column_origin(const ColumnOrigin &column_origin,
                                                  const std::shared_ptr<AbstractLQPNode> &lqp_copy) const {
  Assert(output_column_count() == lqp_copy->output_column_count(), "lqp_copy must be a copy of this");
  return lqp_copy->output_column_origins()[get_output_column_id_by_column_origin(column_origin)];
}

std::vector<std::shared_ptr<AbstractLQPNode>> AbstractLQPNode::parents() const {
  std::vector<std::shared_ptr<AbstractLQPNode>> parents;
  parents.reserve(_parents.size());

  for (const auto& parent_weak_ptr : _parents) {
    const auto parent = parent_weak_ptr.lock();
    DebugAssert(parent, "Failed to lock parent");
    parents.emplace_back(parent);
  }

  return parents;
}

void AbstractLQPNode::remove_parent(const std::shared_ptr<AbstractLQPNode>& parent) {
  const auto child_side = get_child_side(parent);
  parent->set_child(child_side, nullptr);
}

void AbstractLQPNode::clear_parents() {
  // Don't use for-each loop here, as remove_parent manipulates the _parents vector
  while (!_parents.empty()) {
    auto parent = _parents.front().lock();
    DebugAssert(parent, "Failed to lock parent");
    remove_parent(parent);
  }
}

LQPChildSide AbstractLQPNode::get_child_side(const std::shared_ptr<AbstractLQPNode>& parent) const {
  if (parent->_children[0].get() == this) {
    return LQPChildSide::Left;
  } else if (parent->_children[1].get() == this) {
    return LQPChildSide::Right;
  } else {
    Fail("Specified parent node is not actually a parent node of this node.");
    return LQPChildSide::Left;  // Make compilers happy
  }
}

std::vector<LQPChildSide> AbstractLQPNode::get_child_sides() const {
  std::vector<LQPChildSide> child_sides;
  child_sides.reserve(_parents.size());

  for (const auto& parent_weak_ptr : _parents) {
    const auto parent = parent_weak_ptr.lock();
    DebugAssert(parent, "Failed to lock parent");
    child_sides.emplace_back(get_child_side(parent));
  }

  return child_sides;
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::left_child() const { return _children[0]; }

void AbstractLQPNode::set_left_child(const std::shared_ptr<AbstractLQPNode>& left) {
  set_child(LQPChildSide::Left, left);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::right_child() const { return _children[1]; }

void AbstractLQPNode::set_right_child(const std::shared_ptr<AbstractLQPNode>& right) {
  set_child(LQPChildSide::Right, right);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::child(LQPChildSide side) const {
  const auto child_index = static_cast<int>(side);
  return _children[child_index];
}

void AbstractLQPNode::set_child(LQPChildSide side, const std::shared_ptr<AbstractLQPNode>& child) {
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

LQPNodeType AbstractLQPNode::type() const { return _type; }

bool AbstractLQPNode::subtree_is_read_only() const {
  auto read_only = true;
  if (left_child()) read_only &= left_child()->subtree_is_read_only();
  if (right_child()) read_only &= right_child()->subtree_is_read_only();
  return read_only;
}

bool AbstractLQPNode::subtree_is_validated() const {
  if (type() == LQPNodeType::Validate) return true;

  if (!left_child() && !right_child()) return false;

  auto children_validated = true;
  if (left_child()) children_validated &= left_child()->subtree_is_validated();
  if (right_child()) children_validated &= right_child()->subtree_is_validated();
  return children_validated;
}

void AbstractLQPNode::set_statistics(const std::shared_ptr<TableStatistics>& statistics) { _statistics = statistics; }

const std::shared_ptr<TableStatistics> AbstractLQPNode::get_statistics() {
  if (!_statistics) {
    _statistics = derive_statistics_from(left_child(), right_child());
  }

  return _statistics;
}

std::shared_ptr<TableStatistics> AbstractLQPNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  DebugAssert(left_child,
              "Default implementation of derive_statistics_from() requires a left child, override in concrete node "
              "implementation for different behavior");
  DebugAssert(!right_child, "Default implementation of derive_statistics_from() cannot have a right_child");

  return left_child->get_statistics();
}

const std::vector<std::string>& AbstractLQPNode::output_column_names() const {
  /**
   * This function has to be overwritten if columns or their order are in any way redefined by this Node.
   * Examples include Projections, Aggregates, and Joins.
   */
  DebugAssert(left_child() && !right_child(),
              "Node has no or two inputs and therefore needs to override this function.");
  return left_child()->output_column_names();
}

const std::vector<ColumnOrigin>& AbstractLQPNode::output_column_origins() const {
  if (!_output_column_origins) {
    Assert(!left_child() || !right_child(), "Nodes with both children need to override");
    if (left_child()) {
      _output_column_origins = left_child()->output_column_origins();
    } else {
      _output_column_origins.emplace();
      for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
        _output_column_origins->emplace_back(shared_from_this(), column_id);
      }
    }
  }

  return *_output_column_origins;
}

std::optional<ColumnOrigin> AbstractLQPNode::find_column_origin_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  /**
   * If this node carries an alias, that is different from that of the Column reference, we can't resolve the column
   * in this node
   */
  const auto named_column_reference_without_local_column_prefix = _resolve_local_column_prefix(named_column_reference);
  if (!named_column_reference_without_local_column_prefix) {
    return std::nullopt;
  }

  /**
   * If the table name got resolved, look for the Column in the output of this node
   */
  if (!named_column_reference_without_local_column_prefix->table_name) {
    for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
      if (output_column_names()[column_id] == named_column_reference_without_local_column_prefix->column_name) {
        return output_column_origins()[column_id];
      }
    }
    return std::nullopt;
  }

  /**
   * Look for the Column in child nodes
   */
  const auto resolve_named_column_reference = [&](const auto& node,
                                                  const auto& named_column_reference) -> std::optional<ColumnOrigin> {
    if (node) {
      const auto column_origin = node->find_column_origin_by_named_column_reference(named_column_reference);
      if (column_origin) {
        if (find_output_column_id_by_column_origin(*column_origin)) {
          return column_origin;
        }
      }
    }
    return std::nullopt;
  };

  const auto column_origin_from_left =
      resolve_named_column_reference(left_child(), *named_column_reference_without_local_column_prefix);
  const auto column_origin_from_right =
      resolve_named_column_reference(right_child(), *named_column_reference_without_local_column_prefix);

  Assert(!column_origin_from_left || !column_origin_from_right || column_origin_from_left == column_origin_from_right,
         "Column '" + named_column_reference_without_local_column_prefix->as_string() + "' is ambiguous");

  if (column_origin_from_left) {
    return column_origin_from_left;
  }
  return column_origin_from_right;
}

ColumnOrigin AbstractLQPNode::get_column_origin_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  const auto colum_origin = find_column_origin_by_named_column_reference(named_column_reference);
  DebugAssert(colum_origin, "Couldn't resolve column origin of " + named_column_reference.as_string());
  return *colum_origin;
}

ColumnOrigin AbstractLQPNode::get_column_origin_by_output_column_id(const ColumnID column_id) const {
  Assert(column_id < output_column_count(), "ColumnID out of range");
  return output_column_origins()[column_id];
}

std::shared_ptr<const AbstractLQPNode> AbstractLQPNode::find_table_name_origin(const std::string& table_name) const {
  if (_table_alias && *_table_alias == table_name) {
    return shared_from_this();
  }

  if (!left_child() || _table_alias) {
    return nullptr;
  }

  const auto table_name_origin_in_left_child = left_child()->find_table_name_origin(table_name);

  if (right_child()) {
    const auto table_name_origin_in_right_child = right_child()->find_table_name_origin(table_name);

    if (table_name_origin_in_left_child && table_name_origin_in_right_child) {
      Assert(table_name_origin_in_left_child == table_name_origin_in_right_child,
             "If a node has two children, both have to resolve a table name to the same node");
      return table_name_origin_in_left_child;
    } else if (table_name_origin_in_right_child) {
      return table_name_origin_in_right_child;
    }
  }

  return table_name_origin_in_left_child;
}

std::optional<ColumnID> AbstractLQPNode::find_output_column_id_by_column_origin(
    const ColumnOrigin& column_origin) const {
  const auto& output_column_origins = this->output_column_origins();

  const auto iter = std::find(output_column_origins.begin(), output_column_origins.end(), column_origin);

  if (iter == output_column_origins.end()) {
    return std::nullopt;
  }

  return static_cast<ColumnID>(std::distance(output_column_origins.begin(), iter));
}

ColumnID AbstractLQPNode::get_output_column_id_by_column_origin(const ColumnOrigin& column_origin) const {
  const auto column_id = find_output_column_id_by_column_origin(column_origin);
  Assert(column_id, "Couldn't resolve ColumnOrigin");
  return *column_id;
}

size_t AbstractLQPNode::output_column_count() const { return output_column_names().size(); }

void AbstractLQPNode::remove_from_tree() {
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
   * If left_child is nullptr, still call set_child so this node will get untied from the LQP.
   */
  for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
    parents[parent_idx]->set_child(child_sides[parent_idx], left_child);
  }
}

void AbstractLQPNode::replace_with(const std::shared_ptr<AbstractLQPNode>& replacement_node) {
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
   * Untie this node from the LQP
   */
  set_left_child(nullptr);
  set_right_child(nullptr);
}

void AbstractLQPNode::set_alias(const std::optional<std::string>& table_alias) { _table_alias = table_alias; }

void AbstractLQPNode::print(std::ostream& out) const {
  std::vector<bool> levels;
  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> id_by_node;
  size_t id_counter = 0;
  _print_impl(out, levels, id_by_node, id_counter);
}

std::string AbstractLQPNode::get_verbose_column_name(ColumnID column_id) const {
  DebugAssert(!right_child(), "Node with right child needs to override this function.");

  /**
   *  A AbstractLQPNode without a left child should generally be a StoredTableNode, which overrides this function. But
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

std::vector<std::string> AbstractLQPNode::get_verbose_column_names() const {
  std::vector<std::string> verbose_names(output_column_count());
  for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
    verbose_names[column_id] = get_verbose_column_name(column_id);
  }
  return verbose_names;
}

std::optional<NamedColumnReference> AbstractLQPNode::_resolve_local_column_prefix(
    const NamedColumnReference& reference) const {
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

void AbstractLQPNode::_print_impl(std::ostream& out, std::vector<bool>& levels,
                                  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t>& id_by_node,
                                  size_t& id_counter) const {
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

  /**
   * Check whether the node has been printed before
   */
  const auto iter = id_by_node.find(shared_from_this());
  if (iter != id_by_node.end()) {
    out << "Recurring Node --> [" << iter->second << "]" << std::endl;
    return;
  }

  const auto this_node_id = id_counter;
  id_counter++;
  id_by_node.emplace(shared_from_this(), this_node_id);

  /**
   *
   */
  out << "[" << this_node_id << "] " << description() << std::endl;

  levels.emplace_back(right_child() != nullptr);

  if (left_child()) {
    left_child()->_print_impl(out, levels, id_by_node, id_counter);
  }
  if (right_child()) {
    levels.back() = false;
    right_child()->_print_impl(out, levels, id_by_node, id_counter);
  }

  levels.pop_back();
}

void AbstractLQPNode::_child_changed() {
  _statistics.reset();
  _output_column_origins.reset();

  _on_child_changed();
  for (auto& parent : parents()) {
    parent->_child_changed();
  }
}

void AbstractLQPNode::_remove_parent_pointer(const std::shared_ptr<AbstractLQPNode>& parent) {
  const auto iter =
      std::find_if(_parents.begin(), _parents.end(), [&](const auto& other) { return parent == other.lock(); });
  DebugAssert(iter != _parents.end(), "Specified parent node is not actually a parent node of this node.");

  /**
   * TODO(anybody) This is actually a O(n) operation, could be O(1) by just swapping the last element into the deleted
   * element.
   */
  _parents.erase(iter);
}

void AbstractLQPNode::_add_parent_pointer(const std::shared_ptr<AbstractLQPNode>& parent) {
  // Having the same parent multiple times is allowed, e.g. for self joins
  _parents.emplace_back(parent);
}

std::shared_ptr<LQPExpression> AbstractLQPNode::_adjust_expression_to_lqp(const std::shared_ptr<LQPExpression>& expression,
const std::shared_ptr<AbstractLQPNode>& current_lqp,
const std::shared_ptr<AbstractLQPNode>& lqp_copy) const {
  if (!expression) return nullptr;

  if (expression->type() == ExpressionType::Column) {
    expression->set_column_origin(current_lqp->clone_column_origin(expression->column_origin(), lqp_copy));
  }

  for (auto& argument_expression : expression->aggregate_function_arguments()) {
    _adjust_expression_to_lqp(argument_expression, current_lqp, lqp_copy);
  }

  _adjust_expression_to_lqp(expression->left_child(), current_lqp, lqp_copy);
  _adjust_expression_to_lqp(expression->right_child(), current_lqp, lqp_copy);

  return expression;
}

std::string NamedColumnReference::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

}  // namespace opossum
