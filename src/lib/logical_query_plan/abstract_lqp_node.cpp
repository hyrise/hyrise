#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "lqp_column_reference.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace opossum {

class TableStatistics;

AbstractLQPNode::AbstractLQPNode(LQPNodeType node_type) : _type(node_type) {}

LQPColumnReference AbstractLQPNode::adapt_column_reference_to_different_lqp(
    const LQPColumnReference& column_reference, const std::shared_ptr<AbstractLQPNode>& original_lqp,
    const std::shared_ptr<AbstractLQPNode>& copied_lqp) {
  /**
   * Map a ColumnReference to the same ColumnReference in a different LQP, by
   * (1) Figuring out the ColumnID it has in the original node
   * (2) Returning the ColumnReference at that ColumnID in the copied node
   */

  const auto output_column_id = original_lqp->get_output_column_id(column_reference);
  return copied_lqp->output_column_references()[output_column_id];
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

  _child_changed();
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

const std::vector<LQPColumnReference>& AbstractLQPNode::output_column_references() const {
  /**
   * Default implementation of output_column_references() will return the ColumnReferences of the left_child if it exists,
   * otherwise will pretend that all Columns originate in this node.
   * Nodes with both children need to override this as the default implementation can't cover their behaviour.
   */

  if (!_output_column_references) {
    Assert(!right_child(), "Nodes that have two children must override this method");
    if (left_child()) {
      _output_column_references = left_child()->output_column_references();
    } else {
      _output_column_references.emplace();
      for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
        _output_column_references->emplace_back(shared_from_this(), column_id);
      }
    }
  }

  return *_output_column_references;
}

std::optional<LQPColumnReference> AbstractLQPNode::find_column(const QualifiedColumnName& qualified_column_name) const {
  /**
   * If this node carries an alias that is different from that of the NamedColumnReference, we can't resolve the column
   * in this node. E.g. in `SELECT t1.a FROM t1 AS something_else;` "t1.a" can't be resolved since it carries an alias.
   */
  const auto qualified_column_name_without_local_table_name = _resolve_local_table_name(qualified_column_name);
  if (!qualified_column_name_without_local_table_name) {
    return std::nullopt;
  }

  /**
   * If the table name got resolved (i.e., the alias or name of this node equals the table name), look for the Column
   * in the output of this node
   */
  if (!qualified_column_name_without_local_table_name->table_name) {
    for (auto column_id = ColumnID{0}; column_id < output_column_count(); ++column_id) {
      if (output_column_names()[column_id] == qualified_column_name_without_local_table_name->column_name) {
        return output_column_references()[column_id];
      }
    }
    return std::nullopt;
  }

  /**
   * Look for the Column in child nodes
   */
  const auto resolve_qualified_column_name = [&](
      const auto& node, const auto& qualified_column_name) -> std::optional<LQPColumnReference> {
    if (node) {
      const auto column_reference = node->find_column(qualified_column_name);
      if (column_reference) {
        if (find_output_column_id(*column_reference)) {
          return column_reference;
        }
      }
    }
    return std::nullopt;
  };

  const auto column_reference_from_left =
      resolve_qualified_column_name(left_child(), *qualified_column_name_without_local_table_name);
  const auto column_reference_from_right =
      resolve_qualified_column_name(right_child(), *qualified_column_name_without_local_table_name);

  Assert(!column_reference_from_left || !column_reference_from_right ||
             column_reference_from_left == column_reference_from_right,
         "Column '" + qualified_column_name_without_local_table_name->as_string() + "' is ambiguous");

  if (column_reference_from_left) {
    return column_reference_from_left;
  }
  return column_reference_from_right;
}

LQPColumnReference AbstractLQPNode::get_column(const QualifiedColumnName& qualified_column_name) const {
  const auto colum_origin = find_column(qualified_column_name);
  DebugAssert(colum_origin, "Couldn't resolve column origin of " + qualified_column_name.as_string());
  return *colum_origin;
}

std::shared_ptr<const AbstractLQPNode> AbstractLQPNode::find_table_name_origin(const std::string& table_name) const {
  // If this node has an ALIAS that matches the table_name, this is the node we're looking for
  if (_table_alias && *_table_alias == table_name) {
    return shared_from_this();
  }

  // If this node has an alias, it hides the names of tables in its children and search does not continue.
  // Also, it does not need to continue if there are no children
  if (!left_child() || _table_alias) {
    return nullptr;
  }

  const auto table_name_origin_in_left_child = left_child()->find_table_name_origin(table_name);

  if (right_child()) {
    const auto table_name_origin_in_right_child = right_child()->find_table_name_origin(table_name);

    if (table_name_origin_in_left_child && table_name_origin_in_right_child) {
      // Both children could contain the table in the case of a diamond-shaped LQP as produced by an OR.
      // This is legal as long as they ultimately resolve to the same table origin.
      Assert(table_name_origin_in_left_child == table_name_origin_in_right_child,
             "If a node has two children, both have to resolve a table name to the same node");
      return table_name_origin_in_left_child;
    } else if (table_name_origin_in_right_child) {
      return table_name_origin_in_right_child;
    }
  }

  return table_name_origin_in_left_child;
}

std::optional<ColumnID> AbstractLQPNode::find_output_column_id(const LQPColumnReference& column_reference) const {
  const auto& output_column_references = this->output_column_references();
  const auto iter = std::find(output_column_references.begin(), output_column_references.end(), column_reference);

  if (iter == output_column_references.end()) {
    return std::nullopt;
  }

  return static_cast<ColumnID>(std::distance(output_column_references.begin(), iter));
}

ColumnID AbstractLQPNode::get_output_column_id(const LQPColumnReference& column_reference) const {
  const auto column_id = find_output_column_id(column_reference);
  Assert(column_id, "Couldn't resolve LQPColumnReference");
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
  const auto get_children_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractLQPNode>> children;
    if (node->left_child()) children.emplace_back(node->left_child());
    if (node->right_child()) children.emplace_back(node->right_child());
    return children;
  };
  const auto node_print_fn = [](const auto& node, auto& stream) {
    stream << node->description();

    if (node->_table_alias) {
      stream << " -- ALIAS: '" << *node->_table_alias << "'";
    }
  };

  print_directed_acyclic_graph<const AbstractLQPNode>(shared_from_this(), get_children_fn, node_print_fn, out);
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

std::optional<QualifiedColumnName> AbstractLQPNode::_resolve_local_table_name(
    const QualifiedColumnName& qualified_column_name) const {
  if (qualified_column_name.table_name && _table_alias) {
    if (*qualified_column_name.table_name == *_table_alias) {
      // The used table name is the alias of this table. Remove id from the QualifiedColumnName for further search
      auto reference_without_local_alias = qualified_column_name;
      reference_without_local_alias.table_name = std::nullopt;
      return reference_without_local_alias;
    } else {
      return {};
    }
  }
  return qualified_column_name;
}

void AbstractLQPNode::_child_changed() {
  _statistics.reset();
  _output_column_references.reset();

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

std::shared_ptr<LQPExpression> AbstractLQPNode::adapt_expression_to_different_lqp(
    const std::shared_ptr<LQPExpression>& expression, const std::shared_ptr<AbstractLQPNode>& original_lqp,
    const std::shared_ptr<AbstractLQPNode>& copied_lqp) {
  if (!expression) return nullptr;

  if (expression->type() == ExpressionType::Column) {
    expression->set_column_reference(
        adapt_column_reference_to_different_lqp(expression->column_reference(), original_lqp, copied_lqp));
  }

  for (auto& argument_expression : expression->aggregate_function_arguments()) {
    adapt_expression_to_different_lqp(argument_expression, original_lqp, copied_lqp);
  }

  adapt_expression_to_different_lqp(expression->left_child(), original_lqp, copied_lqp);
  adapt_expression_to_different_lqp(expression->right_child(), original_lqp, copied_lqp);

  return expression;
}

std::string QualifiedColumnName::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::deep_copy() const {
  PreviousCopiesMap previous_copies;
  return _deep_copy(previous_copies);
}

std::shared_ptr<AbstractLQPNode> AbstractLQPNode::_deep_copy(PreviousCopiesMap& previous_copies) const {
  if (auto it = previous_copies.find(shared_from_this()); it != previous_copies.cend()) {
    return it->second;
  }

  auto copied_left_child = left_child() ? left_child()->_deep_copy(previous_copies) : nullptr;
  auto copied_right_child = right_child() ? right_child()->_deep_copy(previous_copies) : nullptr;

  // We cannot use the copy constructor here, because it does not work with shared_from_this()
  auto deep_copy = _deep_copy_impl(copied_left_child, copied_right_child);
  if (copied_left_child) deep_copy->set_left_child(copied_left_child);
  if (copied_right_child) deep_copy->set_right_child(copied_right_child);

  previous_copies.emplace(shared_from_this(), deep_copy);

  return deep_copy;
}

}  // namespace opossum
