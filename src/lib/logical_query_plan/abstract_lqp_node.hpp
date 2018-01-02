#pragma once

#include <array>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "column_origin.hpp"
#include "lqp_expression.hpp"
#include "types.hpp"

namespace opossum {

struct ColumnID;
class TableStatistics;

enum class LQPNodeType {
  Aggregate,
  CreateView,
  Delete,
  DropView,
  DummyTable,
  Insert,
  Join,
  Limit,
  Predicate,
  Projection,
  Root,
  ShowColumns,
  ShowTables,
  Sort,
  StoredTable,
  Update,
  Union,
  Validate,
  Mock
};

enum class LQPChildSide { Left, Right };

struct NamedColumnReference {
  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;

  std::string as_string() const;
};

/**
 * Base class for a Node in the Logical Query Plan.
 *
 * The Logical Query Plan (abbreviated LQP) is a Directional Acyclic Graph (DAG) with each node having 0..2 incoming
 * edges and 0..n outgoing edges. The adjacent nodes on incoming edges are called "children" and those on the outgoing
 * edges "parents". The direction of the edges models data flow (with "data" being Tables) where children produce the
 * input data their parents operate on. A Node represents an "Operation" such as the application of a Predicate or a
 * Join.
 *
 * The LQP is created by the SQLTranslator and can optionally be further processed by the Optimizer.
 * The LQPTranslator creates the actual executable Operator-DAG. We are very careful that the LQP remains semantically
 * the same whether it was optimized or not.
 *
 * Design decision:
 * We decided to have mutable Nodes for now. By that we can apply rules without creating new nodes for every
 * optimization rule.
 *
 * We do not want people to copy an LQP node as a copy would still have the same children. Instead, they should use
 * deep_copy().
 */
class AbstractLQPNode : public std::enable_shared_from_this<AbstractLQPNode>, private Noncopyable {
 public:
  explicit AbstractLQPNode(LQPNodeType node_type);

  // Creates a deep copy
  virtual std::shared_ptr<AbstractLQPNode> deep_copy() const;
  ColumnOrigin clone_column_origin(const ColumnOrigin &origin,
                                   const std::shared_ptr<AbstractLQPNode> &lqp_copy) const;

  // @{
  /**
   * Set and get the parents/children of this node.
   *
   * The _parents are implicitly set in set_left_child/set_right_child.
   * For removing parents use remove_parent() or clear_parents().
   *
   * set_child() is a shorthand for set_left_child() or set_right_child(), useful if the side is a runtime value
   */

  /**
   * Locks all parents and returns them as shared_ptrs
   */
  std::vector<std::shared_ptr<AbstractLQPNode>> parents() const;

  void remove_parent(const std::shared_ptr<AbstractLQPNode>& parent);
  void clear_parents();

  /**
   * @pre this has a parent
   * @return whether this is its parents left or right child.
   */
  LQPChildSide get_child_side(const std::shared_ptr<AbstractLQPNode>& parent) const;

  /**
   * @returns {get_child_side(parents()[0], ..., get_child_side(parents()[n-1])}
   */
  std::vector<LQPChildSide> get_child_sides() const;

  std::shared_ptr<AbstractLQPNode> left_child() const;
  void set_left_child(const std::shared_ptr<AbstractLQPNode>& left);

  std::shared_ptr<AbstractLQPNode> right_child() const;
  void set_right_child(const std::shared_ptr<AbstractLQPNode>& right);

  std::shared_ptr<AbstractLQPNode> child(LQPChildSide side) const;

  void set_child(LQPChildSide side, const std::shared_ptr<AbstractLQPNode>& child);
  // @}

  LQPNodeType type() const;

  // Returns whether this subtree is read only. Defaults to true - if a node makes modications, it has to override this
  virtual bool subtree_is_read_only() const;

  // Returns whether all tables in this subtree were validated
  bool subtree_is_validated() const;

  // @{
  /**
   * These functions provide access to statistics for this particular node.
   *
   * AbstractLQPNode::derive_statistics_from() calculates new statistics for this node as they would appear if
   * left_child and right_child WERE its children. This works for the actual children of this node during the lazy
   * initialization in get_statistics() as well as e.g. in an optimizer rule
   * that tries to reorder nodes based on some statistics. In that case it will call this function for all the nodes
   * that shall be reordered with the same reference node.
   *
   * Inheriting nodes are free to override AbstractLQPNode::derive_statistics_from().
   */
  void set_statistics(const std::shared_ptr<TableStatistics>& statistics);
  const std::shared_ptr<TableStatistics> get_statistics();
  virtual std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_child,
      const std::shared_ptr<AbstractLQPNode>& right_child = nullptr) const;
  // @}

  /**
   * @returns the names of the columns this node outputs without any alias added by this node
   */
  virtual const std::vector<std::string>& output_column_names() const;
  virtual const std::vector<ColumnOrigin>& output_column_origins() const;
  size_t output_column_count() const;

  std::optional<ColumnOrigin> find_column_origin_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const;
  ColumnOrigin get_column_origin_by_named_column_reference(const NamedColumnReference& named_column_reference) const;

  // has to be overriden if there is != 1 child
  ColumnOrigin get_column_origin_by_output_column_id(const ColumnID column_id) const;

  // has to be overriden by StoredTableNode
  virtual std::shared_ptr<const AbstractLQPNode> find_table_name_origin(const std::string& table_name) const;

  std::optional<ColumnID> find_output_column_id_by_column_origin(const ColumnOrigin& column_origin) const;
  ColumnID get_output_column_id_by_column_origin(const ColumnOrigin& column_origin) const;

  /**
   * Makes this nodes parents point to this node's left child
   * Unties this node's child from this node
   *
   * @pre this has no right child
   */
  void remove_from_tree();

  /**
   * Replaces 'this' node with @param replacement_node node.
   * @pre replacement_node has neither parent nor children
   */
  void replace_with(const std::shared_ptr<AbstractLQPNode>& replacement_node);

  /**
   * Sets the table alias for this subtree, see _table_alias for details.
   * This is not part of the constructor because it is only used in SQLTranslator::_translate_table_ref.
   */
  void set_alias(const std::optional<std::string>& table_alias);

  // @{
  /**
   * Functions for debugging purposes.
   */

  /**
   * Prints this node and all its descendants formatted as a tree
   */
  void print(std::ostream& out = std::cout) const;

  /**
   * Returns a string describing this node, but nothing about its children.
   */
  virtual std::string description() const = 0;

  /**
   * Generate a name for a column that contains all aliases it went through as well as the name of the table that it
   * originally came from, if any
   */
  virtual std::string get_verbose_column_name(ColumnID column_id) const;

  /**
   * @returns {get_verbose_column_name(0), ..., get_verbose_column_name(n-1)}
   */
  std::vector<std::string> get_verbose_column_names() const;
  // @}

 protected:
  // creates a DEEP copy of the other LQP node. Used for reusing LQPs, e.g., in views.
  virtual std::shared_ptr<AbstractLQPNode> _deep_copy_impl(const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const = 0;

  /**
   * In derived nodes, clear all data that depends on children and only set it lazily on request (see, e.g.
   * output_column_names())
   */
  virtual void _on_child_changed() {}

  // Used to easily differentiate between node types without pointer casts.
  LQPNodeType _type;

  /**
   * Each subtree can be a subselect. A subselect can be given an alias:
   * SELECT y.* FROM (SELECT * FROM x) AS y
   * The alias applies to all nodes above the node where it is set until a new alias is set
   */
  std::optional<std::string> _table_alias;

  // mutable, so it can be lazily initialized in output_column_origins() overrides
  mutable std::optional<std::vector<ColumnOrigin>> _output_column_origins;

  /**
   * If named_column_reference.table_name is the alias set for this subtree, remove the table_name so that we
   * only operate on the column name. If an alias for this subtree is set, but this reference does not match
   * it, the reference cannot be resolved (see knows_table) and std::nullopt is returned.
   */
  virtual std::optional<NamedColumnReference> _resolve_local_column_prefix(
      const NamedColumnReference& named_column_reference) const;

  std::shared_ptr<LQPExpression> _adjust_expression_to_lqp(const std::shared_ptr<LQPExpression>& expression,
                                                           const std::shared_ptr<AbstractLQPNode>& current_lqp, const std::shared_ptr<AbstractLQPNode>& lqp_copy) const;

 private:
  std::vector<std::weak_ptr<AbstractLQPNode>> _parents;
  std::array<std::shared_ptr<AbstractLQPNode>, 2> _children;
  std::shared_ptr<TableStatistics> _statistics;

  /**
   * Reset statistics, call _on_child_changed() for node specific behaviour and call _child_changed() on parents
   */
  void _child_changed();

  /**
   * Actual impl of AbstractLQPNode::print(). AbstractLQPNode::print() just creates the `levels` and `id_by_node`
   * instances used during the recursion.
   */
  void _print_impl(std::ostream& out, std::vector<bool>& levels,
                   std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t>& id_by_node,
                   size_t& id_counter) const;

  // @{
  /**
   * Add or remove a parent without manipulating this parents child ptr. For internal usage in set_left_child(),
   * set_right_child(), remove_parent
   */
  void _remove_parent_pointer(const std::shared_ptr<AbstractLQPNode>& parent);
  void _add_parent_pointer(const std::shared_ptr<AbstractLQPNode>& parent);
  // @}
};

}  // namespace opossum
