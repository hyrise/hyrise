#pragma once

#include <array>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "lqp_column_reference.hpp"
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

// Describes the parent of a Node and which of the parent's children a node is
struct LQPParentRelation {
  std::shared_ptr<AbstractLQPNode> parent;
  LQPChildSide child_side{LQPChildSide::Left};
};

struct QualifiedColumnName {
  QualifiedColumnName(const std::string& column_name, const std::optional<std::string>& table_name =
                                                          std::nullopt);  // NOLINT - Implicit conversion is intended

  std::string as_string() const;

  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;
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
  std::shared_ptr<AbstractLQPNode> deep_copy() const;

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

  /**
   * @return {{parents()[0], get_child_sides()[0]}, ..., {parents()[n-1], get_child_sides()[n-1]}}
   */
  std::vector<LQPParentRelation> parent_relations() const;

  /**
   * Same as parents().size(), but avoids locking all parent pointers
   */
  size_t parent_count() const;

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

  // Returns whether this subtree is read only. Defaults to true - if a node makes modifications, it has to override
  // this
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

  /**
   * @returns the ColumnReferences of the columns this node outputs
   */
  virtual const std::vector<LQPColumnReference>& output_column_references() const;

  /**
   * @return the number of Columns this node outputs. Same as output_column_names().size()
   */
  size_t output_column_count() const;

  // @{
  /**
   * Name resolution for Columns and TableNames.
   */

  /**
   * @param qualified_column_name Must not be ambiguous in this subtree
   * @return The ColumnReference of the qualified_column_name if it can be resolved in this subtree,
   *         std::nullopt otherwise.
   */
  std::optional<LQPColumnReference> find_column(const QualifiedColumnName& qualified_column_name) const;

  /**
   * Convenience method for (*find_column()), DebugAssert()s that the
   * qualified_column_name could be resolved
   */
  LQPColumnReference get_column(const QualifiedColumnName& qualified_column_name) const;

  /**
   * @return the StoredTableNode that is called table_name or any that carries it as an alias in this subtree.
   *         nullptr if the no such node exists.
   */
  virtual std::shared_ptr<const AbstractLQPNode> find_table_name_origin(const std::string& table_name) const;
  // @}

  /**
   * @return The leftmost output ColumnID that stems from column_reference, or std::nullopt if none does
   */
  std::optional<ColumnID> find_output_column_id(const LQPColumnReference& column_reference) const;

  /**
   * Convenience for *find_output_column_id(), DebugAssert()s that the column_reference could be resolved
   */
  ColumnID get_output_column_id(const LQPColumnReference& column_reference) const;

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

  /**
   * @defgroup Utilities for deep_copy()
   * @{
   */
  /**
   * @return the @param expression (you will probably want to pass in a deep_copy), with all ColumnReferences pointing
   * to their equivalent in a deep_copy()ed LQP
   */
  static std::shared_ptr<LQPExpression> adapt_expression_to_different_lqp(
      const std::shared_ptr<LQPExpression>& expression, const std::shared_ptr<AbstractLQPNode>& original_lqp,
      const std::shared_ptr<AbstractLQPNode>& copied_lqp);

  /**
   * @param copied_lqp must be a deep copy of original_lqp
   * @param column_reference must be a ColumnReference original_lqp node outputs
   * @return the ColumnReference equivalent to column_reference within the copied_lqp subtree
   */
  static LQPColumnReference adapt_column_reference_to_different_lqp(
      const LQPColumnReference& column_reference, const std::shared_ptr<AbstractLQPNode>& original_lqp,
      const std::shared_ptr<AbstractLQPNode>& copied_lqp);
  /**
   * @}
   */

  /**
   * @defgroup Comparing two LQPs
   * shallow_equals() compares only the nodes without considering the children, find_first_subplan_mismatch() will compare the entire
   * sub plan
   * @{
   */
  virtual bool shallow_equals(const AbstractLQPNode& rhs) const = 0;

  /**
   * Perform a deep equality check of this LQP with another. Floating point numbers will be compared allowing a small
   * absolute offset.
   * @return std::nullopt if the LQPs were equal. A pair of a node in this LQP and a node in the rhs LQP that were first
   *         discovered to differ.
   */
  std::optional<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>
  find_first_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& rhs) const;
  // @}

 protected:
  // Holds the actual implementation of deep_copy
  using PreviousCopiesMap =
      std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>;
  std::shared_ptr<AbstractLQPNode> _deep_copy(PreviousCopiesMap& previous_copies) const;

  /**
   * Override and create a DEEP copy of this LQP node. Used for reusing LQPs, e.g., in views.
   * @param left_child and @param right_child are deep copies of the left and right child respectively, used for deep-copying
   * ColumnReferences
   */
  virtual std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const = 0;

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

  // mutable, so it can be lazily initialized in output_column_references() overrides
  mutable std::optional<std::vector<LQPColumnReference>> _output_column_references;

  /**
   * If qualified_column_name.table_name is the alias set for this subtree, remove the table_name so that we
   * only operate on the column name. If an alias for this subtree is set, but qualified_column_name.table_name does not
   * match it, the reference cannot be resolved (see knows_table) and std::nullopt is returned.
   */
  virtual std::optional<QualifiedColumnName> _resolve_local_table_name(
      const QualifiedColumnName& qualified_column_name) const;

  /** Utility to compare vectors of Expressions from different LQPs */
  static bool _equals(const AbstractLQPNode& lqp_left,
                      const std::vector<std::shared_ptr<LQPExpression>>& expressions_left,
                      const AbstractLQPNode& lqp_right,
                      const std::vector<std::shared_ptr<LQPExpression>>& expressions_right);

  /** Utility to compare two Expressions from different LQPs */
  static bool _equals(const AbstractLQPNode& lqp_left, const std::shared_ptr<const LQPExpression>& expression_left,
                      const AbstractLQPNode& lqp_right, const std::shared_ptr<const LQPExpression>& expression_right);

  /** Utility to compare vectors of LQPColumnReferences from different LQPs */
  static bool _equals(const AbstractLQPNode& lqp_left, const std::vector<LQPColumnReference>& column_references_left,
                      const AbstractLQPNode& lqp_right, const std::vector<LQPColumnReference>& column_references_right);

  /** Utility to compare two LQPColumnReferences from different LQPs */
  static bool _equals(const AbstractLQPNode& lqp_left, const LQPColumnReference& column_reference_left,
                      const AbstractLQPNode& lqp_right, const LQPColumnReference& column_reference_right);

 private:
  std::vector<std::weak_ptr<AbstractLQPNode>> _parents;
  std::array<std::shared_ptr<AbstractLQPNode>, 2> _children;
  std::shared_ptr<TableStatistics> _statistics;

  /**
   * Reset statistics, call _on_child_changed() for node specific behaviour and call _child_changed() on parents
   */
  void _child_changed();

  static std::optional<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>
  _find_first_subplan_mismatch_impl(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                    const std::shared_ptr<const AbstractLQPNode>& rhs);

  // @{
  /**
   * Add or remove a parent without manipulating this parents child ptr. For internal usage in set_left_child(),
   * set_right_child(), remove_parent
   */
  void _remove_parent_pointer(const std::shared_ptr<AbstractLQPNode>& parent);
  void _add_parent_pointer(const std::shared_ptr<AbstractLQPNode>& parent);
  // @}
};

/**
 * LQP node types should derive from this in order to enable the <NodeType>::make() function that allows for a clean
 * notation when building LQPs via code by allowing to pass in a nodes child(ren) as the last argument(s).
 *
 * const auto input_lqp =
 * PredicateNode::make(_mock_node_a, PredicateCondition::Equals, 42,
 *   PredicateNode::make(_mock_node_b, PredicateCondition::GreaterThan, 50,
 *     PredicateNode::make(_mock_node_b, PredicateCondition::GreaterThan, 40,
 *       ProjectionNode::make_pass_through(
 *         PredicateNode::make(_mock_node_a, PredicateCondition::GreaterThanEquals, 90,
 *           PredicateNode::make(_mock_node_c, PredicateCondition::LessThan, 500,
 *             _mock_node))))));
 */
template <typename DerivedNode>
class EnableMakeForLQPNode {
 public:
  template <int N, typename... Ts>
  using NthTypeOf = typename std::tuple_element<N, std::tuple<Ts...>>::type;

  template <typename... Args>
  static std::shared_ptr<DerivedNode> make(Args&&... args) {
    // clang-format off

    // - using nesting instead of && because both sides of the && would need to be valid
    // - redundant else paths instead of one fallthrough at the end, because it too, needs to be valid.
    if constexpr (sizeof...(Args) > 0) {
      if constexpr (std::is_convertible_v<NthTypeOf<sizeof...(Args)-1, Args...>, std::shared_ptr<AbstractLQPNode>>) {
        auto args_tuple = std::forward_as_tuple(args...);
        if constexpr (sizeof...(Args) > 1) {
          if constexpr (std::is_convertible_v<NthTypeOf<sizeof...(Args)-2, Args...>, std::shared_ptr<AbstractLQPNode>>) {  // NOLINT - too long, but better than breaking
            // last two arguments are shared_ptr<AbstractLQPNode>
            auto node = make_impl(args_tuple, std::make_index_sequence<sizeof...(Args) - 2>());
            node->set_left_child(std::get<sizeof...(Args) - 2>(args_tuple));
            node->set_right_child(std::get<sizeof...(Args) - 1>(args_tuple));
            return node;
          } else {
            // last argument is shared_ptr<AbstractLQPNode>
            auto node = make_impl(args_tuple, std::make_index_sequence<sizeof...(Args)-1>());
            node->set_left_child(std::get<sizeof...(Args)-1>(args_tuple));
            return node;
          }
        } else {
          // last argument is shared_ptr<AbstractLQPNode>
          auto node = make_impl(args_tuple, std::make_index_sequence<sizeof...(Args)-1>());
          node->set_left_child(std::get<sizeof...(Args)-1>(args_tuple));
          return node;
        }
      } else {
        // no shared_ptr<AbstractLQPNode> was passed at the end
        return make_impl(std::forward_as_tuple(args...), std::make_index_sequence<sizeof...(Args)-0>());
      }
    } else {
      // no shared_ptr<AbstractLQPNode> was passed at the end
      return make_impl(std::forward_as_tuple(args...), std::make_index_sequence<sizeof...(Args)-0>());
    }
    // clang-format on
  }

 private:
  template <class Tuple, size_t... I>
  static std::shared_ptr<DerivedNode> make_impl(const Tuple& constructor_arguments,
                                                std::index_sequence<I...> num_constructor_args) {
    return std::make_shared<DerivedNode>(std::get<I>(constructor_arguments)...);
  }
};

}  // namespace opossum
