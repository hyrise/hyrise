#pragma once

#include <vector>

#include "enable_make_for_lqp_node.hpp"
#include "lqp_utils.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;

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

enum class LQPInputSide { Left, Right };

// Describes the output of a Node and which of the output's inputs this Node is
struct LQPOutputRelation {
  std::shared_ptr<AbstractLQPNode> output;
  LQPInputSide input_side{LQPInputSide::Left};
};

class AbstractLQPNode : public std::enable_shared_from_this<AbstractLQPNode> {
 public:
  explicit AbstractLQPNode(const LQPNodeType node_type);
  virtual ~AbstractLQPNode() = default;

  /**
   * @defgroup Access the outputs/inputs
   *
   * The outputs are implicitly set and removed in set_left_input()/set_right_input()/set_input().
   * Design decision: If you delete a node, you explicitly need to call remove_output() on its input.
   *
   * set_input() is a shorthand for set_left_input() or set_right_input(), useful if the side is a runtime value.
   * @{
   */
  std::shared_ptr<AbstractLQPNode> left_input() const;
  std::shared_ptr<AbstractLQPNode> right_input() const;
  std::shared_ptr<AbstractLQPNode> input(LQPInputSide side) const;
  void set_left_input(const std::shared_ptr<AbstractLQPNode>& left);
  void set_right_input(const std::shared_ptr<AbstractLQPNode>& right);
  void set_input(LQPInputSide side, const std::shared_ptr<AbstractLQPNode>& input);

  size_t input_count() const;

  /**
   * @pre this has has @param output as an output
   * @return whether this is the left or right input in the specified output.
   */
  LQPInputSide get_input_side(const std::shared_ptr<AbstractLQPNode>& output) const;

  /**
   * @returns {get_output_side(outputs()[0], ..., get_output_side(outputs()[n-1])}
   */
  std::vector<LQPInputSide> get_input_sides() const;

  /**
   * Locks all outputs (as they are stored in weak_ptrs) and returns them as shared_ptrs
   */
  std::vector<std::shared_ptr<AbstractLQPNode>> outputs() const;

  void remove_output(const std::shared_ptr<AbstractLQPNode>& output);
  void clear_outputs();

  /**
   * @return {{outputs()[0], get_input_sides()[0]}, ..., {outputs()[n-1], get_input_sides()[n-1]}}
   */
  std::vector<LQPOutputRelation> output_relations() const;

  /**
   * Same as outputs().size(), but avoids locking all output pointers
   */
  size_t output_count() const;
  /** @} */

  /**
   * @return A deep copy of the LQP this Node is the root of
   */
  std::shared_ptr<AbstractLQPNode> deep_copy() const;

  /**
   * Compare this node with another, without comparing inputs. Prefer lqp_find_subplan_mismatch() over this function.
   * @param node_mapping    Mapping from nodes in this node's input plans to corresponding nodes in the input plans of
   *                        rhs
   */
  bool shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const;

  virtual const std::vector<std::shared_ptr<AbstractExpression>>& output_column_expressions() const;

  std::optional<ColumnID> find_column(const AbstractExpression& expression) const;

  const LQPNodeType type;

 protected:
  virtual std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping & node_mapping) const = 0;
  virtual bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const = 0;

 private:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(LQPNodeMapping & node_mapping) const;
  std::shared_ptr<AbstractLQPNode> _shallow_copy(LQPNodeMapping & node_mapping) const;

  /**
   * @{
   * For internal usage in set_left_input(), set_right_input(), set_input(), remove_output()
   * Add or remove a output without manipulating this output's input ptr.
   */
  void _add_output_pointer(const std::shared_ptr<AbstractLQPNode>& output);
  void _remove_output_pointer(const std::shared_ptr<AbstractLQPNode>& output);
  /** @} */

  std::vector<std::weak_ptr<AbstractLQPNode>> _outputs;
  std::array<std::shared_ptr<AbstractLQPNode>, 2> _inputs;
};

}  // namespace opossum


//#pragma once
//
//#include <array>
//#include <iostream>
//#include <memory>
//#include <optional>
//#include <string>
//#include <unordered_map>
//#include <vector>
//
//#include "expression/abstract_expression.hpp"
//#include "enable_make_for_lqp_node.hpp"
//#include "plan_column_definition.hpp"
//#include "lqp_column_reference.hpp"
//#include "types.hpp"
//#include "sql/sql_identifier.hpp"
//
//namespace opossum {
//
//class TableStatistics;
//
//enum class LQPNodeType {
//  Aggregate,
//  CreateView,
//  Delete,
//  DropView,
//  DummyTable,
//  Insert,
//  Join,
//  Limit,
//  Predicate,
//  Projection,
//  Root,
//  ShowColumns,
//  ShowTables,
//  Sort,
//  StoredTable,
//  Update,
//  Union,
//  Validate,
//  Mock
//};
//
//enum class LQPInputSide { Left, Right };
//
//// Describes the output of a Node and which of the output's inputs a node is
//struct LQPOutputRelation {
//  std::shared_ptr<AbstractLQPNode> output;
//  LQPInputSide input_side{LQPInputSide::Left};
//};
//
///**
// * Base class for a Node in the Logical Query Plan.
// *
// * The Logical Query Plan (abbreviated LQP) is a Directional Acyclic Graph (DAG) with each node having 0..2 incoming
// * edges and 0..n outgoing edges. The adjacent nodes on incoming edges are called "inputs" and those on the outgoing
// * edges "outputs". The direction of the edges models data flow (with "data" being Tables) where inputs produce the
// * input data their outputs operate on. A Node represents an "Operation" such as the application of a Predicate or a
// * Join.
// *
// * The LQP is created by the SQLTranslator and can optionally be further processed by the Optimizer.
// * The LQPTranslator creates the actual executable Operator-DAG. We are very careful that the LQP remains semantically
// * the same whether it was optimized or not.
// */
//class AbstractLQPNode : public std::enable_shared_from_this<AbstractLQPNode>, private Noncopyable {
// public:
//  explicit AbstractLQPNode(LQPNodeType node_type);
//
//  // Creates a deep copy
//  std::shared_ptr<AbstractLQPNode> deep_copy() const;
//
//  // @{
//  /**
//   * Set and get the outputs/inputs of this node.
//   *
//   * The _outputs are implicitly set in set_left_input/set_right_input.
//   * For removing outputs use remove_output() or clear_outputs().
//   *
//   * set_input() is a shorthand for set_left_input() or set_right_input(), useful if the side is a runtime value
//   */
//
//  /**
//   * Locks all outputs and returns them as shared_ptrs
//   */
//  std::vector<std::shared_ptr<AbstractLQPNode>> outputs() const;
//
//  /**
//   * @return {{outputs()[0], get_input_sides()[0]}, ..., {outputs()[n-1], get_input_sides()[n-1]}}
//   */
//  std::vector<LQPOutputRelation> output_relations() const;
//
//  /**
//   * Same as outputs().size(), but avoids locking all output pointers
//   */
//  size_t output_count() const;
//
//  void remove_output(const std::shared_ptr<AbstractLQPNode>& output);
//  void clear_outputs();
//
//  /**
//   * @pre this has has @param output as an output
//   * @return whether this is the left or right input in the specified output.
//   */
//  LQPInputSide get_input_side(const std::shared_ptr<AbstractLQPNode>& output) const;
//
//  /**
//   * @returns {get_output_side(outputs()[0], ..., get_output_side(outputs()[n-1])}
//   */
//  std::vector<LQPInputSide> get_input_sides() const;
//
//  std::shared_ptr<AbstractLQPNode> left_input() const;
//  void set_left_input(const std::shared_ptr<AbstractLQPNode>& left);
//
//  std::shared_ptr<AbstractLQPNode> right_input() const;
//  void set_right_input(const std::shared_ptr<AbstractLQPNode>& right);
//
//  std::shared_ptr<AbstractLQPNode> input(LQPInputSide side) const;
//
//  /**
//   * @returns the number of inputs
//   */
//  size_t input_count() const;
//
//  void set_input(LQPInputSide side, const std::shared_ptr<AbstractLQPNode>& input);
//  // @}
//
//  LQPNodeType type() const;
//
//  // Returns whether this subtree is read only. Defaults to true - if a node makes modifications, it has to override
//  // this
//  virtual bool subplan_is_read_only() const;
//
//  // Returns whether all tables in this subtree were validated
//  bool subplan_is_validated() const;
//
//  // @{
//  /**
//   * These functions provide access to statistics for this particular node.
//   *
//   * AbstractLQPNode::derive_statistics_from() calculates new statistics for this node as they would appear if
//   * left_input and right_input WERE its inputs. This works for the actual inputs of this node during the lazy
//   * initialization in get_statistics() as well as e.g. in an optimizer rule
//   * that tries to reorder nodes based on some statistics. In that case it will call this function for all the nodes
//   * that shall be reordered with the same reference node.
//   *
//   * Inheriting nodes are free to override AbstractLQPNode::derive_statistics_from().
//   */
//  void set_statistics(const std::shared_ptr<TableStatistics>& statistics);
//  const std::shared_ptr<TableStatistics> get_statistics();
//  virtual std::shared_ptr<TableStatistics> derive_statistics_from(
//      const std::shared_ptr<AbstractLQPNode>& left_input,
//      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const;
//  // @}
//
//  /**
//   * @returns the names of the columns this node outputs without any alias added by this node
//   */
//  virtual const std::vector<std::string>& output_column_names() const;
//
//  /**
//   * @returns the ColumnReferences of the columns this node outputs
//   */
//  virtual const std::vector<LQPColumnReference>& output_column_references() const;
//
//  /**
//   * @returns the Expressions that produce the Columns of this node. If a column is the result of the expressions of
//   *          multiple nodes, the entire, nested, Expression is returned
//   */
//  virtual const std::vector<std::shared_ptr<AbstractExpression>>& output_column_expressions() const;
//
//  /**
//   * @return the number of Columns this node outputs. Same as output_column_names().size()
//   */
//  size_t output_column_count() const;
//
//  // @{
//  /**
//   * Name resolution for Columns and TableNames.
//   */
//
//  /**
//   * @param qualified_column_name Must not be ambiguous in this subtree
//   * @return The ColumnReference of the qualified_column_name if it can be resolved in this subtree,
//   *         std::nullopt otherwise.
//   */
//  std::optional<LQPColumnReference> find_column(const QualifiedColumnName& qualified_column_name) const;
//
//  /**
//   * Convenience method for (*find_column()), DebugAssert()s that the
//   * qualified_column_name could be resolved
//   */
//  LQPColumnReference get_column(const QualifiedColumnName& qualified_column_name) const;
//
//  /**
//   * @return the StoredTableNode that is called table_name or any that carries it as an alias in this subtree.
//   *         nullptr if the no such node exists.
//   */
//  virtual std::shared_ptr<const AbstractLQPNode> find_table_name_origin(const std::string& table_name) const;
//  // @}
//
//  /**
//   * @return The ColumnReference that was created with @param expression, std::nullopt otherwise
//   */
//  std::optional<LQPColumnReference> find_column(const AbstractExpression& expression) const;
//
//  /**
//   * Convenience method for (*find_column()), DebugAssert()s that the qualified_column_name could be resolved
//   */
//  LQPColumnReference get_column(const AbstractExpression& expression) const;
//
//  /**
//   * @return The leftmost output ColumnID that stems from column_reference, or std::nullopt if none does
//   */
//  std::optional<ColumnID> find_output_column_id(const LQPColumnReference& column_reference) const;
//
//  /**
//   * Convenience for *find_output_column_id(), DebugAssert()s that the column_reference could be resolved
//   */
//  ColumnID get_output_column_id(const LQPColumnReference& column_reference) const;
//
//  /**
//   * Makes this nodes outputs point to this node's left input
//   * Unties this node's input from this node
//   *
//   * @pre this has no right input
//   */
//  void remove_from_tree();
//
//  /**
//   * Replaces 'this' node with @param replacement_node node.
//   * @pre replacement_node has neither output nor inputs
//   */
//  void replace_with(const std::shared_ptr<AbstractLQPNode>& replacement_node);
//
//  /**
//   * Sets the table alias for this subtree, see _table_alias for details.
//   * This is not part of the constructor because it is only used in SQLTranslator::_translate_table_ref.
//   */
//  void set_alias(const std::optional<std::string>& table_alias);
//
//  // @{
//  /**
//   * Functions for debugging purposes.
//   */
//
//  /**
//   * Prints this node and all its descendants formatted as a tree
//   */
//  void print(std::ostream& out = std::cout) const;
//
//  /**
//   * Returns a string describing this node, but nothing about its inputs.
//   */
//  virtual std::string description() const = 0;
//
//  /**
//   * Generate a name for a column that contains all aliases it went through as well as the name of the table that it
//   * originally came from, if any
//   */
//  virtual std::string get_verbose_column_name(ColumnID column_id) const;
//
//  /**
//   * @returns {get_verbose_column_name(0), ..., get_verbose_column_name(n-1)}
//   */
//  std::vector<std::string> get_verbose_column_names() const;
//  // @}
//
//  /**
//   * @defgroup Utilities for deep_copy()
//   * @{
//   */
//
//  /**
//   * @param copied_lqp must be a deep copy of original_lqp
//   * @param column_reference must be a ColumnReference original_lqp node outputs
//   * @return the ColumnReference equivalent to column_reference within the copied_lqp subtree
//   */
//  static LQPColumnReference adapt_column_reference_to_different_lqp(
//      const LQPColumnReference& column_reference, const AbstractLQPNode& original_lqp,
//      AbstractLQPNode& copied_lqp);
//  /**
//   * @}
//   */
//
//  /**
//   * @defgroup Comparing two LQPs
//   * shallow_equals() compares only the nodes without considering the inputs, find_first_subplan_mismatch() will compare the entire
//   * sub plan
//   * @{
//   */
//  virtual bool shallow_equals(const AbstractLQPNode& rhs) const = 0;
//
//  /**
//   * Perform a deep equality check of this LQP with another. Floating point numbers will be compared allowing a small
//   * absolute offset.
//   * @return std::nullopt if the LQPs were equal. A pair of a node in this LQP and a node in the rhs LQP that were first
//   *         discovered to differ.
//   */
//  std::optional<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>
//  find_first_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& rhs) const;
//  // @}
//
// protected:
//  // Holds the actual implementation of deep_copy
//  using PreviousCopiesMap =
//      std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>;
//  std::shared_ptr<AbstractLQPNode> _deep_copy(PreviousCopiesMap& previous_copies) const;
//
//  /**
//   * Override and create a DEEP copy of this LQP node. Used for reusing LQPs, e.g., in views.
//   * @param left_input and @param right_input are deep copies of the left and right input respectively, used for deep-copying
//   * ColumnReferences
//   */
//  virtual std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
//      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
//      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const = 0;
//
//  /**
//   * In derived nodes, clear all data that depends on inputs and only set it lazily on request (see, e.g.
//   * output_column_names())
//   */
//  virtual void _on_input_changed() {}
//
//  // Used to easily differentiate between node types without pointer casts.
//  LQPNodeType _type;
//
//  /**
//   * Each subtree can be a subselect. A subselect can be given an alias:
//   * SELECT y.* FROM (SELECT * FROM x) AS y
//   * The alias applies to all nodes above the node where it is set until a new alias is set
//   */
//  std::optional<std::string> _table_alias;
//
//  // mutable, so it can be lazily initialized in output_column_references() overrides
//  mutable std::optional<std::vector<LQPColumnReference>> _output_column_references;
//  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _output_column_expressions;
//  mutable std::optional<std::vector<QualifiedColumnName>> _qualified_output_column_names;
//
//  /**
//   * If qualified_column_name.table_name is the alias set for this subtree, remove the table_name so that we
//   * only operate on the column name. If an alias for this subtree is set, but qualified_column_name.table_name does not
//   * match it, the reference cannot be resolved (see knows_table) and std::nullopt is returned.
//   */
//  virtual std::optional<QualifiedColumnName> _resolve_local_table_name(
//      const QualifiedColumnName& qualified_column_name) const;
//
//  /** Utility to compare vectors of LQPColumnReferences from different LQPs */
//  static bool _equals(const AbstractLQPNode& lqp_left, const std::vector<LQPColumnReference>& column_references_left,
//                      const AbstractLQPNode& lqp_right, const std::vector<LQPColumnReference>& column_references_right);
//
//  /** Utility to compare two LQPColumnReferences from different LQPs */
//  static bool _equals(const AbstractLQPNode& lqp_left, const LQPColumnReference& column_reference_left,
//                      const AbstractLQPNode& lqp_right, const LQPColumnReference& column_reference_right);
//
// private:
//  std::vector<std::weak_ptr<AbstractLQPNode>> _outputs;
//  std::array<std::shared_ptr<AbstractLQPNode>, 2> _inputs;
//  std::shared_ptr<TableStatistics> _statistics;
//
//  /**
//   * Reset statistics, call _on_input_changed() for node specific behaviour and call _input_changed() on outputs
//   */
//  void _input_changed();
//
//  static std::optional<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>
//  _find_first_subplan_mismatch_impl(const std::shared_ptr<const AbstractLQPNode>& lhs,
//                                    const std::shared_ptr<const AbstractLQPNode>& rhs);
//
//  // @{
//  /**
//   * Add or remove a output without manipulating this outputs input ptr. For internal usage in set_left_input(),
//   * set_right_input(), remove_output
//   */
//  void _remove_output_pointer(const std::shared_ptr<AbstractLQPNode>& output);
//  void _add_output_pointer(const std::shared_ptr<AbstractLQPNode>& output);
//  // @}
//};
//
//}  // namespace opossum
