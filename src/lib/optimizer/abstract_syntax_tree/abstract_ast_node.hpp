#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace opossum {

struct ColumnID;
class Expression;
class TableStatistics;

enum class ASTNodeType {
  Aggregate,
  Delete,
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
  Mock
};

enum class ASTChildSide { Left, Right };

struct NamedColumnReference {
  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;

  std::string as_string() const;
};

/**
 * Base class for a Node in the Abstract Syntax Tree on which the Query Optimization is performed on.
 * Usually produced by SQLToASTTranslator and turned into actual Operators by ASTToOperatorTranslator.
 *
 * Design decision:
 * We decided to have mutable Nodes for now.
 * By that we can apply rules without creating new nodes for every optimization rule.
 */
class AbstractASTNode : public std::enable_shared_from_this<AbstractASTNode> {
 public:
  explicit AbstractASTNode(ASTNodeType node_type);

  /**
   * Returns whether this node shall be considered by the optimizer or not.
   */
  virtual bool is_optimizable() const;

  /**
   * @pre this has a parent
   * @return whether this is its parents left or right child.
   */
  ASTChildSide get_child_side() const;

  // @{
  /**
   * Set and get the parent/children of this node.
   *
   * The _parent is implicitly set in set_left_child/set_right_child.
   * For un-setting _parent use clear_parent().
   *
   * set_child() is a shorthand for set_left_child() or set_right_child(), useful if the side is a runtime value
   */
  std::shared_ptr<AbstractASTNode> parent() const;
  void clear_parent();

  const std::shared_ptr<AbstractASTNode>& left_child() const;
  void set_left_child(const std::shared_ptr<AbstractASTNode>& left);

  const std::shared_ptr<AbstractASTNode>& right_child() const;
  void set_right_child(const std::shared_ptr<AbstractASTNode>& right);

  void set_child(ASTChildSide side, const std::shared_ptr<AbstractASTNode>& child);
  // @}

  ASTNodeType type() const;

  // @{
  /**
   * These functions provide access to statistics for this particular node.
   *
   * AbstractASTNode::derive_statistics_from() calculates new statistics for this node as they would appear if
   * left_child and right_child WERE its children. This works for the actual children of this node during the lazy
   * initialization in get_statistics() as well as e.g. in an optimizer rule
   * that tries to reorder nodes based on some statistics. In that case it will call this function for all the nodes
   * that shall be reordered with the same reference node.
   *
   * Inheriting nodes are free to override AbstractASTNode::derive_statistics_from().
   */
  void set_statistics(const std::shared_ptr<TableStatistics>& statistics);
  const std::shared_ptr<TableStatistics> get_statistics();
  virtual std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractASTNode>& left_child,
      const std::shared_ptr<AbstractASTNode>& right_child = nullptr) const;
  // @}

  virtual const std::vector<std::string>& output_column_names() const;

  /**
   * This function is public for testing purposes only, otherwise should only be used internally
   *
   * Every node will output a list of column and all nodes except StoredTableNode take a list of columns as input from
   * their predecessor.
   */
  virtual const std::vector<ColumnID>& output_column_id_to_input_column_id() const;

  size_t output_col_count() const;

  // @{
  /**
   * These functions are part of the "ColumnID Resolution". See SQLToAstTranslator class comment for a general
   * discussion
   * on this.
   *
   * AbstractASTNode::find_column_id_by_named_column_reference() looks for the @param named_column_reference in the
   * columns that this node outputs. If it can find it, the corresponding ColumnID will be returned, otherwise std::nullopt
   * is returned.
   *
   * AbstractASTNode::get_column_id_by_named_column_reference() is more strict and will fail if the
   * @param named_column_reference cannot be found.
   *
   * NOTE: If a node outputs a column "x" but ALIASes it as, say, "y", these two functions will only find
   * ColumnIdentifier{"y", std::nullopt} and NEITHER ColumnIdentifier{"x", "table_name"} nor
   * ColumnIdentifier{"y", "table_name"}
   *
   * NOTE: These functions will possibly result in a full recursive traversal of the ancestors of this node.
   *
   * Find more information in our blog: https://medium.com/hyrise/the-gentle-art-of-referring-to-columns-634f057bd810
   */
  ColumnID get_column_id_by_named_column_reference(const NamedColumnReference& named_column_reference) const;
  virtual std::optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const;
  // @}

  /**
   * Checks whether this node or any of its ancestors retrieve the table @param table_name from the StorageManager
   * or as an alias of a subquery.
   *
   * Used especially to figure out which of the children of a Join is referenced.
   *
   * The standard requires subqueries (known as derived tables) to have an alias. The original name(s) of the table(s)
   * that became part of that subselect are not accessible anymore once an alias is given.
   *
   *        <table reference> ::=
   *            <table name> [ [ AS ] <correlation name>
   *                [ <left paren> <derived column list> <right paren> ] ]
   *          | <derived table> [ AS ] <correlation name>
   *                [ <left paren> <derived column list> <right paren> ]
   *          | <joined table>
   */
  virtual bool knows_table(const std::string& table_name) const;

  /**
   * This function is part of the "ColumnID Resolution". See SQLToAstTranslator class comment for a general discussion
   * on this.
   *
   * Returns all ColumnIDs of this node, i.e., a vector with [0, output_col_count)
   */
  virtual std::vector<ColumnID> get_output_column_ids() const;

  /**
   * This function is part of the "ColumnID Resolution". See SQLToAstTranslator class comment for a general discussion
   * on this.
   *
   * Returns all ColumnIDs of this node that belong to a table. Used for resolving wildcards in queries like
   * `SELECT T1.*, T2.a FROM T1, T2`
   *
   * @param table_name can be an alias.
   */
  virtual std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const;

  /**
   * If a node only has a left child, it is possible to remove this node from the tree, connecting this
   * node's child with this node's parent.
   * Fails if this node has two children
   */
  void remove_from_tree();

  /**
   * Replaces @param node_to_replace with this node.
   * Fails if this node was already part of a tree, i.e. has a parent or children
   */
  void replace_in_tree(const std::shared_ptr<AbstractASTNode>& node_to_replace);

  /**
   * Sets the table alias for this subtree, see _table_alias for details.
   * This is not part of the constructor because it is only used in SQLToASTTranslator::_translate_table_ref.
   */
  void set_alias(const std::optional<std::string>& table_alias);

  void print(std::ostream& out = std::cout, std::vector<bool> levels = {}) const;
  virtual std::string description() const = 0;

 protected:
  virtual void _on_child_changed() {}

  // Used to easily differentiate between node types without pointer casts.
  ASTNodeType _type;

  // Each subtree can be a subselect. A subselect can be given an alias:
  // SELECT y.* FROM (SELECT * FROM x) AS y
  // The alias applies to all nodes above the node where it is set until a new alias is set
  std::optional<std::string> _table_alias;

  // If named_column_reference.table_name is the alias set for this subtree, remove the table_name so that we
  // only operatore on the column name. If an alias for this subtree is set, but this reference does not match
  // it, the reference cannot be resolved (see knows_table) and std::nullopt is returned.
  std::optional<NamedColumnReference> _resolve_local_alias(const NamedColumnReference& named_column_reference) const;

 private:
  std::weak_ptr<AbstractASTNode> _parent;
  std::shared_ptr<AbstractASTNode> _left_child;
  std::shared_ptr<AbstractASTNode> _right_child;

  std::shared_ptr<TableStatistics> _statistics;
};

}  // namespace opossum
