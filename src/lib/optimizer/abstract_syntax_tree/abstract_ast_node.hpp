#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common.hpp"

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
  ShowColumns,
  ShowTables,
  Sort,
  StoredTable,
  Update
};

struct NamedColumnReference {
  std::string column_name;
  optional<std::string> table_name;
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

  // @{
  /**
   * Set and get the parent/children of this node.
   *
   * The _parent is implicitly set in set_left_child/set_right_child.
   * For un-setting _parent use clear_parent().
   */
  std::shared_ptr<AbstractASTNode> parent() const;
  void clear_parent();

  const std::shared_ptr<AbstractASTNode> &left_child() const;
  void set_left_child(const std::shared_ptr<AbstractASTNode> &left);

  const std::shared_ptr<AbstractASTNode> &right_child() const;
  void set_right_child(const std::shared_ptr<AbstractASTNode> &right);
  // @}

  ASTNodeType type() const;

  void set_statistics(const std::shared_ptr<TableStatistics> &statistics);
  const std::shared_ptr<TableStatistics> get_statistics();
  virtual const std::shared_ptr<TableStatistics> get_statistics_from(
      const std::shared_ptr<AbstractASTNode> &other_node) const;

  virtual const std::vector<std::string> &output_column_names() const;

  /**
   * This function is public for testing purposes only, otherwise should only be used internally
   *
   * Every node will output a list of column and all nodes except StoredTableNode take a list of columns as input from
   * their predecessor.
   */
  virtual const std::vector<ColumnID> &output_column_id_to_input_column_id() const;

  size_t output_col_count() const;

  // @{
  /**
   * These functions are part of the "ColumnID Resolution". See SQLToAstTranslator class comment for a general
   * discussion
   * on this.
   *
   * AbstractASTNode::find_column_id_by_named_column_reference() looks for the @param named_column_reference in the
   * columns that this node outputs. If it can find it, the corresponding ColumnID will be returned, otherwise nullopt
   * is returned.
   *
   * AbstractASTNode::get_column_id_by_named_column_reference() is more strict and will fail if the
   * @param named_column_reference cannot be found.
   *
   * NOTE: As long as - TODO(anybody) - Subquery ALIASES are not supported only the StoredTableNode has to concern
   * itself with the ColumnIdentifierName::table_name, all other nodes are fine searching for
   * ColumnIdentifierName::
   *
   * NOTE: If a node outputs a column "x" but ALIASes it as, say, "y", these two functions will only find
   * ColumnIdentifier{"y", nullopt} and NEITHER ColumnIdentifier{"x", "table_name"} nor
   * ColumnIdentifier{"y", "table_name"}
   *
   * NOTE: These functions will possibly result in a full recursive traversal of the ancestors of this node.
   *
   * Find more information in our blog: https://medium.com/hyrise/the-gentle-art-of-referring-to-columns-634f057bd810
   */
  ColumnID get_column_id_by_named_column_reference(const NamedColumnReference &named_column_reference) const;
  virtual optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference &named_column_reference) const;
  // @}

  /**
   * Checks whether this node or any of its ancestors retrieve the table @param table_name from the StorageManager
   * (or - TODO(anybody) - once subquery ALIASES are implemented whether it or any of its ancestors assign an ALIAS
   * with the name  @param table_name.)
   *
   * Used especially to figure out which of the children of a Join is referenced.
   */
  virtual bool knows_table(const std::string &table_name) const;

  /**
   * This function is part of the "ColumnID Resolution". See SQLToAstTranslator class comment for a general discussion
   * on this.
   *
   * Returns all ColumnIDs of this node that belong to a table. Used for resolving wildcards in queries like
   * `SELECT T1.*, T2.a FROM T1, T2`
   *
   * @param table_name can be an alias.
   */
  virtual std::vector<ColumnID> get_output_column_ids_for_table(const std::string &table_name) const;

  void print(const uint32_t level = 0, std::ostream &out = std::cout) const;
  virtual std::string description() const = 0;

 protected:
  virtual void _on_child_changed() {}
  virtual const std::shared_ptr<TableStatistics> _gather_statistics() const;

  // Used to easily differentiate between node types without pointer casts.
  ASTNodeType _type;

 private:
  std::weak_ptr<AbstractASTNode> _parent;
  std::shared_ptr<AbstractASTNode> _left_child;
  std::shared_ptr<AbstractASTNode> _right_child;

  std::shared_ptr<TableStatistics> _statistics;
};

}  // namespace opossum
