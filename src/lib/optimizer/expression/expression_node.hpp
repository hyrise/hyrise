#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "common.hpp"
#include "types.hpp"

namespace opossum {

/**
 * The basic idea of this ExpressionNode is to have a representation of SQL Expressions within Hyrise and especially the
 * optimizer.
 *
 * Similar as for the AST we might have a tree of ExpressionNodes,
 * e.g. 'columnA = 5' would be represented as a root expression with the type ExpressionType::Equals and
 * two child nodes of types ExpressionType::ColumnReference and ExpressionType::Literal.
 *
 * For now we decided to have a single ExpressionNode without further specializations. This goes hand in hand with the
 * approach used in hsql::Expr.
 */
class ExpressionNode : public std::enable_shared_from_this<ExpressionNode> {
 public:
  /*
   * This constructor is meant for internal use only and therefor should be private.
   * However, in C++ one is not able to call std::make_shared with a private constructor.
   *
   * We debated between creating the shared_ptr explicitly and making the constructor public.
   * For now we decided to follow the latter.
   */
  ExpressionNode(const ExpressionType type, const AllTypeVariant& value,
                 const std::vector<std::shared_ptr<ExpressionNode>>& expression_list, const std::string& name,
                 const std::string& table, const std::string& alias);

  /*
   * Factory Methods to create Expressions of specific type
   */
  static std::shared_ptr<ExpressionNode> create_expression(const ExpressionType type);

  static std::shared_ptr<ExpressionNode> create_column_reference(const std::string& table_name,
                                                                 const std::string& column_name,
                                                                 const std::string& alias);

  static std::shared_ptr<ExpressionNode> create_literal(const AllTypeVariant& value);

  static std::shared_ptr<ExpressionNode> create_parameter(const AllTypeVariant& value);

  static std::shared_ptr<ExpressionNode> create_function_reference(
      const std::string& function_name, const std::vector<std::shared_ptr<ExpressionNode>>& expression_list,
      const std::string& alias);

  /*
   * Helper methods for Expression Trees
   */
  const std::weak_ptr<ExpressionNode>& parent() const;
  void clear_parent();

  const std::shared_ptr<ExpressionNode>& left_child() const;
  void set_left_child(const std::shared_ptr<ExpressionNode>& left);

  const std::shared_ptr<ExpressionNode>& right_child() const;
  void set_right_child(const std::shared_ptr<ExpressionNode>& right);

  const ExpressionType type() const;

  /*
   * Methods for debug printing
   */
  void print(const uint8_t level = 0) const;
  const std::string description() const;

  // Is +, -, *, /
  bool is_arithmetic_operator() const;

  /*
   * Getters
   */
  const std::string& table_name() const;

  const std::string& name() const;

  const std::string& alias() const;

  const AllTypeVariant value() const;

  const std::vector<std::shared_ptr<ExpressionNode>>& expression_list() const;

  // Expression as string, parse-able by Projection
  std::string to_expression_string() const;

 private:
  // the type of the expression
  const ExpressionType _type;
  // the value of an expression, e.g. of a Literal
  const AllTypeVariant _value;
  /*
   * A list of Expressions used in FunctionReferences and CASE Expressions.
   * Not sure if this is the perfect way to go forward, but this is how hsql::Expr handles this.
   *
   * In case there are at most two expressions in this list, one could replace this list with an additional layer in the
   * Expression hierarchy.
   * E.g. for CASE one could argue that the THEN case becomes the left child, whereas ELSE becomes the right child.
   */
  const std::vector<std::shared_ptr<ExpressionNode>> _expression_list;

  // a name, which could the column name or a function name
  const std::string _name;
  // a table name, only used for ColumnReferences
  const std::string _table;
  // an alias, used for ColumnReferences, Selects, FunctionReferences
  const std::string _alias;

  std::weak_ptr<ExpressionNode> _parent;
  std::shared_ptr<ExpressionNode> _left_child;
  std::shared_ptr<ExpressionNode> _right_child;
};

}  // namespace opossum
