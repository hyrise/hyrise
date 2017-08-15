#pragma once

#include <iostream>
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
   * The naive approach of befriending std::make_shared does not work here, as the implementation of std::make_shared is
   * compiler-specific and usually relies on internal impl-classes.
   * (e.g.:
   * https://stackoverflow.com/questions/3541632/using-make-shared-with-a-protected-constructor-abstract-interface)
   * We refrained from using the suggested pass-key-idiom as it only increases complexity but does not help removing a
   * public constructor.
   *
   * In the end we debated between creating the shared_ptr explicitly in the factory methods
   * and making the constructor public. For now we decided to follow the latter.
   *
   * We highly suggest using one of the create_*-methods over using this constructor.
   */
  ExpressionNode(const ExpressionType type, const AllTypeVariant& value,
                 const std::vector<std::shared_ptr<ExpressionNode>>& expression_list, const std::string& name,
                 const ColumnID column_id, const optional<std::string>& alias = {});

  /*
   * Factory Methods to create Expressions of specific type
   */
  static std::shared_ptr<ExpressionNode> create_expression(const ExpressionType type);

  static std::shared_ptr<ExpressionNode> create_column_reference(const ColumnID column_id,
                                                                 const optional<std::string>& alias = {});

  static std::vector<std::shared_ptr<ExpressionNode>> create_column_references(const std::vector<ColumnID>& column_ids,
                                                                               const std::vector<std::string>& aliases);

  /**
   * A literal can have an alias in order to allow queries like `SELECT 1 as one FROM t`.
   */
  static std::shared_ptr<ExpressionNode> create_literal(const AllTypeVariant& value,
                                                        const optional<std::string>& alias = {});

  static std::shared_ptr<ExpressionNode> create_parameter(const AllTypeVariant& value);

  static std::shared_ptr<ExpressionNode> create_function_reference(
      const std::string& function_name, const std::vector<std::shared_ptr<ExpressionNode>>& expression_list,
      const optional<std::string>& alias);

  static std::shared_ptr<ExpressionNode> create_binary_operator(ExpressionType type,
                                                                const std::shared_ptr<ExpressionNode>& left,
                                                                const std::shared_ptr<ExpressionNode>& right,
                                                                const optional<std::string>& alias = {});

  static std::shared_ptr<ExpressionNode> create_select_all();

  /*
   * Helper methods for Expression Trees
   */
  const std::weak_ptr<ExpressionNode> parent() const;
  void clear_parent();

  const std::shared_ptr<ExpressionNode> left_child() const;
  void set_left_child(const std::shared_ptr<ExpressionNode>& left);

  const std::shared_ptr<ExpressionNode> right_child() const;
  void set_right_child(const std::shared_ptr<ExpressionNode>& right);

  const ExpressionType type() const;

  /*
   * Methods for debug printing
   */
  void print(const uint32_t level = 0, std::ostream& out = std::cout) const;

  const std::string description() const;

  // Is +, -, * (arithmetic usage, not SELECT * FROM), /, %, ^
  bool is_arithmetic_operator() const;

  // Returns true if the expression is a literal or column reference.
  bool is_operand() const;

  // Returns true if the expression requires two children.
  bool is_binary_operator() const;

  /*
   * Getters
   */
  const ColumnID column_id() const;

  const std::string& name() const;

  const optional<std::string>& alias() const;

  const AllTypeVariant value() const;

  const std::vector<std::shared_ptr<ExpressionNode>>& expression_list() const;

  // Expression as string
  std::string to_string() const;

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

  // a name, which could be a function name
  const std::string _name;
  // a column that might be referenced
  const ColumnID _column_id;
  // an alias, used for ColumnReferences, Selects, FunctionReferences
  const optional<std::string> _alias;

  std::weak_ptr<ExpressionNode> _parent;
  std::shared_ptr<ExpressionNode> _left_child;
  std::shared_ptr<ExpressionNode> _right_child;
};

}  // namespace opossum
