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

class AbstractASTNode;

/**
 * The basic idea of Expressions is to have a unified representation of any SQL Expressions within Hyrise
 * and especially its optimizer.
 *
 * Expressions are structured as a binary tree
 * e.g. 'columnA = 5' would be represented as a root expression with the type ExpressionType::Equals and
 * two child nodes of types ExpressionType::ColumnIdentifier and ExpressionType::Literal.
 *
 * For now we decided to have a single Expression without further specializations. This goes hand in hand with the
 * approach used in hsql::Expr.
 */
class Expression : public std::enable_shared_from_this<Expression> {
 public:
  /*
   * This constructor is meant for internal use only and therefore should be private.
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
   *
   * Find more information in our blog: https://medium.com/hyrise/a-matter-of-self-expression-5fea2dd0a72
   */
  explicit Expression(ExpressionType type);

  // @{
  /**
   * Factory Methods to create Expressions of specific type
   */
  static std::shared_ptr<Expression> create_column(const ColumnID column_id,
                                                   const optional<std::string>& alias = nullopt);

  static std::vector<std::shared_ptr<Expression>> create_columns(
      const std::vector<ColumnID>& column_ids, const optional<std::vector<std::string>>& aliases = nullopt);

  // A literal can have an alias in order to allow queries like `SELECT 1 as one FROM t`.
  static std::shared_ptr<Expression> create_literal(const AllTypeVariant& value,
                                                    const optional<std::string>& alias = nullopt);

  static std::shared_ptr<Expression> create_value_placeholder(ValuePlaceholder value_placeholder);

  static std::shared_ptr<Expression> create_aggregate_function(
      AggregateFunction aggregate_function, const std::vector<std::shared_ptr<Expression>>& expression_list,
      const optional<std::string>& alias = nullopt);

  static std::shared_ptr<Expression> create_binary_operator(ExpressionType type,
                                                            const std::shared_ptr<Expression>& left,
                                                            const std::shared_ptr<Expression>& right,
                                                            const optional<std::string>& alias = nullopt);

  static std::shared_ptr<Expression> create_unary_operator(ExpressionType type,
                                                           const std::shared_ptr<Expression>& input,
                                                           const optional<std::string>& alias = nullopt);

  static std::shared_ptr<Expression> create_select_star(const optional<std::string>& table_name = {});
  // @}

  // @{
  /**
   * Helper methods for Expression Trees, set_left_child() and set_right_child() will set parent
   */
  const std::weak_ptr<Expression> parent() const;
  void clear_parent();

  const std::shared_ptr<Expression> left_child() const;
  void set_left_child(const std::shared_ptr<Expression>& left);

  const std::shared_ptr<Expression> right_child() const;
  void set_right_child(const std::shared_ptr<Expression>& right);
  // @}

  const ExpressionType type() const;

  /*
   * Methods for debug printing
   */
  void print(const uint32_t level = 0, std::ostream& out = std::cout) const;

  const std::string description() const;

  // @{
  /**
   * Check semantics of the expression
   */

  // Is arithmetic or logical operator
  bool is_operator() const;

  // Is +, -, * (arithmetic usage, not SELECT * FROM), /, %, ^
  bool is_arithmetic_operator() const;

  // Is >, >=, AND, OR, NOT
  bool is_logical_operator() const;

  // Returns true if the expression is a literal or column reference.
  bool is_operand() const;

  // Returns true if the expression requires two children.
  bool is_binary_operator() const;

  // Returns true if the expression requires one child.
  bool is_unary_operator() const;

  // Returns true if the expression is a NULL literal.
  bool is_null_literal() const;
  // @}

  // @{
  /**
   * Getters. Only call them if you are sure the type() has such a member
   */
  const ColumnID column_id() const;
  AggregateFunction aggregate_function() const;
  const AllTypeVariant value() const;
  const std::vector<std::shared_ptr<Expression>>& expression_list() const;
  ValuePlaceholder value_placeholder() const;
  // @}

  /**
   * Getters that can be called to check whether a member is set.
   */
  const optional<std::string>& table_name() const;
  const optional<std::string>& alias() const;

  void set_expression_list(const std::vector<std::shared_ptr<Expression>>& expression_list);

  void set_alias(const std::string& alias);

  /**
   * Returns the Expression as a string. Not meant to produce pretty outputs. Mostly intended to create column names
   * for SELECT lists with expressions: `SELECT a > 5 FROM ...`, here, the column name "a > 5" is generated using this
   * method. ColumnIDs need to be resolved to names and therefore need @param input_column_names.
   */
  std::string to_string(const std::vector<std::string>& input_column_names = {}) const;

  bool operator==(const Expression& rhs) const;

 private:
  // the type of the expression
  const ExpressionType _type;
  // the value of an expression, e.g. of a Literal
  optional<AllTypeVariant> _value;

  optional<AggregateFunction> _aggregate_function;

  /*
   * A list of Expressions used in FunctionIdentifiers and CASE Expressions.
   * Not sure if this is the perfect way to go forward, but this is how hsql::Expr handles this.
   *
   * In case there are at most two expressions in this list, one could replace this list with an additional layer in the
   * Expression hierarchy.
   * E.g. for CASE one could argue that the THEN case becomes the left child, whereas ELSE becomes the right child.
   */
  std::vector<std::shared_ptr<Expression>> _expression_list;

  optional<std::string> _table_name;

  // a column that might be referenced
  optional<ColumnID> _column_id;

  // an alias, used for ColumnReferences, Selects, FunctionIdentifiers
  optional<std::string> _alias;

  optional<ValuePlaceholder> _value_placeholder;

  // @{
  // Members for the tree structure
  std::weak_ptr<Expression> _parent;
  std::shared_ptr<Expression> _left_child;
  std::shared_ptr<Expression> _right_child;
  // @}
};

}  // namespace opossum
