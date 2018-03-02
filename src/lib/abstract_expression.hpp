#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * CRTP class forming the base class for LQPExpression and PQPExpression. CRTP so pointers to children have the
 * correct type et al. PQPExpressions use ColumnIDs to refer to Columns, LQPExpressions use LQPColumnReferences.
 *
 * The basic idea of Expressions is to have a unified representation of any SQL Expressions within Hyrise
 * and especially its optimizer.
 *
 * Expressions are structured as a binary tree
 * e.g. 'columnA = 5' would be represented as a root expression with the type ExpressionType::Equals and
 * two child nodes of types ExpressionType::Column and ExpressionType::Literal.
 *
 * For now we decided to have a single Expression without further specializations. This goes hand in hand with the
 * approach used in hsql::Expr.
 */
template <typename DerivedExpression>
class AbstractExpression : public std::enable_shared_from_this<DerivedExpression>, private Noncopyable {
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
  explicit AbstractExpression(ExpressionType type);

  // creates a DEEP copy of the other expression. Used for reusing LQPs, e.g., in views.
  std::shared_ptr<DerivedExpression> deep_copy() const;

  // @{
  /**
   * Factory Methods to create Expressions of specific type
   */
  // A literal can have an alias in order to allow queries like `SELECT 1 as one FROM t`.
  static std::shared_ptr<DerivedExpression> create_literal(const AllTypeVariant& value,
                                                           const std::optional<std::string>& alias = std::nullopt);

  static std::shared_ptr<DerivedExpression> create_value_placeholder(ValuePlaceholder value_placeholder);

  static std::shared_ptr<DerivedExpression> create_aggregate_function(
      AggregateFunction aggregate_function, const std::vector<std::shared_ptr<DerivedExpression>>& function_arguments,
      const std::optional<std::string>& alias = std::nullopt);

  static std::shared_ptr<DerivedExpression> create_binary_operator(
      ExpressionType type, const std::shared_ptr<DerivedExpression>& left,
      const std::shared_ptr<DerivedExpression>& right, const std::optional<std::string>& alias = std::nullopt);

  static std::shared_ptr<DerivedExpression> create_unary_operator(
      ExpressionType type, const std::shared_ptr<DerivedExpression>& input,
      const std::optional<std::string>& alias = std::nullopt);

  static std::shared_ptr<DerivedExpression> create_select_star(const std::optional<std::string>& table_name = {});

  // @}

  // @{
  /**
   * Helper methods for Expression Trees
   */
  const std::shared_ptr<DerivedExpression> left_child() const;
  void set_left_child(const std::shared_ptr<DerivedExpression>& left);

  const std::shared_ptr<DerivedExpression> right_child() const;
  void set_right_child(const std::shared_ptr<DerivedExpression>& right);
  // @}

  ExpressionType type() const;

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

  // Returns true if the expression is a subselect.
  bool is_subselect() const;
  // @}

  // @{
  /**
   * Getters. Only call them if you are sure the type() has such a member
   */
  AggregateFunction aggregate_function() const;
  const AllTypeVariant value() const;
  const std::vector<std::shared_ptr<DerivedExpression>>& aggregate_function_arguments() const;
  ValuePlaceholder value_placeholder() const;
  // @}

  /**
   * Getters that can be called to check whether a member is set.
   */
  const std::optional<std::string>& table_name() const;
  const std::optional<std::string>& alias() const;

  void set_aggregate_function_arguments(
      const std::vector<std::shared_ptr<DerivedExpression>>& aggregate_function_arguments);

  void set_alias(const std::string& alias);

  /**
   * Returns the Expression as a string. Not meant to produce pretty outputs. Mostly intended to create column names
   * for SELECT lists with expressions: `SELECT a > 5 FROM ...`, here, the column name "a > 5" is generated using this
   * method. ColumnIDs need to be resolved to names and therefore need @param input_column_names.
   */
  virtual std::string to_string(const std::optional<std::vector<std::string>>& input_column_names = std::nullopt,
                                bool is_root = true) const;

 protected:
  // Not to be used directly, derived classes should implement it in the public scope and use this internally
  bool operator==(const AbstractExpression& other) const;

  virtual void _deep_copy_impl(const std::shared_ptr<DerivedExpression>& copy) const = 0;

  // the type of the expression
  const ExpressionType _type;
  // the value of an expression, e.g. of a Literal
  std::optional<AllTypeVariant> _value;

  std::optional<AggregateFunction> _aggregate_function;

  /*
   * A list of Expressions used in FunctionIdentifiers and CASE Expressions.
   * Not sure if this is the perfect way to go forward, but this is how hsql::Expr handles this.
   *
   * In case there are at most two expressions in this list, one could replace this list with an additional layer in the
   * Expression hierarchy.
   * E.g. for CASE one could argue that the THEN case becomes the left child, whereas ELSE becomes the right child.
   */
  std::vector<std::shared_ptr<DerivedExpression>> _aggregate_function_arguments;

  std::optional<std::string> _table_name;
  std::optional<std::string> _alias;

  std::optional<ValuePlaceholder> _value_placeholder;

  // @{
  // Members for the tree structure
  std::shared_ptr<DerivedExpression> _left_child;
  std::shared_ptr<DerivedExpression> _right_child;
  // @}

  friend class LQPExpression;
  friend class PQPExpression;  // For creating OperatorExpressions from LQPExpressions
};

}  // namespace opossum
