#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "all_type_variant.hpp"
#include "expression_precedence.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

enum class ExpressionType {
  Aggregate,
  Arithmetic,
  Cast,
  Case,
  CorrelatedParameter,
  PQPColumn,
  LQPColumn,
  Exists,
  Extract,
  Function,
  List,
  Logical,
  Placeholder,
  Predicate,
  PQPSubquery,
  LQPSubquery,
  UnaryMinus,
  Value
};

/**
 * AbstractExpression is the self-contained data structure describing Expressions in Hyrise.
 *
 * Expressions in Hyrise are everything down from Literals and Columns, over Arithmetics (a + b, ...),
 * Logicals (a AND b), up to Lists (`('a', 'b')`) and Subqueries. Check out the classes derived from AbstractExpression
 * for all available types.
 *
 * Expressions are evaluated (typically for all rows of a Chunk) using the ExpressionEvaluator.
 */
class AbstractExpression : public std::enable_shared_from_this<AbstractExpression> {
 public:
  explicit AbstractExpression(const ExpressionType type,
                              const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  virtual ~AbstractExpression() = default;

  /**
   * Recursively check for Expression equality.
   * @pre Both expressions need to reference the same LQP
   */
  bool operator==(const AbstractExpression& other) const;
  bool operator!=(const AbstractExpression& other) const;

  /**
   * Certain expression types (Parameters, Literals, and Columns) don't require computation and therefore don't require
   * temporary columns with their result in them
   */
  virtual bool requires_computation() const;

  virtual std::shared_ptr<AbstractExpression> deep_copy() const = 0;

  /**
   * @return a human readable string representing the Expression that can be used as a column name
   */
  virtual std::string as_column_name() const = 0;

  /**
   * @return the DataType of the result of the expression
   */
  virtual DataType data_type() const = 0;

  /**
   * @return    whether an expression, executed on the output of a plan, would
   *            produce a nullable result.
   * @note      Determining whether an Expression (isolated from a plan to execute it on) is nullable is intentionally
   *            not supported: This is because "expression X is nullable" does NOT always equal "a column containing
   *            expression X is nullable": An expression `a + 5` (with `a` being a non-nullable column),
   *            e.g., is actually nullable if `a` comes from the null-supplying side of an outer join.
   */
  bool is_nullable_on_lqp(const AbstractLQPNode& lqp) const;

  size_t hash() const;

  const ExpressionType type;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;

 protected:
  /**
   * Override to check for equality without checking the arguments. No override needed if derived expression has no
   * data members.
   */
  virtual bool _shallow_equals(const AbstractExpression& expression) const = 0;

  /**
   * Override to hash data fields in derived types. No override needed if derived expression has no
   * data members.
   */
  virtual size_t _shallow_hash() const;

  virtual bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const;

  /**
   * Used internally in _enclose_argument_as_column_name() to put parentheses around expression arguments if they have a lower
   * precedence than the expression itself.
   * Lower precedence indicates tighter binding, compare https://en.cppreference.com/w/cpp/language/operator_precedence
   *
   * @return  0 by default
   */
  virtual ExpressionPrecedence _precedence() const;

  /**
   * @return    argument.as_column_name(), enclosed by parentheses if the argument precedence is lower than
   *            this->_precedence()
   */
  std::string _enclose_argument_as_column_name(const AbstractExpression& argument) const;
};

// So that google test, e.g., prints readable error messages
inline std::ostream& operator<<(std::ostream& stream, const AbstractExpression& expression) {
  return stream << expression.as_column_name();
}

// Wrapper around expression->hash(), to enable hash based containers containing std::shared_ptr<AbstractExpression>
struct ExpressionSharedPtrHash final {
  size_t operator()(const std::shared_ptr<AbstractExpression>& expression) const { return expression->hash(); }
  size_t operator()(const std::shared_ptr<const AbstractExpression>& expression) const { return expression->hash(); }
};

// Wrapper around AbstractExpression::operator==(), to enable hash based containers containing
// std::shared_ptr<AbstractExpression>
struct ExpressionSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<const AbstractExpression>& expression_a,
                    const std::shared_ptr<const AbstractExpression>& expression_b) const {
    return *expression_a == *expression_b;
  }

  size_t operator()(const std::shared_ptr<AbstractExpression>& expression_a,
                    const std::shared_ptr<AbstractExpression>& expression_b) const {
    return *expression_a == *expression_b;
  }
};

template <typename Value>
using ExpressionUnorderedMap =
    std::unordered_map<std::shared_ptr<AbstractExpression>, Value, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

template <typename Value>
using ConstExpressionUnorderedMap = std::unordered_map<std::shared_ptr<const AbstractExpression>, Value,
                                                       ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

using ExpressionUnorderedSet =
    std::unordered_set<std::shared_ptr<AbstractExpression>, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

}  // namespace opossum
