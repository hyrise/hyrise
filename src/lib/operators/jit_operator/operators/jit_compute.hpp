#pragma once

#include "jit_abstract_operator.hpp"

namespace opossum {

/* JitExpression represents a SQL expression - this includes arithmetic and logical expressions as well as comparisons.
 * JitExpressions work on JitTupleValues and are structured as a binary tree. All leafs of that tree reference a tuple
 * value in the JitRuntimeContext and are of type ExpressionType::Column - independent of whether these values actually
 * came from a column, are literal values or placeholders.
 * Each JitExpressions can compute its value and stores it in its assigned result JitTupleValue. JitExpressions are also
 * able to compute the data type of the expression they represent.
 *
 * Using AbstractExpression as a base class for JitExpressions seems like a logical choice. However, AbstractExpression
 * adds a lot of bloat during code specialization. We thus decided against deriving from it here.
 */
class JitExpression {
 public:
  using Ptr = std::shared_ptr<const JitExpression>;

  explicit JitExpression(const JitTupleValue& tuple_value);
  JitExpression(const JitExpression::Ptr& child, const ExpressionType expression_type, const size_t result_tuple_index);
  JitExpression(const JitExpression::Ptr& left_child, const ExpressionType expression_type,
                const JitExpression::Ptr& right_child, const size_t result_tuple_index);

  std::string to_string() const;

  ExpressionType expression_type() const { return _expression_type; }
  JitTupleValue result() const { return _result_value; }

  void compute(JitRuntimeContext& ctx) const;

 private:
  std::pair<const JitDataType, const bool> _compute_result_type();

  bool _is_binary_operator() const;

  const Ptr _left_child;
  const Ptr _right_child;
  const ExpressionType _expression_type;
  const JitTupleValue _result_value;
};

/* The JitCompute operator computes a single expression on the current tuple.
 * Most of the heavy lifting is done by the JitExpression itself.
 */
class JitCompute : public JitAbstractOperator {
 public:
  explicit JitCompute(const JitExpression::Ptr& expression);

  std::string description() const final;

 private:
  void next(JitRuntimeContext& ctx) const final;

  JitExpression::Ptr _expression;
};

}  // namespace opossum
