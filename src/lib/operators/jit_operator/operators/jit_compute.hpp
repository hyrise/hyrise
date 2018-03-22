#pragma once

#include "jit_abstract_operator.hpp"

namespace opossum {

/* JitExpression represents a SQL expression - this includes arithmetic and logical expressions as well as comparisons.
 * Each JitExpression works on JitTupleValues and is structured as a binary tree. All leaves of that tree reference a tuple
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
  explicit JitExpression(const JitTupleValue& tuple_value);
  JitExpression(const std::shared_ptr<const JitExpression>& child, const ExpressionType expression_type,
                const size_t result_tuple_index);
  JitExpression(const std::shared_ptr<const JitExpression>& left_child, const ExpressionType expression_type,
                const std::shared_ptr<const JitExpression>& right_child, const size_t result_tuple_index);

  std::string to_string() const;

  ExpressionType expression_type() const { return _expression_type; }
  const JitTupleValue& result() const { return _result_value; }

  void compute(JitRuntimeContext& context) const;

 private:
  std::pair<const DataType, const bool> _compute_result_type();

  bool _is_binary_operator() const;

  const std::shared_ptr<const JitExpression> _left_child;
  const std::shared_ptr<const JitExpression> _right_child;
  const ExpressionType _expression_type;
  const JitTupleValue _result_value;
};

/* The JitCompute operator computes a single expression on the current tuple.
 * Most of the heavy lifting is done by the JitExpression itself.
 */
class JitCompute : public JitAbstractOperator {
 public:
  explicit JitCompute(const std::shared_ptr<const JitExpression>& expression);

  std::string description() const final;

 private:
  void _consume(JitRuntimeContext& context) const final;

  std::shared_ptr<const JitExpression> _expression;
};

}  // namespace opossum
