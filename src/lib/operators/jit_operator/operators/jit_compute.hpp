#pragma once

#include "jit_abstract_operator.hpp"

namespace opossum {

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

class JitCompute : public JitAbstractOperator {
 public:
  explicit JitCompute(const JitExpression::Ptr& expression);

  std::string description() const final;

 private:
  void next(JitRuntimeContext& ctx) const final;

  JitExpression::Ptr _expression;
};

}  // namespace opossum
