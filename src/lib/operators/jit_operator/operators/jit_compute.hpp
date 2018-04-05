#pragma once

#include "abstract_jittable.hpp"
#include "jit_expression.hpp"

namespace opossum {

/* The JitCompute operator computes a single expression on the current tuple.
 * Most of the heavy lifting is done by the JitExpression itself.
 */
class JitCompute : public AbstractJittable {
 public:
  explicit JitCompute(const JitExpressionCSPtr& expression);

  std::string description() const final;

 private:
  void _consume(JitRuntimeContext& context) const final;

  JitExpressionCSPtr _expression;
};

}  // namespace opossum
