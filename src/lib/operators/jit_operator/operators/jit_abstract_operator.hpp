#pragma once

#include <vector>

#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

/**
 * JitAbstractOperator is the abstract super class of all operators used within a JitOperator.
 * Usually, multiple operators are linked together to form an operator chain.
 * The operators work in a push-based fashion: The virtual "next" function is called for each tuple.
 * The operator can then process the tuple and finally call its own "emit" function to pass the tuple
 * on to the next operator in the chain.
 */
class JitAbstractOperator {
 public:
  using Ptr = std::shared_ptr<JitAbstractOperator>;

  virtual ~JitAbstractOperator() = default;

  void set_next_operator(const JitAbstractOperator::Ptr& next_operator) { _next_operator = next_operator; }

  virtual std::string description() const = 0;

 protected:
  void emit(JitRuntimeContext& ctx) const { _next_operator->next(ctx); }

 private:
  virtual void next(JitRuntimeContext& ctx) const = 0;

  JitAbstractOperator::Ptr _next_operator;
};

}  // namespace opossum
