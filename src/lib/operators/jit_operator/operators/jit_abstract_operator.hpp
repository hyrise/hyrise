#pragma once

#include <vector>

#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

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
